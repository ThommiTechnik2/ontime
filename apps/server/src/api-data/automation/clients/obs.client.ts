import { createHash, randomUUID } from 'node:crypto';

import { LogOrigin, OBSOutput, OBSStep, Playback, RuntimeStore } from 'ontime-types';
import { getErrorMessage } from 'ontime-utils';
import { DeepReadonly } from 'ts-essentials';
import { WebSocket } from 'ws';

import { getState } from '../../../stores/runtimeState.js';
import * as logger from '../../logs/logs.service.js';
import { parseTemplateNested } from '../automation.utils.js';
import { reportSendSuccess } from './sendFailures.js';

/**
 * Minimal client for obs-websocket v5
 * https://github.com/obsproject/obs-websocket/blob/master/docs/generated/protocol.md
 *
 * Each OBS output is sent as one request batch, executed by OBS in order (SerialRealtime)
 * This guarantees that eg: setting the preview, the transition and taking happen in sequence
 */

const enum OpCode {
  Hello = 0,
  Identify = 1,
  Identified = 2,
  RequestBatch = 8,
  RequestBatchResponse = 9,
}

/** obs-websocket RequestBatchExecutionType.SerialRealtime */
const serialRealtime = 0;
const connectTimeoutMs = 3000;
const batchTimeoutMs = 60_000;

/** OBS limits for SetCurrentSceneTransitionDuration */
const minTransitionDuration = 50;
const maxTransitionDuration = 20_000;
/** OBS limit for the Sleep request */
const maxSleep = 50_000;
/**
 * OBS applies preview and transition changes asynchronously on its UI thread
 * a take sent immediately after them would transition the previous preview
 */
const settleBeforeTakeMs = 100;
/** how often we check the running timer while waiting for a time left */
const remainingPollMs = 10;

type OBSRequest = { requestType: string; requestData?: Record<string, unknown> };

/**
 * A part of the steps, sent as one batch
 * untilRemaining holds the time left in the running event to wait for before sending
 */
type OBSPhase = { untilRemaining: number | null; requests: OBSRequest[] };

type WaitToken = { cancelled: boolean; continueNow: boolean };

type WaitingSequence = { token: WaitToken; nextId: string | null; done: Promise<void>; finished: boolean };

/**
 * The last sequence with a wait for time left, per OBS instance
 * - when the event it waits for starts, it is completed right away and the new sequence runs after it
 * - otherwise, a new sequence with a wait replaces it
 */
const waitingSequences = new Map<string, WaitingSequence>();

type BatchResult = {
  requestType: string;
  requestStatus: { result: boolean; code: number; comment?: string };
};

type PendingBatch = {
  resolve: (results: BatchResult[]) => void;
  reject: (error: Error) => void;
  timeout: NodeJS.Timeout;
};

type Connection = {
  socket: WebSocket;
  ready: Promise<void>;
  pending: Map<string, PendingBatch>;
};

/** one connection per OBS instance, opened on the first send */
const connections = new Map<string, Connection>();

/**
 * Expose possibility to send an ordered list of requests to OBS
 */
export function emitOBS(output: OBSOutput, store: DeepReadonly<RuntimeStore>) {
  const targetIP = parseTemplateNested(output.targetIP, store).trim();
  const target = `${targetIP}:${output.targetPort}`;
  const plan = toPlan(output.steps, store, target);

  if (plan.every((phase) => phase.requests.length === 0)) {
    logger.info(LogOrigin.Automation, 'OBS output skipped', { reason: 'no-steps', target });
    return;
  }

  // the event which was running when the output fired, waits refer to its time left
  const context = { eventId: store.eventNow?.id ?? null, nextId: store.eventNext?.id ?? null };

  const hasWait = plan.some((phase) => phase.untilRemaining !== null);
  const destination = { host: targetIP, port: output.targetPort, password: output.password, target };

  // a previous sequence may still be waiting to take into the event which starts now
  let after: Promise<void> = Promise.resolve();
  const previous = waitingSequences.get(target);
  if (previous && !previous.finished) {
    if (context.eventId !== null && context.eventId === previous.nextId) {
      previous.token.continueNow = true;
      after = previous.done;
    } else if (hasWait) {
      previous.token.cancelled = true;
    }
  }

  const token: WaitToken = { cancelled: false, continueNow: false };
  const sequence: WaitingSequence = { token, nextId: context.nextId, done: Promise.resolve(), finished: false };
  sequence.done = after
    .then(() => runPlan(plan, destination, context, token))
    .finally(() => {
      sequence.finished = true;
    });

  if (hasWait) {
    waitingSequences.set(target, sequence);
  }
}

type Destination = { host: string; port: number; password: string; target: string };

async function runPlan(
  plan: OBSPhase[],
  destination: Destination,
  context: { eventId: string | null; nextId: string | null },
  token: WaitToken,
) {
  const { target } = destination;
  for (const phase of plan) {
    if (phase.untilRemaining !== null) {
      // waiting for the end of an event only makes sense if another event follows
      if (!context.eventId || !context.nextId) {
        logger.info(LogOrigin.Automation, 'OBS sequence stopped', { reason: 'no-next-event', target });
        return;
      }
      const outcome = await waitForRemaining(phase.untilRemaining, context.eventId, context.nextId, token);
      if (outcome === 'cancel') {
        logger.info(LogOrigin.Automation, 'OBS sequence stopped', { reason: 'playback-changed', target });
        return;
      }
    }

    if (phase.requests.length === 0) continue;
    const sent = await sendAndReport(phase.requests, destination);
    if (!sent) return;
  }
}

/**
 * Resolves once the running event has at most the given time left
 * - waits while paused
 * - continues if playback already moved on to the next event, eg: for cuts at the end of an event
 * - cancels if playback stopped or jumped to another event
 */
function waitForRemaining(
  threshold: number,
  eventId: string,
  nextId: string,
  token: WaitToken,
): Promise<'continue' | 'cancel'> {
  return new Promise((resolve) => {
    const check = () => {
      if (token.cancelled) return 'cancel';
      if (token.continueNow) return 'continue';
      const { eventNow, timer } = getState();
      if (eventNow?.id === nextId) return 'continue';
      if (eventNow?.id !== eventId || timer.playback === Playback.Stop) return 'cancel';
      const isRunning = timer.playback === Playback.Play || timer.playback === Playback.Roll;
      if (isRunning && timer.current !== null && timer.current <= threshold) return 'continue';
      return null;
    };

    const immediate = check();
    if (immediate) {
      resolve(immediate);
      return;
    }
    const interval = setInterval(() => {
      const outcome = check();
      if (outcome) {
        clearInterval(interval);
        resolve(outcome);
      }
    }, remainingPollMs);
  });
}

/** sends one batch and logs the outcome, returns whether OBS could be reached */
async function sendAndReport(requests: OBSRequest[], destination: Destination): Promise<boolean> {
  const { target } = destination;
  logger.info(LogOrigin.Automation, 'OBS batch sending', {
    target,
    steps: requests.map((request) => request.requestType).join(' > '),
  });

  try {
    const results = await sendBatch(destination.host, destination.port, destination.password, requests);
    const failed = results.filter((result) => !result.requestStatus.result);
    if (failed.length === 0) {
      logger.info(LogOrigin.Automation, 'OBS batch done', { target, steps: results.length });
      reportSendSuccess(target);
      return true;
    }
    // a failed step does not stop the others, we report the problems to the user
    failed.forEach((result) => {
      logger.warning(LogOrigin.Automation, 'OBS request failed', {
        target,
        request: result.requestType,
        error: result.requestStatus.comment ?? String(result.requestStatus.code),
      });
    });
    return true;
  } catch (error) {
    // OBS outputs fire on discrete events, we report every failure to make problems visible
    logger.warning(LogOrigin.Automation, 'OBS send failed', { target, error: getErrorMessage(error) });
    return false;
  }
}

/**
 * Splits the steps at each "wait until time left" into phases
 * An empty time left waits for the end of the event, eg: a cut
 */
export function toPlan(steps: OBSStep[], store: DeepReadonly<RuntimeStore>, target = ''): OBSPhase[] {
  const groups: { untilRemaining: number | null; steps: OBSStep[] }[] = [{ untilRemaining: null, steps: [] }];
  for (const step of steps) {
    if (step.action === 'wait-until-remaining') {
      const threshold = toMilliseconds(resolveValue(step.value, store)) ?? 0;
      groups.push({ untilRemaining: threshold, steps: [] });
    } else {
      groups.at(-1)!.steps.push(step);
    }
  }
  return groups.map((group) => ({
    untilRemaining: group.untilRemaining,
    requests: toRequests(group.steps, store, target),
  }));
}

/**
 * Converts the user steps into obs-websocket requests
 * Steps whose value resolves empty are skipped, eg: an event without a transition in its custom field
 */
export function toRequests(steps: OBSStep[], store: DeepReadonly<RuntimeStore>, target = ''): OBSRequest[] {
  const requests: OBSRequest[] = [];

  for (const step of steps) {
    const value = resolveValue(step.value, store);

    switch (step.action) {
      case 'preview-scene':
        if (value) requests.push({ requestType: 'SetCurrentPreviewScene', requestData: { sceneName: value } });
        else logSkipped(step, target);
        break;
      case 'program-scene':
        if (value) requests.push({ requestType: 'SetCurrentProgramScene', requestData: { sceneName: value } });
        else logSkipped(step, target);
        break;
      case 'transition':
        if (value) {
          requests.push({ requestType: 'SetCurrentSceneTransition', requestData: { transitionName: value } });
        } else logSkipped(step, target);
        break;
      case 'transition-duration': {
        const duration = toMilliseconds(value);
        if (duration === null) {
          logSkipped(step, target);
          break;
        }
        // a cut ignores the duration, so we clamp rather than letting OBS reject the request
        const clamped = Math.min(Math.max(duration, minTransitionDuration), maxTransitionDuration);
        requests.push({
          requestType: 'SetCurrentSceneTransitionDuration',
          requestData: { transitionDuration: clamped },
        });
        break;
      }
      case 'studio-transition': {
        const previous = requests.at(-1);
        if (previous && previous.requestType !== 'Sleep') {
          requests.push({ requestType: 'Sleep', requestData: { sleepMillis: settleBeforeTakeMs } });
        }
        requests.push({ requestType: 'TriggerStudioModeTransition' });
        break;
      }
      case 'wait': {
        const sleep = toMilliseconds(value);
        if (sleep === null || sleep === 0) {
          logSkipped(step, target);
          break;
        }
        requests.push({ requestType: 'Sleep', requestData: { sleepMillis: Math.min(sleep, maxSleep) } });
        break;
      }
      case 'wait-until-remaining':
        // handled by toPlan, which splits the steps around it
        break;
      case 'run-macro': {
        // comma separated macro names of the Advanced Scene Switcher plugin
        const names = value
          .split(',')
          .map((name) => name.trim())
          .filter(Boolean);
        if (names.length === 0) {
          logSkipped(step, target);
          break;
        }
        names.forEach((name) => {
          requests.push({
            requestType: 'CallVendorRequest',
            requestData: {
              vendorName: 'AdvancedSceneSwitcher',
              requestType: 'AdvancedSceneSwitcherRunMacro',
              requestData: { name },
            },
          });
        });
        break;
      }
      default:
        step.action satisfies never;
    }
  }

  return requests;
}

/** resolves templates, a template which could not be resolved is treated as empty */
function resolveValue(template: string, store: DeepReadonly<RuntimeStore>): string {
  const value = parseTemplateNested(template, store).trim();
  if (value.includes('{{')) {
    return '';
  }
  return value;
}

function toMilliseconds(value: string): number | null {
  if (value === '') return null;
  const number = Number(value);
  if (!Number.isFinite(number) || number < 0) return null;
  return Math.round(number);
}

function logSkipped(step: OBSStep, target: string) {
  logger.info(LogOrigin.Automation, 'OBS step skipped', { reason: 'empty-value', action: step.action, target });
}

/**
 * Sends a request batch, opening the connection if needed
 */
async function sendBatch(host: string, port: number, password: string, requests: OBSRequest[]) {
  const connection = getConnection(host, port, password);
  await connection.ready;

  const requestId = randomUUID();
  return new Promise<BatchResult[]>((resolve, reject) => {
    const timeout = setTimeout(() => {
      connection.pending.delete(requestId);
      reject(new Error('OBS did not answer the request'));
    }, batchTimeoutMs);
    connection.pending.set(requestId, { resolve, reject, timeout });

    connection.socket.send(
      JSON.stringify({
        op: OpCode.RequestBatch,
        d: { requestId, haltOnFailure: false, executionType: serialRealtime, requests },
      }),
    );
  });
}

function getConnection(host: string, port: number, password: string): Connection {
  const key = `${host}:${port}:${password}`;
  const existing = connections.get(key);
  if (existing && existing.socket.readyState <= WebSocket.OPEN) {
    return existing;
  }

  const socket = new WebSocket(`ws://${host}:${port}`, 'obswebsocket.json');
  const pending = new Map<string, PendingBatch>();

  const ready = new Promise<void>((resolve, reject) => {
    const timeout = setTimeout(() => {
      reject(new Error('Timed out connecting to OBS'));
      socket.terminate();
    }, connectTimeoutMs);

    socket.on('message', (raw) => {
      let message: { op: number; d: Record<string, any> };
      try {
        message = JSON.parse(raw.toString());
      } catch {
        return;
      }

      switch (message.op) {
        case OpCode.Hello: {
          const auth = message.d.authentication as { challenge: string; salt: string } | undefined;
          const identify: Record<string, unknown> = { rpcVersion: 1, eventSubscriptions: 0 };
          if (auth) {
            identify.authentication = makeAuthentication(password, auth.salt, auth.challenge);
          }
          socket.send(JSON.stringify({ op: OpCode.Identify, d: identify }));
          break;
        }
        case OpCode.Identified:
          clearTimeout(timeout);
          logger.info(LogOrigin.Automation, 'OBS connected', { target: `${host}:${port}` });
          resolve();
          break;
        case OpCode.RequestBatchResponse: {
          const batch = pending.get(message.d.requestId);
          if (batch) {
            clearTimeout(batch.timeout);
            pending.delete(message.d.requestId);
            batch.resolve(message.d.results ?? []);
          }
          break;
        }
      }
    });

    socket.on('error', (error) => {
      clearTimeout(timeout);
      logger.warning(LogOrigin.Automation, 'OBS connection error', {
        target: `${host}:${port}`,
        error: getErrorMessage(error),
      });
      reject(error);
    });

    socket.on('close', (code, reason) => {
      clearTimeout(timeout);
      connections.delete(key);
      // 4009 is obs-websocket's authentication failed
      const message = code === 4009 ? 'OBS authentication failed' : `OBS closed the connection (${code} ${reason})`;
      reject(new Error(message));
      pending.forEach((batch) => {
        clearTimeout(batch.timeout);
        batch.reject(new Error(message));
      });
      pending.clear();
    });
  });

  // avoid unhandled rejections when nobody is waiting on the connection
  ready.catch(() => {});

  const connection = { socket, ready, pending };
  connections.set(key, connection);
  return connection;
}

/**
 * Authentication string as defined by obs-websocket v5
 * base64(sha256(base64(sha256(password + salt)) + challenge))
 */
export function makeAuthentication(password: string, salt: string, challenge: string): string {
  const secret = createHash('sha256')
    .update(password + salt)
    .digest('base64');
  return createHash('sha256')
    .update(secret + challenge)
    .digest('base64');
}

/** closes all open connections, used on shutdown and in tests */
export function closeOBSConnections() {
  waitingSequences.forEach((sequence) => (sequence.token.cancelled = true));
  waitingSequences.clear();
  connections.forEach((connection) => connection.socket.terminate());
  connections.clear();
}
