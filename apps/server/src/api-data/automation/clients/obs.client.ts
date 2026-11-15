import { createHash, randomUUID } from 'node:crypto';

import { LogOrigin, OBSOutput, OBSStep, RuntimeStore } from 'ontime-types';
import { getErrorMessage } from 'ontime-utils';
import { DeepReadonly } from 'ts-essentials';
import { WebSocket } from 'ws';

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

type OBSRequest = { requestType: string; requestData?: Record<string, unknown> };

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
  const requests = toRequests(output.steps, store, target);

  if (requests.length === 0) {
    logger.info(LogOrigin.Automation, 'OBS output skipped', { reason: 'no-steps', target });
    return;
  }

  logger.info(LogOrigin.Automation, 'OBS batch sending', {
    target,
    steps: requests.map((request) => request.requestType).join(' > '),
  });

  void sendBatch(targetIP, output.targetPort, output.password, requests)
    .then((results) => {
      const failed = results.filter((result) => !result.requestStatus.result);
      if (failed.length === 0) {
        logger.info(LogOrigin.Automation, 'OBS batch done', { target, steps: results.length });
        reportSendSuccess(target);
        return;
      }
      // a failed step does not stop the others, we report the problems to the user
      failed.forEach((result) => {
        logger.warning(LogOrigin.Automation, 'OBS request failed', {
          target,
          request: result.requestType,
          error: result.requestStatus.comment ?? String(result.requestStatus.code),
        });
      });
    })
    .catch((error) => {
      // OBS outputs fire on discrete events, we report every failure to make problems visible
      logger.warning(LogOrigin.Automation, 'OBS send failed', { target, error: getErrorMessage(error) });
    });
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
  connections.forEach((connection) => connection.socket.terminate());
  connections.clear();
}
