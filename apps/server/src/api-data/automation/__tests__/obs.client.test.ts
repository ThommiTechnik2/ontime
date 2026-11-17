import type { AddressInfo } from 'node:net';

import { type OBSOutput, runtimeStorePlaceholder } from 'ontime-types';
import { WebSocketServer } from 'ws';

import * as runtimeState from '../../../stores/runtimeState.js';
import * as logger from '../../logs/logs.service.js';
import { parseOutput } from '../automation.validation.js';
import { closeOBSConnections, emitOBS, makeAuthentication, toPlan, toRequests } from '../clients/obs.client.js';

vi.mock('../../logs/logs.service.js', () => ({ info: vi.fn(), warning: vi.fn(), error: vi.fn() }));
vi.mock('../../../stores/runtimeState.js', () => ({ getState: vi.fn() }));

/** the running timer, as the wait reads it from the runtime state */
function setRuntime(eventId: string | null, current: number | null, playback = 'play') {
  vi.mocked(runtimeState.getState).mockReturnValue({
    eventNow: eventId ? { id: eventId } : null,
    timer: { current, playback },
  } as unknown as ReturnType<typeof runtimeState.getState>);
}

const store = {
  ...runtimeStorePlaceholder,
  eventNow: { id: 'now', title: 'Kam 6', custom: { Uebergang: 'Überblende', Dauer: '5000' } },
  eventNext: { id: 'next', title: 'Kam 1', custom: { Uebergang: '', Dauer: '' } },
} as unknown as typeof runtimeStorePlaceholder;

describe('toRequests()', () => {
  it('maps steps to obs-websocket requests in order, resolving templates', () => {
    const requests = toRequests(
      [
        { action: 'preview-scene', value: '{{eventNow.title}}' },
        { action: 'transition', value: '{{eventNow.custom.Uebergang}}' },
        { action: 'transition-duration', value: '{{eventNow.custom.Dauer}}' },
        { action: 'studio-transition', value: '' },
        { action: 'wait', value: '{{eventNow.custom.Dauer}}' },
        { action: 'preview-scene', value: '{{eventNext.title}}' },
      ],
      store,
    );

    expect(requests).toStrictEqual([
      { requestType: 'SetCurrentPreviewScene', requestData: { sceneName: 'Kam 6' } },
      { requestType: 'SetCurrentSceneTransition', requestData: { transitionName: 'Überblende' } },
      { requestType: 'SetCurrentSceneTransitionDuration', requestData: { transitionDuration: 5000 } },
      { requestType: 'Sleep', requestData: { sleepMillis: 100 } },
      { requestType: 'TriggerStudioModeTransition' },
      { requestType: 'Sleep', requestData: { sleepMillis: 5000 } },
      { requestType: 'SetCurrentPreviewScene', requestData: { sceneName: 'Kam 1' } },
    ]);
  });

  it('skips steps whose value is empty or unresolved', () => {
    const requests = toRequests(
      [
        { action: 'transition', value: '{{eventNext.custom.Uebergang}}' },
        { action: 'transition-duration', value: '{{eventNext.custom.Dauer}}' },
        { action: 'preview-scene', value: '{{eventNow.custom.doesNotExist}}' },
        { action: 'wait', value: '0' },
      ],
      store,
    );
    expect(requests).toStrictEqual([]);
  });

  it('lets OBS settle before a take, unless the user already waits', () => {
    expect(toRequests([{ action: 'studio-transition', value: '' }], store)).toStrictEqual([
      { requestType: 'TriggerStudioModeTransition' },
    ]);
    const types = toRequests(
      [
        { action: 'preview-scene', value: 'Kam 1' },
        { action: 'wait', value: '500' },
        { action: 'studio-transition', value: '' },
      ],
      store,
    ).map((request) => request.requestType);
    expect(types).toStrictEqual(['SetCurrentPreviewScene', 'Sleep', 'TriggerStudioModeTransition']);
  });

  it('clamps the transition duration to the range OBS accepts', () => {
    const requests = toRequests(
      [
        { action: 'transition-duration', value: '0' },
        { action: 'transition-duration', value: '99999' },
      ],
      store,
    );
    expect(requests.map((request) => request.requestData?.transitionDuration)).toStrictEqual([50, 20000]);
  });
});

describe('toPlan()', () => {
  it('splits the steps at each wait for time left', () => {
    const plan = toPlan(
      [
        { action: 'preview-scene', value: '{{eventNext.title}}' },
        { action: 'wait-until-remaining', value: '{{eventNow.custom.Dauer}}' },
        { action: 'studio-transition', value: '' },
      ],
      store,
    );
    expect(plan).toStrictEqual([
      {
        untilRemaining: null,
        requests: [{ requestType: 'SetCurrentPreviewScene', requestData: { sceneName: 'Kam 1' } }],
      },
      { untilRemaining: 5000, requests: [{ requestType: 'TriggerStudioModeTransition' }] },
    ]);
  });

  it('waits for the end of the event when the time left is empty', () => {
    const plan = toPlan([{ action: 'wait-until-remaining', value: '{{eventNext.custom.Dauer}}' }], store);
    expect(plan.at(-1)?.untilRemaining).toBe(0);
  });
});

describe('parseOutput() with OBS outputs', () => {
  it('accepts a valid output and defaults optional fields', () => {
    const result = parseOutput({
      type: 'obs',
      targetIP: '127.0.0.1',
      targetPort: 4455,
      steps: [{ action: 'studio-transition' }],
    });
    expect(result).toStrictEqual({
      type: 'obs',
      targetIP: '127.0.0.1',
      targetPort: 4455,
      password: '',
      steps: [{ action: 'studio-transition', value: '' }],
    });
  });

  it('rejects unknown actions and invalid targets', () => {
    expect(() =>
      parseOutput({ type: 'obs', targetIP: '127.0.0.1', targetPort: 4455, steps: [{ action: 'explode' }] }),
    ).toThrow();
    expect(() => parseOutput({ type: 'obs', targetIP: 'not a host', targetPort: 4455, steps: [] })).toThrow();
    expect(() => parseOutput({ type: 'obs', targetIP: '127.0.0.1', targetPort: 70000, steps: [] })).toThrow();
  });
});

describe('emitOBS() against a simulated obs-websocket server', () => {
  const password = 'secret';
  const salt = 'lM1GncleQOaCu9lT1yeUZhFYnqhsLLP1G5lAGo3ixaI=';
  const challenge = '+IxH4CnCiqpX1rM9scsNynZzbOe4KhDeYcTNS3PDaeY=';

  let server: WebSocketServer;
  let port: number;
  let received: { op: number; d: any }[];

  beforeEach(async () => {
    received = [];
    server = new WebSocketServer({ port: 0 });
    await new Promise<void>((resolve) => server.on('listening', () => resolve()));
    port = (server.address() as AddressInfo).port;

    server.on('connection', (socket, request) => {
      expect(request.headers['sec-websocket-protocol']).toBe('obswebsocket.json');
      socket.send(
        JSON.stringify({
          op: 0,
          d: { obsWebSocketVersion: '5.7.6', rpcVersion: 1, authentication: { challenge, salt } },
        }),
      );
      socket.on('message', (raw) => {
        const message = JSON.parse(raw.toString());
        received.push(message);
        if (message.op === 1) {
          if (message.d.authentication !== makeAuthentication(password, salt, challenge)) {
            socket.close(4009, 'Authentication failed.');
            return;
          }
          socket.send(JSON.stringify({ op: 2, d: { negotiatedRpcVersion: 1 } }));
        }
        if (message.op === 8) {
          socket.send(
            JSON.stringify({
              op: 9,
              d: {
                requestId: message.d.requestId,
                results: message.d.requests.map((r: { requestType: string }) => ({
                  requestType: r.requestType,
                  requestStatus: { result: true, code: 100 },
                })),
              },
            }),
          );
        }
      });
    });
  });

  afterEach(async () => {
    closeOBSConnections();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    vi.mocked(logger.warning).mockClear();
  });

  const makeOutput = (overrides: Partial<OBSOutput> = {}): OBSOutput => ({
    type: 'obs',
    targetIP: '127.0.0.1',
    targetPort: port,
    password,
    steps: [
      { action: 'preview-scene', value: '{{eventNow.title}}' },
      { action: 'transition', value: '{{eventNow.custom.Uebergang}}' },
      { action: 'studio-transition', value: '' },
    ],
    ...overrides,
  });

  it('authenticates and sends the steps as one ordered batch', async () => {
    emitOBS(makeOutput(), store);
    await vi.waitFor(() => expect(received.some((m) => m.op === 8)).toBe(true));

    const batch = received.find((m) => m.op === 8)!;
    expect(batch.d.executionType).toBe(0); // SerialRealtime
    expect(batch.d.haltOnFailure).toBe(false);
    expect(batch.d.requests.map((r: { requestType: string }) => r.requestType)).toStrictEqual([
      'SetCurrentPreviewScene',
      'SetCurrentSceneTransition',
      'Sleep',
      'TriggerStudioModeTransition',
    ]);
  });

  it('reuses the connection for later sends', async () => {
    emitOBS(makeOutput(), store);
    await vi.waitFor(() => expect(received.filter((m) => m.op === 8)).toHaveLength(1));
    emitOBS(makeOutput(), store);
    await vi.waitFor(() => expect(received.filter((m) => m.op === 8)).toHaveLength(2));
    expect(received.filter((m) => m.op === 1)).toHaveLength(1);
  });

  describe('waiting for the time left in the running event', () => {
    const takeOutput = () =>
      makeOutput({
        steps: [
          { action: 'preview-scene', value: '{{eventNext.title}}' },
          { action: 'wait-until-remaining', value: '2000' },
          { action: 'studio-transition', value: '' },
        ],
      });
    const batches = () => received.filter((m) => m.op === 8);

    it('sends the take once the event has the given time left', async () => {
      setRuntime('now', 5000);
      emitOBS(takeOutput(), store);
      await vi.waitFor(() => expect(batches()).toHaveLength(1));
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(batches()).toHaveLength(1);

      setRuntime('now', 1990);
      await vi.waitFor(() => expect(batches()).toHaveLength(2));
      expect(batches()[1].d.requests.map((r: { requestType: string }) => r.requestType)).toStrictEqual([
        'TriggerStudioModeTransition',
      ]);
    });

    it('keeps waiting while paused', async () => {
      setRuntime('now', 1000, 'pause');
      emitOBS(takeOutput(), store);
      await vi.waitFor(() => expect(batches()).toHaveLength(1));
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(batches()).toHaveLength(1);
      setRuntime('now', 1000, 'play');
      await vi.waitFor(() => expect(batches()).toHaveLength(2));
    });

    it('takes when playback already moved on to the next event, eg: a cut', async () => {
      setRuntime('now', 5000);
      emitOBS(takeOutput(), store);
      await vi.waitFor(() => expect(batches()).toHaveLength(1));
      setRuntime('next', 10000);
      await vi.waitFor(() => expect(batches()).toHaveLength(2));
    });

    it('completes a waiting take before the sequence of the event which starts, eg: a cut', async () => {
      setRuntime('now', 5000);
      emitOBS(takeOutput(), store);
      await vi.waitFor(() => expect(batches()).toHaveLength(1));

      // the next event starts and fires its own sequence before the wait noticed it
      const nextStore = {
        ...store,
        eventNow: { ...store.eventNow, id: 'next', title: 'Kam 1' },
        eventNext: { ...store.eventNext, id: 'after', title: 'Kam 3' },
      } as typeof store;
      emitOBS(takeOutput(), nextStore);

      await vi.waitFor(() => expect(batches()).toHaveLength(3));
      const types = batches().map((batch) => batch.d.requests.map((r: { requestType: string }) => r.requestType));
      expect(types[1]).toStrictEqual(['TriggerStudioModeTransition']);
      expect(batches()[2].d.requests[0].requestData).toStrictEqual({ sceneName: 'Kam 3' });
    });

    it('drops the take when playback stops or jumps elsewhere', async () => {
      setRuntime('now', 5000);
      emitOBS(takeOutput(), store);
      await vi.waitFor(() => expect(batches()).toHaveLength(1));
      setRuntime('somewhere-else', 1000);
      await new Promise((resolve) => setTimeout(resolve, 60));
      expect(batches()).toHaveLength(1);
    });
  });

  it('reports a wrong password', async () => {
    emitOBS(makeOutput({ password: 'wrong' }), store);
    await vi.waitFor(() =>
      expect(logger.warning).toHaveBeenCalledWith(
        expect.anything(),
        'OBS send failed',
        expect.objectContaining({ error: 'OBS authentication failed' }),
      ),
    );
  });
});
