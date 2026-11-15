import type { AddressInfo } from 'node:net';

import { type OBSOutput, runtimeStorePlaceholder } from 'ontime-types';
import { WebSocketServer } from 'ws';

import * as logger from '../../logs/logs.service.js';
import { parseOutput } from '../automation.validation.js';
import { closeOBSConnections, emitOBS, makeAuthentication, toRequests } from '../clients/obs.client.js';

vi.mock('../../logs/logs.service.js', () => ({ info: vi.fn(), warning: vi.fn(), error: vi.fn() }));

const store = {
  ...runtimeStorePlaceholder,
  eventNow: { title: 'Kam 6', custom: { Uebergang: 'Überblende', Dauer: '5000' } },
  eventNext: { title: 'Kam 1', custom: { Uebergang: '', Dauer: '' } },
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
