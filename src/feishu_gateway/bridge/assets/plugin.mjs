import { createHash, timingSafeEqual } from 'node:crypto';
import { constants, lstatSync, openSync, closeSync, fstatSync, readFileSync, writeFileSync, appendFileSync, fsyncSync, mkdirSync, chmodSync, unlinkSync } from 'node:fs';
import { realpath, stat } from 'node:fs/promises';
import { createServer } from 'node:http';
import { createConnection } from 'node:net';
import { dirname, isAbsolute, join, normalize } from 'node:path';
import { fileURLToPath } from 'node:url';

export const name = 'feishu-session-bridge';
export const inject = ['connection', 'remoteAccess', 'sessionQuery', 'typertGateway', 'workspaceRegistry'];

const methods = Object.freeze(['list', 'state', 'prompt', 'wait', 'events']);
const uuid = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/;
const uid = process.getuid();
const limit = 16 * 1024;
const inventoryLimit = 1024;
const responseLimit = 512 * 1024;
const role = Object.freeze(Object.fromEntries(methods.map((method) => [method, {
  name: `${name}:${method}`, resource: 'session', actions: [`session/${method}`],
}])));
const globalList = {
  role: { name: `${name}:catalog`, resource: 'global', actions: ['session/list'] }, resource: { kind: 'global' },
};

class Refusal extends Error {
  constructor(code, status = 400) {
    super(code);
    this.status = status;
  }
}

function owned(info, mode) {
  return info.uid === uid && (info.mode & 0o777) === mode;
}

function readPrivate(path, maxBytes = 256) {
  const fd = openSync(path, constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK);
  try {
    const info = fstatSync(fd);
    if (!info.isFile() || !owned(info, 0o600) || info.size > maxBytes) throw new Error('insecure bridge file');
    return readFileSync(fd, 'utf8');
  } finally {
    closeSync(fd);
  }
}

function optionalPrivate(path, maxBytes) {
  try {
    return readPrivate(path, maxBytes);
  } catch (error) {
    if (error.code === 'ENOENT') return undefined;
    throw error;
  }
}

function validSessionId(value) {
  return typeof value === 'string' && value.startsWith('session-') && uuid.test(value.slice(8));
}

function validWorkspace(path) {
  return typeof path === 'string' && path.length > 1 && Buffer.byteLength(path) <= 1024
    && !path.startsWith('//') && !path.endsWith('/') && !/[\x00-\x1f\x7f]/u.test(path)
    && isAbsolute(path) && normalize(path) === path;
}

function readScope(directory) {
  const files = ['bridge-session', 'bridge-workspace'];
  const [sessionId, cwd] = files.map((file) => optionalPrivate(join(directory, file))?.trim());
  const legacy = sessionId !== undefined || cwd !== undefined ? { sessionId, cwd } : undefined;
  if (legacy && (!validSessionId(sessionId) || !validWorkspace(cwd))) throw new Error('invalid legacy scope');
  const workspaceConfig = optionalPrivate(join(directory, 'bridge-workspaces.json'), 32 * 1024);
  if (workspaceConfig === undefined && !legacy) throw new Error('missing bridge scope');
  if (workspaceConfig !== undefined) parseWorkspaceRoots(JSON.parse(workspaceConfig));
  return { legacy, workspaceMode: workspaceConfig !== undefined };
}

function parseWorkspaceRoots(config) {
  validate(config, ['roots']);
  if (!Array.isArray(config.roots) || config.roots.length > 32) throw new Refusal('invalid');
  const seen = new Set();
  return config.roots.map((root) => {
    const keys = root && Object.hasOwn(root, 'includeDescendants') ? ['path', 'includeDescendants'] : ['path'];
    validate(root, keys);
    const { path, includeDescendants = false } = root;
    if (!validWorkspace(path) || typeof includeDescendants !== 'boolean' || seen.has(path)) throw new Refusal('invalid');
    seen.add(path);
    return { path, includeDescendants };
  });
}

function matchesRoot(cwd, roots) {
  return roots.some(({ path, includeDescendants }) => cwd === path || (includeDescendants && cwd.startsWith(`${path}/`)));
}

function covers(available, requested) {
  return requested.every(({ path, includeDescendants }) => available.some((root) => (
    matchesRoot(path, [root]) && (!includeDescendants || root.includeDescendants)
  )));
}

async function canonicalDirectory(cwd) {
  try {
    return await realpath(cwd) === cwd && (await stat(cwd)).isDirectory();
  } catch (error) {
    if (['ENOENT', 'ENOTDIR', 'ELOOP'].includes(error.code)) return false;
    throw error;
  }
}

function privateDirectory(path) {
  const info = lstatSync(path);
  if (!info.isDirectory() || !owned(info, 0o700)) throw new Error('insecure bridge directory');
}

async function removeStaleSocket(path) {
  let original;
  try {
    original = lstatSync(path);
  } catch (error) {
    if (error.code === 'ENOENT') return;
    throw error;
  }
  if (!original.isSocket() || !owned(original, 0o600)) throw new Error('insecure bridge socket');

  const refusal = await new Promise((resolve) => {
    const probe = createConnection(path);
    const timer = setTimeout(() => {
      probe.destroy();
      resolve(undefined);
    }, 1200);
    probe.once('connect', () => {
      clearTimeout(timer);
      probe.destroy();
      resolve(undefined);
    });
    probe.once('error', (error) => {
      clearTimeout(timer);
      resolve(error.code);
    });
  });
  if (refusal !== 'ECONNREFUSED') throw new Error('existing bridge socket may have a listener');
  const current = lstatSync(path);
  if (!current.isSocket() || !owned(current, 0o600) || current.dev !== original.dev || current.ino !== original.ino) {
    throw new Error('bridge socket changed during probe');
  }
  unlinkSync(path);
}

function authenticate(header, token) {
  if (typeof header !== 'string') return false;
  const value = Buffer.from(header, 'ascii');
  return value.length === token.length && /^[\w-]{43}$/.test(header) && timingSafeEqual(value, token);
}

function validate(value, keys) {
  if (
    !value || typeof value !== 'object' || Array.isArray(value)
    || Object.keys(value).length !== keys.length || !keys.every((key) => Object.hasOwn(value, key))
  ) throw new Refusal('invalid');
}

async function readRequest(req) {
  const chunks = [];
  let size = 0;
  for await (const chunk of req) {
    size += chunk.length;
    if (size > limit * 4) throw new Refusal('invalid', 413);
    chunks.push(chunk);
  }
  try {
    return JSON.parse(Buffer.concat(chunks).toString('utf8'));
  } catch {
    throw new Refusal('invalid');
  }
}

function safeState(state) {
  if (
    !Number.isSafeInteger(state.cursor) || state.cursor < 0
    || typeof state.active !== 'boolean' || typeof state.running !== 'boolean'
    || !(state.queue === null || Array.isArray(state.queue)) || !Array.isArray(state.pendingInput)
  ) throw new Refusal('dsh_protocol', 502);
  return {
    cursor: state.cursor,
    active: state.active,
    running: state.running,
    queued: state.queue?.length ?? 0,
    pendingInput: state.pendingInput.length,
    approvalPending: state.pendingInput.includes('approval'),
  };
}

function digest(text) {
  return createHash('sha256').update(text).digest('hex');
}

function visibleEvents(page, requestId, claim) {
  const events = page.records.map((record) => record.event).sort((left, right) => left.seq - right.seq);
  const starts = events.filter((event) => event.seq > claim.cursor &&
    event.type === 'user/message' && event.data?.source?.rpcId === requestId);
  if (!starts.length) return { records: [], toolCalls: 0, turnCurrent: false };
  const [start] = starts;
  const input = start.data?.content;
  if (starts.length !== 1 || input?.length !== 1 || input[0].type !== 'text' || typeof input[0].text !== 'string'
    || digest(input[0].text) !== claim.digest) throw new Refusal('dsh_protocol', 502);
  const records = [{ type: start.type, seq: start.seq, requestId }];
  const boundary = events.findLast((event) => event.seq > claim.cursor && event.seq < start.seq &&
    ['turn/start', 'turn/end'].includes(event.type));
  let turn = start.data?.turn ?? (boundary?.type === 'turn/start' ? boundary.data?.turn : undefined);
  let toolCalls = 0;
  let lastAnswer;
  let turnCurrent = true;
  for (const { type, seq, data } of events) {
    if (seq <= start.seq) continue;
    if (type === 'user/message') {
      const source = data?.source;
      if (source?.rpcId !== undefined || !['runtime-context', 'skill-catalog', 'time-context'].includes(source?.kind)) {
        turnCurrent = false;
        break;
      }
    }
    if (type === 'turn/start') {
      if (turn !== undefined && data?.turn !== turn) {
        turnCurrent = false;
        break;
      }
      turn ??= data?.turn;
    }
    if (type === 'tool/call') toolCalls += 1;
    if (type !== 'assistant/message' && type !== 'turn/end') continue;
    turn ??= data?.turn;
    if (turn === undefined || data.turn !== turn) {
      turnCurrent = false;
      break;
    }
    if (type === 'turn/end') {
      turnCurrent = false;
      if (data.reason?.kind === 'completed' && lastAnswer) records.push(lastAnswer);
      records.push({ type, seq, turn, reason: data.reason?.kind });
      break;
    }
    const text = (data.message?.content ?? [])
      .filter((part) => part.type === 'text' && typeof part.text === 'string')
      .map((part) => part.text).join('').trim();
    lastAnswer = {
      type, seq, turn, textSha256: digest(text),
      text: text.length > 8000 ? text.slice(0, 8000) : text,
    };
  }
  if (Number.isSafeInteger(turn)) records[0].turn = turn;
  return { records, toolCalls, turnCurrent: turnCurrent && Number.isSafeInteger(turn) };
}

export async function apply(ctx) {
  const directory = dirname(fileURLToPath(import.meta.url));
  const socket = join(directory, 'bridge.sock');
  if (Buffer.byteLength(socket) >= 104) throw new Error('bridge socket path is too long');
  privateDirectory(directory);
  const { legacy, workspaceMode } = readScope(directory);
  const encoded = readPrivate(join(directory, 'bridge-token')).trim();
  if (Buffer.from(encoded, 'base64url').toString('base64url') !== encoded || encoded.length !== 43) {
    throw new Error('invalid bridge token');
  }
  const token = Buffer.from(encoded, 'ascii');
  const ledger = join(directory, 'requests');
  mkdirSync(ledger, { mode: 0o700, recursive: true });
  privateDirectory(ledger);

  const bindingsByPrincipal = new WeakMap();
  const nativePeers = new Set();
  const unbind = ctx.remoteAccess.registerPolicy(name, { bindings: (subject) => bindingsByPrincipal.get(subject) ?? [] });
  const sockets = new Set();
  const submitting = new Set();
  const completions = join(directory, 'completions.jsonl');
  let stopping = false;

  function completionConfig() {
    try {
      const value = JSON.parse(readPrivate(join(directory, 'completion.json'), 4096));
      if (!value || typeof value.url !== 'string' || typeof value.token !== 'string') return null;
      if (!/^http:\/\/127\.0\.0\.1:[0-9]{1,5}\/completion$/.test(value.url)) return null;
      if (value.token.length < 16 || value.token.length > 256) return null;
      return value;
    } catch {
      return null;
    }
  }

  function completionText(session, turn) {
    try {
      const events = session.snapshotEvents();
      const answer = events.findLast((item) => item.type === 'assistant/message' && item.data?.turn === turn);
      const text = (answer?.data?.message?.content ?? [])
        .filter((part) => part.type === 'text' && typeof part.text === 'string')
        .map((part) => part.text).join('').trim();
      return text.length > 16384 ? text.slice(0, 16384) : text;
    } catch {
      return '';
    }
  }

  function completionRequest(session, turn, endSeq) {
    try {
      const start = session.snapshotEvents().findLast((item) =>
        item.seq < endSeq && item.type === 'user/message'
        && typeof item.data?.source?.rpcId === 'string');
      return start?.data.source.rpcId;
    } catch {
      return undefined;
    }
  }

  function onSessionEvent(session, event) {
    if (event?.type !== 'turn/end' || !validSessionId(session?.id)) return;
    const turn = event.data?.turn;
    const reason = event.data?.reason?.kind;
    if (!Number.isSafeInteger(turn) || typeof reason !== 'string') return;
    const record = {
      eventId: `${session.id}:${event.seq}`,
      sessionId: session.id,
      requestId: completionRequest(session, turn, event.seq),
      turn,
      reason,
      text: completionText(session, turn),
      occurredAt: Date.now(),
    };
    try {
      appendFileSync(completions, JSON.stringify(record) + '\n', { mode: 0o600 });
      chmodSync(completions, 0o600);
    } catch (error) {
      // Completion feed is best-effort; the request/response APIs remain intact.
    }
    const webhook = completionConfig();
    if (webhook) {
      const payload = {
        event_id: `dsh:${record.eventId}`,
        source: 'dsh-bridge',
        host: webhook.host,
        session_id: record.sessionId,
        requestId: record.requestId,
        turn_id: String(record.turn),
        state: reason === 'completed' ? 'completed' : 'failed',
        seq: Date.now(),
        output: record.text,
      };
      try {
        void fetch(webhook.url, {
          method: 'POST',
          headers: { 'Content-Type': 'application/json', 'X-Gateway-Token': webhook.token },
          body: JSON.stringify(payload),
          signal: AbortSignal.timeout(1500),
        }).catch(() => {});
      } catch {
        // Never let completion delivery affect the DSH session.
      }
    }
  }

  const offCompletion = ctx.on('session/event', onSessionEvent);
  ctx.effect(() => () => {
    if (typeof offCompletion === 'function') offCompletion();
  }, 'feishu-session-bridge: completion feed');

  function archivedSessions() {
    const archive = ctx.workspaceRegistry.archivedSessionIds;
    if (!Array.isArray(archive)) throw new Refusal('dsh_protocol', 502);
    return new Set(archive);
  }

  function currentRoots() {
    if (!workspaceMode) return null;
    try {
      return parseWorkspaceRoots(JSON.parse(readPrivate(join(directory, 'bridge-workspaces.json'), 32 * 1024)));
    } catch {
      // Never fall back to a legacy session when a live scope is unreadable.
      throw new Refusal('bridge_unavailable', 503);
    }
  }

  async function inventory(roots, target) {
    const signal = AbortSignal.timeout(8000);
    const records = await ctx.sessionQuery.listSessions(signal);
    if (!Array.isArray(records)) throw new Refusal('dsh_protocol', 502);
    const archivedIds = archivedSessions();
    const accepted = new Map();
    const checkedDirectories = new Map();
    for (const { header, formatStatus } of records) {
      signal.throwIfAborted();
      const { id, cwd, origin, parentSession, archived, ephemeral } = header ?? {};
      if (!validSessionId(id) || !validWorkspace(cwd) || archived || archivedIds.has(id) || ephemeral || parentSession != null
        || origin === 'subagent' || (formatStatus !== undefined && formatStatus !== 'current')) continue;
      if (target && (id !== target.sessionId || cwd !== target.cwd)) continue;
      if (roots) {
        if (!matchesRoot(cwd, roots)) continue;
        if (!checkedDirectories.has(cwd)) checkedDirectories.set(cwd, await canonicalDirectory(cwd));
        if (!checkedDirectories.get(cwd)) continue;
      } else if (id !== legacy.sessionId || cwd !== legacy.cwd) {
        continue;
      }
      if (accepted.has(id)) throw new Refusal('dsh_protocol', 502);
      accepted.set(id, cwd);
      if (accepted.size > inventoryLimit) throw new Refusal('catalog_too_large', 503);
    }
    const latestArchive = archivedSessions();
    for (const id of accepted.keys()) if (latestArchive.has(id)) accepted.delete(id);
    return accepted;
  }

  async function usePeer(sessionIds, allowedMethods, perform) {
    if (stopping) throw new Refusal('bridge_unavailable', 503);
    const principal = Object.freeze({ kind: name });
    const grants = allowedMethods.includes('list') ? [globalList] : [];
    for (const id of sessionIds) for (const method of allowedMethods) {
      grants.push({ role: role[method], resource: { kind: 'session', id } });
    }
    bindingsByPrincipal.set(principal, grants);
    const peer = ctx.connection.createPeer({ principal, interactions: 'none' });
    nativePeers.add(peer);
    try {
      return await perform(peer);
    } finally {
      bindingsByPrincipal.delete(principal);
      nativePeers.delete(peer);
      await peer.dispose();
    }
  }

  async function rpc(peer, method, args) {
    const timeout = method === 'wait' ? 31000 : 8000;
    const reply = await ctx.typertGateway.invokeRpc(`session/${method}`, { args }, AbortSignal.timeout(timeout), peer);
    if (!reply?.ok) {
      if (method === 'prompt' && ['session/writer-held', 'session/agent-busy'].includes(reply?.error?.code)) {
        throw new Refusal('busy', 409);
      }
      throw new Refusal('dsh_rejected', 409);
    }
    return reply.value;
  }

  async function listSessions(roots) {
    if (roots?.length === 0) return { items: [] };
    const allowed = await inventory(roots);
    const list = await usePeer(allowed.keys(), ['list'], (peer) => rpc(peer, 'list', { _request: {} }));
    if (!Array.isArray(list.items)) throw new Refusal('dsh_protocol', 502);
    const listed = new Set();
    const archive = archivedSessions();
    const items = [];
    for (const item of list.items) {
      const { sessionId, cwd, agentAvailable, running } = item;
      if (archive.has(sessionId)) continue;
      if (listed.has(sessionId) || !allowed.has(sessionId) || allowed.get(sessionId) !== cwd
        || item.archived || item.ephemeral || item.parentSessionId != null || item.origin === 'subagent') {
        throw new Refusal('dsh_protocol', 502);
      }
      listed.add(sessionId);
      const title = item.title ?? item.projections?.values?.title;
      items.push({ sessionId, title: typeof title === 'string' ? title.slice(0, 240) : null, agentAvailable, running,
        ...(roots ? { cwd } : {}) });
    }
    if (listed.size !== allowed.size) throw new Refusal('catalog_changed', 503);
    if (roots && !covers(currentRoots(), roots)) throw new Refusal('catalog_changed', 503);
    if (Buffer.byteLength(JSON.stringify(items)) > responseLimit - 256) throw new Refusal('catalog_too_large', 503);
    return { items };
  }

  async function requireTarget(target) {
    if (!(await inventory(currentRoots(), target)).has(target.sessionId)) throw new Refusal('wrong_session', 403);
  }

  function requireWorkspace(cwd) {
    if (workspaceMode && !matchesRoot(cwd, currentRoots())) throw new Refusal('wrong_session', 403);
  }

  async function state(peer, sessionId) {
    return safeState(await rpc(peer, 'state', { request: { sessionId } }));
  }

  function readClaim(target, requestId, submittingClaim = false) {
    if (typeof requestId !== 'string' || !uuid.test(requestId)) throw new Refusal('invalid');
    try {
      const claim = JSON.parse(readPrivate(join(ledger, requestId), 2048));
      const knownCwd = claim.cwd === undefined && claim.sessionId === legacy?.sessionId ? legacy.cwd : claim.cwd;
      if (claim.sessionId !== target.sessionId || knownCwd !== target.cwd || !Number.isSafeInteger(claim.cursor)
        || claim.cursor < 0 || !/^[a-f0-9]{64}$/u.test(claim.digest)) {
        throw submittingClaim ? new Refusal('request_conflict', 409) : new Refusal('unknown_request', 404);
      }
      return claim;
    } catch (error) {
      if (error.code === 'ENOENT') return null;
      throw error;
    }
  }

  function claimRequest(target, requestId, cursor, digest) {
    let fd;
    try {
      fd = openSync(join(ledger, requestId), 'wx', 0o600);
    } catch (error) {
      if (error.code === 'EEXIST') throw new Refusal('request_conflict', 409);
      throw error;
    }
    const { sessionId, cwd } = target;
    try {
      writeFileSync(fd, JSON.stringify({ sessionId, cursor, digest, ...(workspaceMode ? { cwd } : {}) }));
      fsyncSync(fd);
      const dir = openSync(ledger, constants.O_RDONLY | constants.O_DIRECTORY);
      try {
        fsyncSync(dir);
      } finally {
        closeSync(dir);
      }
    } finally {
      closeSync(fd);
    }
  }

  async function prompt(target, message) {
    if (typeof message.text !== 'string' || !message.text.trim() || Buffer.byteLength(message.text) > limit) {
      throw new Refusal('invalid');
    }
    const textDigest = digest(message.text);
    const { sessionId } = target;
    const existing = readClaim(target, message.requestId, true);
    if (existing) throw new Refusal(existing.digest === textDigest ? 'already_claimed' : 'request_conflict', 409);
    if (submitting.has(sessionId)) throw new Refusal('busy', 409);
    submitting.add(sessionId);
    try {
      await requireTarget(target);
      return await usePeer([sessionId], ['state', 'prompt'], async (peer) => {
        const current = await state(peer, sessionId);
        if ((!workspaceMode && !current.active) || current.running || current.queued || current.pendingInput) {
          throw new Refusal('busy', 409);
        }
        await requireTarget(target);
        requireWorkspace(target.cwd);
        claimRequest(target, message.requestId, current.cursor, textDigest);
        requireWorkspace(target.cwd);
        const result = await rpc(peer, 'prompt', { request: {
          sessionId, requestId: message.requestId, mode: 'queue', content: [{ type: 'text', text: message.text }],
        } });
        if (result?.accepted !== true) throw new Refusal('submission_uncertain', 409);
        return { accepted: true, requestId: message.requestId, cursor: current.cursor };
      });
    } finally {
      submitting.delete(sessionId);
    }
  }

  async function dispatch(message) {
    const method = message?.method;
    const keys = {
      list: [], state: ['sessionId'], prompt: ['sessionId', 'requestId', 'text'],
      wait: ['sessionId', 'requestId', 'timeoutMs'], events: ['sessionId', 'requestId'],
    };
    if (!methods.includes(method)) throw new Refusal('invalid');
    if (method === 'list') {
      const available = currentRoots();
      const narrowed = workspaceMode && Object.hasOwn(message, 'workspaceScope');
      validate(message, narrowed ? ['method', 'workspaceScope'] : ['method']);
      const requested = narrowed ? parseWorkspaceRoots(message.workspaceScope) : available;
      if (narrowed && !covers(available, requested)) throw new Refusal('wrong_session', 403);
      return listSessions(requested);
    }
    validate(message, ['method', ...keys[method], ...(workspaceMode ? ['workspace'] : [])]);
    const sessionId = message.sessionId;
    const cwd = workspaceMode ? message.workspace : legacy.cwd;
    if (!validSessionId(sessionId) || !validWorkspace(cwd) || (!workspaceMode && sessionId !== legacy.sessionId)) {
      throw new Refusal('wrong_session', 403);
    }
    requireWorkspace(cwd);
    const target = { sessionId, cwd };
    if (method === 'prompt') return prompt(target, message);

    await requireTarget(target);
    requireWorkspace(cwd);
    if (method === 'state') return usePeer([sessionId], ['state'], (peer) => state(peer, sessionId));

    const claim = readClaim(target, message.requestId);
    if (!claim) throw new Refusal('unknown_request', 404);
    if (method === 'wait') {
      const { timeoutMs } = message;
      if (!Number.isSafeInteger(timeoutMs) || timeoutMs < 1 || timeoutMs > 30000) throw new Refusal('invalid');
      return usePeer([sessionId], ['wait'], async (peer) => {
        const waited = await rpc(peer, 'wait', { request: { sessionId, afterSeq: claim.cursor, timeoutMs, maxMessages: 2 } });
        return { reason: waited.reason, state: safeState(waited.state) };
      });
    }
    return usePeer([sessionId], ['state', 'events'], async (peer) => {
      const { cursor } = await state(peer, sessionId);
      const page = await rpc(peer, 'events', { request: {
        address: { kind: 'session', sessionId }, throughSeq: cursor,
        maxMessages: 8, turnWindow: { minMessages: 2, minTurns: 1 },
      } });
      return { cursor, ...visibleEvents(page, message.requestId, claim) };
    });
  }

  const server = createServer({ requestTimeout: 35000, headersTimeout: 5000, maxHeaderSize: 4096 }, async (req, res) => {
    let status = 200;
    let response;
    try {
      if (req.method !== 'POST' || req.url !== '/v1') throw new Refusal('invalid', 404);
      if (!authenticate(req.headers['x-dsh-bridge-token'], token)) throw new Refusal('unauthorized', 401);
      response = { ok: true, value: await dispatch(await readRequest(req)) };
    } catch (error) {
      status = error instanceof Refusal ? error.status : 500;
      response = { ok: false, error: error instanceof Refusal ? error.message : 'bridge_unavailable' };
    }
    res.writeHead(status, { 'Content-Type': 'application/json', 'Cache-Control': 'no-store', Connection: 'close' });
    res.end(JSON.stringify(response));
  });
  server.maxConnections = 8;
  server.on('connection', (connection) => {
    sockets.add(connection);
    connection.once('close', () => sockets.delete(connection));
  });

  async function stop() {
    stopping = true;
    for (const connection of sockets) connection.destroy();
    if (server.listening) await new Promise((resolve) => server.close(resolve));
    unbind();
    await Promise.allSettled(Array.from(nativePeers, (peer) => peer.dispose()));
  }

  try {
    await removeStaleSocket(socket);
    await new Promise((resolve, reject) => {
      server.once('error', reject);
      server.listen(socket, () => {
        server.removeListener('error', reject);
        resolve();
      });
    });
    chmodSync(socket, 0o600);
  } catch (error) {
    await stop();
    throw error;
  }
  ctx.effect(() => stop, 'feishu-session-bridge: close Unix socket');
}
