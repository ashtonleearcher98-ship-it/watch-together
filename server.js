'use strict';

// One Node process owns ephemeral rooms and temporary MP4 uploads.
const express = require('express');
const http = require('node:http');
const path = require('node:path');
const { randomBytes, randomUUID } = require('node:crypto');
const { Server } = require('socket.io');
const fs = require('node:fs');
const fsp = require('node:fs/promises');
const os = require('node:os');
const { Transform } = require('node:stream');
const { pipeline } = require('node:stream/promises');
// A private per-process directory avoids exposing uploads through static assets.
const uploadDir = fs.mkdtempSync(path.join(os.tmpdir(), 'watch-party-'));
const MAX_FILE = 1024 * 1024 * 1024; // 1 GiB per MP4; bounded for hobby hosting.
const MAX_STORAGE = 2 * MAX_FILE; // Global budget includes in-flight reservations.
let reservedBytes = 0;
const uploads = new Map();
function releaseUpload(upload) {
  if (!upload || upload.released) return;
  upload.released = true; reservedBytes -= upload.size;
  uploads.delete(upload.token); clearTimeout(upload.timer);
  upload.abort?.();
  fsp.rm(upload.path, { force: true }).catch(() => {});
}
function releaseMedia(media) {
  if (!media || media.released) return;
  media.released = true; reservedBytes -= media.size;
  fsp.rm(media.path, { force: true }).catch(() => {});
}
const app = express();
const server = http.createServer(app);
const io = new Server(server, {
  maxHttpBufferSize: 16 * 1024, // Reject large signaling packets, including media.
  perMessageDeflate: false,
  // Same-origin browser connections only. Non-browser clients still require validation below.
  allowRequest(req, done) {
    const origin = req.headers.origin;
    try { done(null, !origin || new URL(origin).host === req.headers.host); }
    catch { done(null, false); }
  }
});
app.disable('x-powered-by');
app.use((req, res, next) => {
  res.setHeader('X-Content-Type-Options', 'nosniff');
  res.setHeader('Referrer-Policy', 'no-referrer');
  res.setHeader('Permissions-Policy', 'camera=(), microphone=()');
  next();
});
app.get('/health', (req, res) => res.json({ ok: true }));
// Only the frontend is published, never repository files or local videos.
app.use(express.static(path.join(__dirname, 'public'), { dotfiles: 'deny' }));

const rooms = new Map();
const MAX_ROOMS = 500, MAX_USERS = 12;
const clean = (s, n) => typeof s === 'string' ? s.replace(/[\u0000-\u001f\u007f]/g, '').trim().slice(0, n) : '';
const finite = n => typeof n === 'number' && Number.isFinite(n);
const channel = id => `party:${id}`;
const roomFor = socket => rooms.get(socket.data.roomId);
const position = room => Math.min(room.media?.duration || 0,
  room.state.time + (room.state.playing ? (Date.now() - room.state.at) / 1000 : 0));
// Never publish filesystem paths or write credentials.
const snapshot = room => ({ ...room.state, time: position(room), at: Date.now(),
  media: room.media ? { name: room.media.name, size: room.media.size, duration: room.media.duration, key: room.media.key, url: `/media/${room.id}/${room.media.key}` } : null, hostId: room.hostId });
const publishUsers = room => io.to(channel(room.id)).emit('users', {
  hostId: room.hostId,
  users: [...room.users.values()].map(u => ({ id: u.id, name: u.name, ready: u.ready, buffering: u.buffering }))
});
function revise(room, time = position(room)) {
  // Desired playback and effective playback differ while anyone is buffering/unready.
  const blocked = [...room.users.values()].some(u => !u.ready || u.buffering);
  room.state = { time, desired: room.state.desired, playing: room.state.desired && !blocked,
    blocked, at: Date.now(), revision: room.state.revision + 1 };
}
function leave(socket) {
  const room = roomFor(socket);
  if (!room) return;
  room.users.delete(socket.id);
  socket.leave(channel(room.id));
  delete socket.data.roomId;
  if (!room.users.size) { releaseUpload(room.upload); releaseMedia(room.media); rooms.delete(room.id); return; }
  if (room.hostId === socket.id) {
    releaseUpload(room.upload); room.upload = null;
    // Prefer someone whose shared video is already ready.
    room.hostId = [...room.users.values()].find(u => u.ready)?.id || room.users.keys().next().value;
  }
  revise(room);
  publishUsers(room);
  io.to(channel(room.id)).emit('state', snapshot(room));
}

io.on('connection', socket => {
  // Per-connection token buckets bound chat/control bursts and creation spam.
  const buckets = new Map();
  function permit(key, capacity = 30, refill = 10) {
    const now = Date.now(), old = buckets.get(key) || { tokens: capacity, at: now };
    old.tokens = Math.min(capacity, old.tokens + (now - old.at) * refill / 1000);
    old.at = now;
    const allowed = old.tokens >= 1;
    if (allowed) old.tokens--;
    buckets.set(key, old);
    return allowed;
  }
  const reply = (ack, value) => { if (typeof ack === 'function') ack(value); };
  socket.on('enter', (input, ack) => {
    if (!permit('enter', 4, 0.2)) return reply(ack, { error: 'Please wait before entering another room.' });
    if (!input || typeof input !== 'object') return reply(ack, { error: 'Invalid request.' });
    const name = clean(input.name, 32) || 'Guest';
    let room;
    if (input.create === true) {
      if (rooms.size >= MAX_ROOMS) return reply(ack, { error: 'Server busy. Try later.' });
      let id;
      do { id = randomBytes(9).toString('hex').toUpperCase(); } while (rooms.has(id));
      room = { id, hostId: socket.id, users: new Map(), media: null, chat: [],
        state: { time: 0, desired: false, playing: false, blocked: true, at: Date.now(), revision: 0 } };
    } else {
      const id = clean(input.roomId, 18).toUpperCase();
      if (!/^[A-F0-9]{18}$/.test(id) || !(room = rooms.get(id)))
        return reply(ack, { error: 'Room not found. It may have closed or the server restarted.' });
      if (room.users.size >= MAX_USERS && !room.users.has(socket.id)) return reply(ack, { error: 'Room is full (12 people).' });
      if (room.users.has(socket.id)) return reply(ack, { error: 'Already in this room.' });
    }
    leave(socket);
    rooms.set(room.id, room);
    socket.data.roomId = room.id;
    room.users.set(socket.id, { id: socket.id, name, ready: false, buffering: false, media: null });
    socket.join(channel(room.id));
    revise(room);
    reply(ack, { roomId: room.id, state: snapshot(room), chat: room.chat });
    publishUsers(room);
    socket.to(channel(room.id)).emit('state', snapshot(room));
  });
  socket.on('upload-start', (input, ack) => {
    const room = roomFor(socket);
    if (!room || socket.id !== room.hostId || !permit('upload', 2, 0.1))
      return reply(ack, { error: 'Only the host can upload. Please wait between uploads.' });
    if (room.upload && !room.upload.released) return reply(ack, { error: 'An upload is already in progress.' });
    if (!input || !Number.isSafeInteger(input.size) || input.size <= 0 || input.size > MAX_FILE ||
        !finite(input.duration) || input.duration <= 0 || input.duration > 604800 || !/\.mp4$/i.test(input.name || ''))
      return reply(ack, { error: 'Choose a valid MP4 no larger than 1 GiB.' });
    if (reservedBytes + input.size > MAX_STORAGE) return reply(ack, { error: 'Temporary video storage is full. Try after another room closes.' });
    const token = randomBytes(32).toString('hex');
    const upload = { token, roomId: room.id, hostId: socket.id, name: clean(input.name, 180),
      duration: input.duration, size: input.size, path: path.join(uploadDir, token + '.mp4'), started: false };
    reservedBytes += upload.size; room.upload = upload; uploads.set(token, upload);
    upload.timer = setTimeout(() => releaseUpload(upload), 30 * 60 * 1000);
    upload.timer.unref();
    reply(ack, { url: `/upload/${token}`, maxBytes: MAX_FILE });
  });
  socket.on('ready', (input, ack) => {
    const room = roomFor(socket);
    if (!room || !permit('ready', 10, 3) || !input || typeof input.ready !== 'boolean' || input.key !== room.media?.key)
      return reply(ack, { error: 'Video has changed. Reload the shared video.' });
    const member = room.users.get(socket.id);
    member.ready = input.ready; member.buffering = false;
    revise(room); publishUsers(room);
    io.to(channel(room.id)).emit('state', snapshot(room));
    reply(ack, { ok: true });
  });
  socket.on('control', (input, ack) => {
    const room = roomFor(socket);
    if (!room || !permit('control')) return reply(ack, { error: 'Cannot control playback right now.' });
    if (socket.id !== room.hostId) return reply(ack, { error: 'Only the host controls playback.' });
    if (!room.media || !room.users.get(socket.id).ready || !input || !['play', 'pause', 'seek'].includes(input.action) || !finite(input.time))
      return reply(ack, { error: 'Invalid playback command.' });
    if (input.action === 'play') room.state.desired = true;
    if (input.action === 'pause') room.state.desired = false;
    revise(room, Math.max(0, Math.min(room.media.duration, input.time)));
    // Action goes to OTHER room members only. The origin receives an authoritative ACK.
    const state = snapshot(room);
    socket.to(channel(room.id)).emit('state', state);
    reply(ack, { state });
  });
  socket.on('buffer', value => {
    const room = roomFor(socket);
    if (!room || typeof value !== 'boolean' || !permit('buffer', 12, 4)) return;
    const u = room.users.get(socket.id);
    if (!u.ready || u.buffering === value) return;
    u.buffering = value;
    revise(room);
    publishUsers(room);
    io.to(channel(room.id)).emit('state', snapshot(room));
  });
  socket.on('chat', (value, ack) => {
    const room = roomFor(socket), text = clean(value, 1000);
    if (!room || !text || !permit('chat', 5, 1)) return reply(ack, { error: 'Message empty or sent too quickly.' });
    const message = { id: randomUUID(), userId: socket.id, name: room.users.get(socket.id).name, text, at: Date.now() };
    room.chat.push(message);
    if (room.chat.length > 100) room.chat.shift();
    socket.to(channel(room.id)).emit('chat', message);
    reply(ack, { message });
  });
  // Periodic state requests correct drift and missed packets without syncing video bytes.
  socket.on('sync', ack => {
    const room = roomFor(socket);
    if (room && permit('sync', 6, 2)) reply(ack, { state: snapshot(room) });
  });
  socket.on('clock', ack => { if (permit('clock', 5, 1)) reply(ack, Date.now()); });
  socket.on('leave', ack => { leave(socket); reply(ack, { ok: true }); });
  socket.on('disconnect', () => leave(socket));
});

// Raw-body uploads are piped to disk with backpressure; no full-video RAM buffer.
app.put('/upload/:token', async (req, res) => {
  const upload = uploads.get(req.params.token);
  const room = upload && rooms.get(upload.roomId);
  if (!upload || upload.started || upload.released || !room || room.hostId !== upload.hostId)
    return res.status(403).json({ error: 'Upload expired or not authorized.' });
  if (req.headers['content-type'] !== 'video/mp4' || Number(req.headers['content-length']) !== upload.size) {
    releaseUpload(upload);
    return res.status(400).json({ error: 'Upload must contain the selected MP4 bytes.' });
  }
  upload.started = true;
  const controller = new AbortController();
  upload.abort = () => controller.abort();
  req.setTimeout(60000, () => controller.abort());
  let received = 0;
  const limit = new Transform({ transform(chunk, encoding, callback) {
    received += chunk.length;
    if (received > upload.size) callback(new Error('File too large.'));
    else callback(null, chunk);
  }});
  try {
    await pipeline(req, limit, fs.createWriteStream(upload.path, { flags: 'wx' }), { signal: controller.signal });
    if (received !== upload.size || rooms.get(room.id) !== room || room.hostId !== upload.hostId || upload.released)
      throw new Error('Upload interrupted or room closed.');
    const handle = await fsp.open(upload.path, 'r');
    const header = Buffer.alloc(12);
    try { await handle.read(header, 0, 12, 0); } finally { await handle.close(); }
    if (header.toString('ascii', 4, 8) !== 'ftyp') throw new Error('This file is not an MP4 container.');
    releaseMedia(room.media);
    // Move the reservation from an upload into the active video without double-counting.
    clearTimeout(upload.timer); uploads.delete(upload.token); upload.released = true; upload.abort = null;
    room.upload = null;
    room.media = { name: upload.name, size: upload.size, duration: upload.duration,
      key: randomBytes(24).toString('hex'), path: upload.path };
    for (const member of room.users.values()) { member.ready = false; member.buffering = false; }
    room.state.desired = false; room.state.playing = false;
    revise(room, 0); publishUsers(room);
    io.to(channel(room.id)).emit('state', snapshot(room));
    res.json({ ok: true });
  } catch (error) {
    releaseUpload(upload);
    await fsp.rm(upload.path, { force: true }).catch(() => {});
    if (!res.headersSent && !res.destroyed) res.status(400).json({ error: 'Upload failed. Select a valid MP4 and try again.' });
  }
});
// Unpredictable per-video capability URLs. Express handles Range/HEAD/206/416,
// enabling timeline scrubbing without downloading the entire movie first.
app.get('/media/:roomId/:key', (req, res) => {
  const media = rooms.get(req.params.roomId)?.media;
  if (!media || req.params.key !== media.key) return res.sendStatus(404);
  res.setHeader('Cache-Control', 'private, no-store');
  res.setHeader('Content-Type', 'video/mp4');
  res.sendFile(media.path, { acceptRanges: true }, error => {
    if (error && !res.headersSent) res.status(error.statusCode || 500).end();
  });
});
server.requestTimeout = 0; // Large uploads use an inactivity timeout instead.
server.listen(process.env.PORT || 3000, '0.0.0.0', () => console.log('Watch Together upload server ready'));
// Rooms and uploads disappear on Render restarts. No persistent disk required.
process.on('SIGTERM', () => { io.close(); server.close(() => {
  fs.rmSync(uploadDir, { recursive: true, force: true }); process.exit(0);
}); });
