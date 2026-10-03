'use strict';

// One Node process owns all ephemeral rooms. No upload routes or media storage.
const express = require('express');
const http = require('node:http');
const path = require('node:path');
const { randomBytes, randomUUID } = require('node:crypto');
const { Server } = require('socket.io');
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
const snapshot = room => ({ ...room.state, time: position(room), at: Date.now(), media: room.media, hostId: room.hostId });
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
function compatible(a, b) {
  // Metadata is a sanity check, not proof of byte-identical files.
  return a && b && a.size === b.size && Math.abs(a.duration - b.duration) < 0.25;
}
function leave(socket) {
  const room = roomFor(socket);
  if (!room) return;
  room.users.delete(socket.id);
  socket.leave(channel(room.id));
  delete socket.data.roomId;
  if (!room.users.size) { rooms.delete(room.id); return; }
  if (room.hostId === socket.id) {
    // Prefer a participant with a matching file when transferring control.
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
  socket.on('media', (input, ack) => {
    const room = roomFor(socket);
    if (!room || !permit('media', 4, 1)) return reply(ack, { error: 'Cannot update file right now.' });
    const u = room.users.get(socket.id);
    if (input === null) { u.media = null; u.ready = false; u.buffering = false; }
    else {
      if (!input || !Number.isSafeInteger(input.size) || input.size <= 0 || !finite(input.duration) || input.duration <= 0 || input.duration > 604800)
        return reply(ack, { error: 'Invalid video metadata.' });
      u.media = { name: clean(input.name, 180), size: input.size, duration: input.duration };
      u.buffering = false;
      if (room.hostId === socket.id) {
        const changed = !compatible(room.media, u.media);
        room.media = u.media;
        if (changed) { room.state.desired = false; room.state.time = 0; room.state.playing = false; }
        for (const member of room.users.values()) member.ready = compatible(room.media, member.media);
      } else u.ready = compatible(room.media, u.media);
    }
    revise(room);
    publishUsers(room);
    io.to(channel(room.id)).emit('state', snapshot(room));
    reply(ack, { ok: true, ready: u.ready });
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

server.listen(process.env.PORT || 3000, '0.0.0.0', () => console.log('Watch Together signaling server ready'));
// Render restarts erase rooms. Clients report this instead of silently creating unrelated rooms.
process.on('SIGTERM', () => { io.close(); server.close(() => process.exit(0)); });
