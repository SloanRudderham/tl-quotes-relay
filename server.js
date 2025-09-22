// server.js — TL Quotes Relay (pure market data SSE)
require('dotenv').config();
const express = require('express');
const cors = require('cors');
const { io } = require('socket.io-client');

const app = express();
app.use(cors());
app.use(express.json());

// ----- Config -----
const SERVER = process.env.TL_SERVER || 'wss://api.tradelocker.com';
const TYPE   = process.env.TL_ENV    || 'LIVE';
const KEY    = process.env.TL_BRAND_KEY;
const PORT   = Number(process.env.PORT || 8080);

const HEARTBEAT_MS = Number(process.env.HEARTBEAT_MS || 1000);
const STALE_MS     = Number(process.env.STALE_MS || 120000);
const READ_TOKEN   = process.env.READ_TOKEN || ''; // optional

if (!KEY) { console.error('Missing TL_BRAND_KEY'); process.exit(1); }

// ----- State -----
const quotes = new Map(); // symbol -> { bid, ask, last, time }
const subs   = new Set(); // { res, filter:Set<string>, ping }
let lastBrandEventAt = 0;
let lastConnectError = '';

// ----- Helpers -----
const n = (v)=>{ const x = Number(v); return Number.isFinite(x) ? x : NaN; };
function first(obj, paths){
  for (const p of paths){
    const parts = p.split('.');
    let cur = obj;
    for (const k of parts){
      if (cur && Object.prototype.hasOwnProperty.call(cur, k)) cur = cur[k];
      else { cur = undefined; break; }
    }
    if (cur !== undefined && cur !== null && cur !== '') return cur;
  }
  return undefined;
}
function parseSymbolsParam(q){
  if (Array.isArray(q)) q = q.join(',');
  return new Set(String(q || '').split(',').map(s=>s.trim()).filter(Boolean));
}
function checkToken(req, res){
  if (!READ_TOKEN) return true;
  const provided = String(req.query.token || req.headers['x-read-token'] || '');
  if (provided === READ_TOKEN) return true;
  res.status(401).json({ ok:false, error:'unauthorized' }); return false;
}
function pushQuote(symbol){
  const q = quotes.get(symbol); if (!q) return;
  const line = `event: quotes\ndata: ${JSON.stringify({ serverTime: Date.now(), symbol, quote: q 
})}\n\n`;
  for (const sub of subs){
    if (sub.filter.size && !sub.filter.has(symbol)) continue;
    sub.res.write(line);
  }
}
function touchQuote(symbol, patch){
  if (!symbol) return;
  const cur = quotes.get(symbol) || {};
  const now = Date.now();
  const out = {
    bid: Number.isFinite(patch.bid)  ? patch.bid  : cur.bid,
    ask: Number.isFinite(patch.ask)  ? patch.ask  : cur.ask,
    last: Number.isFinite(patch.last)? patch.last : cur.last,
    time: patch.time || now,
  };
  quotes.set(symbol, out);
  pushQuote(symbol);
}
function processQuoteLikeEvent(m){
  const src = m?.quote || m?.data?.quote || m?.payload?.quote || m?.payload || m;
  const symbol = String(first(src, ['symbol','instrument','symbolName','s','Symbol']) || '');
  if (!symbol) return;

  const bid  = n(first(src, ['bid','b','Bid','BidPrice']));
  const ask  = n(first(src, ['ask','a','Ask','AskPrice']));
  const last = n(first(src, ['last','price','p','mark','Last','LastPrice']));
  const time = Number(first(src, ['time','ts','timestamp','Time','Timestamp'])) || Date.now();

  touchQuote(symbol, {
    bid: Number.isFinite(bid) ? bid : undefined,
    ask: Number.isFinite(ask) ? ask : undefined,
    last: Number.isFinite(last) ? last : undefined,
    time,
  });
}

// ----- Brand socket -----
const socket = io(SERVER + '/brand-socket', {
  path: '/brand-api/socket.io',
  transports: ['websocket','polling'],
  query: { type: TYPE },
  extraHeaders: { 'brand-api-key': KEY },
  reconnection: true,
  reconnectionAttempts: Infinity,
  reconnectionDelay: 1000,
  reconnectionDelayMax: 10000,
  randomizationFactor: 0.5,
  timeout: 20000,
  forceNew: true,
});

socket.on('connect', () => { console.log('[BrandSocket] connected', socket.id); lastConnectError 
= ''; });
socket.on('disconnect', r => console.warn('[BrandSocket] disconnected', r));
socket.on('connect_error', (err) => {
  lastConnectError = (err && (err.message || String(err))) || 'connect_error';
  console.error('[BrandSocket] connect_error', lastConnectError, { description: err?.description, 
context: err?.context, data: err?.data });
});
socket.on('error', e => console.error('[BrandSocket] error', e));

socket.on('stream', (m) => {
  lastBrandEventAt = Date.now();
  const t = (m?.type || '').toString().toUpperCase();
  if (t.includes('QUOTE') || t.includes('TICK') || t.includes('PRIC') || t.includes('MARKET') || 
m?.quote || m?.data?.quote || m?.payload?.quote){
    processQuoteLikeEvent(m);
  }
});

setInterval(() => {
  if (!STALE_MS) return;
  if (!socket.connected) {
    const age = Date.now() - (lastBrandEventAt || 0);
    if (age > STALE_MS) { console.warn('[Watchdog] stale, exiting'); process.exit(1); }
  }
}, 30000);

// ----- Routes -----
// Live quotes SSE
app.get('/quotes/stream', (req, res) => {
  if (!checkToken(req, res)) return;

  const filter = parseSymbolsParam(req.query.symbols || req.query['symbols[]']);
  res.writeHead(200, {
    'Content-Type': 'text/event-stream',
    'Cache-Control': 'no-cache, no-transform',
    'Connection': 'keep-alive',
    'Access-Control-Allow-Origin': '*',
    'X-Accel-Buffering': 'no',
  });
  res.write(`event: hello\ndata: ${JSON.stringify({ ok:true, env: TYPE })}\n\n`);

  // initial snapshot (respect filter)
  const all = {};
  for (const [s, q] of quotes.entries()) {
    if (filter.size && !filter.has(s)) continue;
    all[s] = q;
  }
  res.write(`event: quotes\ndata: ${JSON.stringify({ serverTime: Date.now(), all })}\n\n`);

  const sub = { res, filter, ping: setInterval(() => res.write(`: ping\n\n`), HEARTBEAT_MS) };
  subs.add(sub);
  req.on('close', () => { clearInterval(sub.ping); subs.delete(sub); });
});

// JSON snapshot
app.get('/quotes/state', (req, res) => {
  if (!checkToken(req, res)) return;
  const filter = parseSymbolsParam(req.query.symbols || req.query['symbols[]']);
  const out = {};
  for (const [s, q] of quotes.entries()) {
    if (filter.size && !filter.has(s)) continue;
    out[s] = q;
  }
  res.json({ serverTime: Date.now(), count: Object.keys(out).length, quotes: out });
});

// Health
app.get('/health', (_req,res)=> res.json({ ok:true, env: TYPE, symbols: quotes.size }));
app.get('/brand/status', (_req,res)=> res.json({
  env: TYPE, server: SERVER, connected: socket.connected, symbols: quotes.size,
  lastBrandEventAt, lastConnectError, now: Date.now(),
}));

process.on('unhandledRejection', (r)=>console.error('[unhandledRejection]', r));
process.on('uncaughtException', (e)=>{ console.error('[uncaughtException]', e); process.exit(1); 
});

app.listen(PORT, ()=> console.log(`Quotes relay listening on :${PORT}`));

