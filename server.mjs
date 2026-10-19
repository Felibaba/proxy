// Piper proxy: CORS + automatic chunking + streaming + chunk cache.
// No npm dependencies. Needs Node 18+.
//
//   POST /tts          JSON {"text","length_scale","voice","gap_ms"}  -> one finished WAV
//   POST|GET /tts/stream   same fields (GET uses ?text=...)          -> WAV that starts
//                          playing as soon as the first chunk is ready
//   GET  /warm         wakes the Piper service (use before a session)
//   GET  /health
//
// Env:
//   PIPER_URL       e.g. https://postira.onrender.com/
//   ALLOWED_ORIGIN  site allowed to call this (default "*")
//   API_KEY         optional; send as header "x-api-key" or query "?key="
//   CHUNK_CHARS     target chunk size (default 250)

import http from "node:http";
import crypto from "node:crypto";

const PIPER_URL = process.env.PIPER_URL;
const ALLOWED_ORIGIN = process.env.ALLOWED_ORIGIN || "*";
const API_KEY = process.env.API_KEY || "";
const PORT = Number(process.env.PORT || 3000);
const CHUNK_CHARS = Number(process.env.CHUNK_CHARS || 250);

const MAX_TOTAL_CHARS = 10_000;          // per request
const MAX_BODY = 64 * 1024;
const MAX_PENDING = 3;                   // requests waiting in line
const CACHE_MAX_BYTES = 40 * 1024 * 1024;
const PARA_PAUSE_MS = 400;               // pause after a paragraph
const REQUEST_TIMEOUT_MS = 150_000;      // covers a Piper cold start

if (!PIPER_URL) {
  console.error("Set the PIPER_URL environment variable.");
  process.exit(1);
}

const cors = {
  "Access-Control-Allow-Origin": ALLOWED_ORIGIN,
  "Access-Control-Allow-Methods": "GET, POST, OPTIONS",
  "Access-Control-Allow-Headers": "Content-Type, x-api-key",
  "Access-Control-Expose-Headers": "Content-Type, Content-Length, X-Chunks",
};

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
function send(res, status, msg) {
  res.writeHead(status, { ...cors, "Content-Type": "text/plain" }).end(msg);
}

// ---------- one synthesis at a time (protects Piper's RAM) ----------
let queue = Promise.resolve();
let pending = 0;
function runExclusive(fn) {
  const p = queue.then(fn);
  queue = p.catch(() => {});
  return p;
}

// ---------- text splitting ----------
function hardSplit(s) {
  const out = [];
  let cur = "";
  for (const w of s.split(/\s+/)) {
    if ((cur + " " + w).trim().length > CHUNK_CHARS && cur) {
      out.push(cur);
      cur = w;
    } else {
      cur = (cur ? cur + " " : "") + w;
    }
  }
  if (cur) out.push(cur);
  return out;
}

// Returns [{text, para}] where para=true marks the end of a paragraph.
function splitText(text) {
  const paras = text.replace(/\r/g, "").split(/\n+/).map((s) => s.trim()).filter(Boolean);
  const chunks = [];
  for (const p of paras) {
    const start = chunks.length;
    const sentences = p.match(/[^.!?â€¦]+(?:[.!?â€¦]+["')\]]*|$)\s*/g) || [p];
    let cur = "";
    for (let s of sentences) {
      s = s.trim();
      if (!s) continue;
      if (s.length > CHUNK_CHARS) {
        if (cur) { chunks.push({ text: cur, para: false }); cur = ""; }
        for (const piece of hardSplit(s)) chunks.push({ text: piece, para: false });
        continue;
      }
      if (cur && (cur + " " + s).length > CHUNK_CHARS) {
        chunks.push({ text: cur, para: false });
        cur = s;
      } else {
        cur = cur ? cur + " " + s : s;
      }
    }
    if (cur) chunks.push({ text: cur, para: false });
    if (chunks.length > start) chunks[chunks.length - 1].para = true;
  }
  return chunks;
}

// ---------- WAV helpers ----------
function parseWav(buf) {
  let off = 12, rate = 22050, ch = 1, bits = 16;
  while (off + 8 <= buf.length) {
    const id = buf.toString("ascii", off, off + 4);
    let size = buf.readUInt32LE(off + 4);
    if (id === "fmt ") {
      ch = buf.readUInt16LE(off + 10);
      rate = buf.readUInt32LE(off + 12);
      bits = buf.readUInt16LE(off + 22);
    } else if (id === "data") {
      const start = off + 8;
      if (size === 0xffffffff || size > buf.length - start) size = buf.length - start;
      return { rate, ch, bits, pcm: Buffer.from(buf.subarray(start, start + size)) };
    }
    off += 8 + size + (size & 1);
  }
  throw new Error("bad wav from piper");
}

// dataLen === null => unknown length (streaming)
function wavHeader({ rate, ch, bits }, dataLen) {
  const b = Buffer.alloc(44);
  const unknown = dataLen === null;
  b.write("RIFF", 0, "ascii");
  b.writeUInt32LE(unknown ? 0xffffffff : 36 + dataLen, 4);
  b.write("WAVEfmt ", 8, "ascii");
  b.writeUInt32LE(16, 16);
  b.writeUInt16LE(1, 20);
  b.writeUInt16LE(ch, 22);
  b.writeUInt32LE(rate, 24);
  b.writeUInt32LE((rate * ch * bits) / 8, 28);
  b.writeUInt16LE((ch * bits) / 8, 32);
  b.writeUInt16LE(bits, 34);
  b.write("data", 36, "ascii");
  b.writeUInt32LE(unknown ? 0xffffffff : dataLen, 40);
  return b;
}

const silence = (f, ms) =>
  Buffer.alloc(Math.round((f.rate * ms) / 1000) * f.ch * (f.bits / 8));

// ---------- chunk cache (edited scripts only re-render changed chunks) ----------
const cache = new Map();
let cacheBytes = 0;
function cacheGet(k) {
  const v = cache.get(k);
  if (v) { cache.delete(k); cache.set(k, v); }
  return v;
}
function cacheSet(k, v) {
  if (cache.has(k)) { cacheBytes -= cache.get(k).pcm.length; cache.delete(k); }
  cache.set(k, v);
  cacheBytes += v.pcm.length;
  while (cacheBytes > CACHE_MAX_BYTES && cache.size > 1) {
    const [ok, ov] = cache.entries().next().value;
    cache.delete(ok);
    cacheBytes -= ov.pcm.length;
  }
}

// ---------- talk to Piper ----------
async function getChunk(chunk, opts) {
  const payload = { text: chunk.text, ...opts };
  const key = crypto.createHash("sha1").update(JSON.stringify(payload)).digest("hex");
  const hit = cacheGet(key);
  if (hit) return hit;

  let lastErr;
  for (let attempt = 0; attempt < 2; attempt++) {
    try {
      const r = await fetch(PIPER_URL, {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });
      if (!r.ok) throw new Error(`piper returned ${r.status}`);
      const parsed = parseWav(Buffer.from(await r.arrayBuffer()));
      cacheSet(key, parsed);
      return parsed;
    } catch (e) {
      lastErr = e;
      await sleep(500);
    }
  }
  throw lastErr;
}

async function warm() {
  const r = await fetch(new URL("voices", PIPER_URL), {
    signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
  });
  return r.ok;
}

// ---------- request handling ----------
function readBody(req) {
  return new Promise((resolve, reject) => {
    let size = 0;
    const chunks = [];
    req.on("data", (c) => {
      size += c.length;
      if (size > MAX_BODY) {
        reject(new Error("too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => resolve(Buffer.concat(chunks).toString("utf8")));
    req.on("error", reject);
  });
}

async function handleSynth(req, res, url, stream) {
  let body;
  if (req.method === "GET") {
    body = Object.fromEntries(url.searchParams);
  } else {
    const raw = await readBody(req);
    try { body = JSON.parse(raw); } catch { body = { text: raw }; }
  }

  const text = String(body.text ?? "").trim();
  if (!text) return send(res, 400, "empty text");
  if (text.length > MAX_TOTAL_CHARS) return send(res, 413, `max ${MAX_TOTAL_CHARS} characters`);

  const opts = {};
  const ls = Number(body.length_scale);
  if (ls >= 0.5 && ls <= 2) opts.length_scale = ls;
  if (typeof body.voice === "string" && /^[\w.-]+$/.test(body.voice)) opts.voice = body.voice;

  let gap = Number(body.gap_ms);
  if (!Number.isFinite(gap)) gap = 150;
  gap = Math.min(Math.max(gap, 0), 1500);

  const chunks = splitText(text);
  if (pending >= MAX_PENDING) {
    res.setHeader("Retry-After", "10");
    return send(res, 429, "busy, retry shortly");
  }

  pending++;
  let closed = false;
  res.on("close", () => { closed = true; }); // caller hung up: stop doing work
  try {
    if (!stream) {
      const parts = [];
      let fmt = null;
      await runExclusive(async () => {
        for (const c of chunks) {
          if (closed) return;
          const r = await getChunk(c, opts);
          fmt ??= r;
          parts.push(r.pcm, silence(r, c.para ? PARA_PAUSE_MS : gap));
        }
      });
      if (closed || !fmt) return;
      const pcm = Buffer.concat(parts);
      const headers = {
        ...cors,
        "Content-Type": "audio/wav",
        "Content-Length": 44 + pcm.length,
        "X-Chunks": String(chunks.length),
      };
      if (body.download) headers["Content-Disposition"] = 'attachment; filename="speech.wav"';
      res.writeHead(200, headers);
      res.end(Buffer.concat([wavHeader(fmt, pcm.length), pcm]));
    } else {
      let started = false;
      await runExclusive(async () => {
        for (const c of chunks) {
          if (closed) return;
          const r = await getChunk(c, opts);
          if (!started) {
            res.writeHead(200, {
              ...cors,
              "Content-Type": "audio/wav",
              "Cache-Control": "no-store",
              "X-Accel-Buffering": "no",
              "X-Chunks": String(chunks.length),
            });
            res.write(wavHeader(r, null));
            started = true;
          }
          res.write(Buffer.concat([r.pcm, silence(r, c.para ? PARA_PAUSE_MS : gap)]));
        }
      });
      if (started) res.end();
    }
  } finally {
    pending--;
  }
}

const server = http.createServer(async (req, res) => {
  try {
    const url = new URL(req.url, "http://localhost");
    const path = url.pathname;

    if (req.method === "OPTIONS") return void res.writeHead(204, cors).end();
    if (req.method === "GET" && path === "/health") return send(res, 200, "ok");
    if (req.method === "GET" && path === "/warm") {
      return send(res, (await warm()) ? 200 : 502, "warm");
    }

    const isSynth = path === "/tts" && (req.method === "POST" || req.method === "GET");
    const isStream = path === "/tts/stream" && (req.method === "POST" || req.method === "GET");
    if (!isSynth && !isStream) return send(res, 404, "not found");

    const key = req.headers["x-api-key"] || url.searchParams.get("key");
    if (API_KEY && key !== API_KEY) return send(res, 401, "unauthorized");

    await handleSynth(req, res, url, isStream);
  } catch (err) {
    console.error(err.message);
    if (res.headersSent) res.destroy();
    else send(res, err.message === "too large" ? 413 : 500, "synthesis failed");
  }
});

// Longer than Render's load balancer idle timeout, avoids random 502s
server.keepAliveTimeout = 65_000;
server.headersTimeout = 66_000;

server.listen(PORT, () => {
  console.log(`Proxy on :${PORT} -> ${PIPER_URL}`);
  warm().catch(() => {}); // wake Piper when the proxy starts
});
