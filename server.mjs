// Foldbook Server : tourne sur le NAS, à côté des livres. Il les rend lisibles par les personnes
// que le propriétaire invite, où qu'elles soient, en passant par le relais (connexion sortante :
// aucun port à ouvrir sur la box). Sur le réseau local, il répond aussi en direct, port 8686.
//
// Il parle un petit WebDAV (PROPFIND, HEAD, GET avec plages d'octets) : l'app lit une
// bibliothèque partagée exactement comme un serveur WebDAV, CBZ page par page compris.
// Chaque requête doit porter le jeton du compte Foldbook (Authorization: Bearer …) ; le
// serveur vérifie le compte auprès de Supabase, puis ce que la personne a le droit de voir.
import fs from 'node:fs';
import fsp from 'node:fs/promises';
import http from 'node:http';
import os from 'node:os';
import path from 'node:path';
import { Readable } from 'node:stream';
import WebSocket from 'ws';

const VERSION = '1.0.3';
const LIBRARY = path.resolve(process.env.LIBRARY_DIR ?? '/library');
const DATA = path.resolve(process.env.DATA_DIR ?? '/data');
const PORT = Number(process.env.PORT ?? 8686);
const NAME = process.env.SERVER_NAME ?? os.hostname();
const RELAY = process.env.RELAY_URL ?? 'wss://relay.foldbook.app/connect';
const SUPABASE_URL = process.env.SUPABASE_URL ?? 'https://zvglqyjsxixuqepvfawz.supabase.co';
const SUPABASE_KEY = process.env.SUPABASE_KEY ?? 'sb_publishable_0viM9HcMprjLPwhiLG1U9A_2uBwvrU4';

/** Ce que Synology et les systèmes sèment dans les dossiers, jamais montré. */
const SKIP = (name) => name.startsWith('.') || name.startsWith('@') || name.startsWith('#') || name === 'lost+found';

const log = (...args) => console.log(new Date().toISOString(), ...args);

/* ——————————————————— L'identité du serveur ——————————————————— */

const identityFile = path.join(DATA, 'server.json');
let identity = null;
/** Ce que Supabase dit au dernier battement : propriétaire, code d'association, invités. */
let state = { owner: null, name: NAME, pair_code: null, shared: null, members: [] };

async function rpc(fn, args, token) {
  const res = await fetch(`${SUPABASE_URL}/rest/v1/rpc/${fn}`, {
    method: 'POST',
    headers: {
      apikey: SUPABASE_KEY,
      'content-type': 'application/json',
      ...(token ? { authorization: `Bearer ${token}` } : {}),
    },
    body: JSON.stringify(args),
  });
  if (!res.ok) throw new Error(`${fn} : ${res.status} ${await res.text()}`);
  return res.json();
}

async function loadIdentity() {
  try {
    identity = JSON.parse(await fsp.readFile(identityFile, 'utf8'));
  } catch {
    const r = await rpc('foldbook_server_register', { p_name: NAME, p_version: VERSION });
    identity = { id: r.id, secret: r.secret };
    await fsp.mkdir(DATA, { recursive: true });
    await fsp.writeFile(identityFile, JSON.stringify(identity, null, 2), { mode: 0o600 });
    log('Nouveau serveur enregistré :', identity.id);
  }
}

/** Ce que Foldbook sait lire. */
const BOOK = /\.(cbz|cbr|cb7|cbt|pdf|epub)$/i;

/**
 * Un dossier contient-il des livres ? On regarde jusqu'à quatre niveaux, sans dépasser
 * quelques milliers d'entrées : un dossier de films ou de téléchargements n'en a pas.
 */
async function hasBooks(dir, depth = 4, budget = { left: 4000 }) {
  const entries = await fsp.readdir(dir, { withFileTypes: true }).catch(() => []);
  const subdirs = [];
  for (const e of entries) {
    if (--budget.left < 0) return false;
    if (SKIP(e.name)) continue;
    if (e.isFile() && BOOK.test(e.name)) return true;
    if (e.isDirectory()) subdirs.push(e.name);
  }
  if (depth <= 1) return false;
  for (const name of subdirs) if (await hasBooks(path.join(dir, name), depth - 1, budget)) return true;
  return false;
}

/**
 * Les dossiers de la racine, chacun avec « contient des livres » ou non, recalculés toutes
 * les 10 minutes. Le propriétaire choisit dans l'app ceux qu'il prête (`state.shared`) ; la
 * liste à cocher part de ceux qui contiennent des livres.
 */
let folders = { list: [], at: 0 };
async function libraries() {
  if (Date.now() - folders.at > 10 * 60_000) {
    const entries = await fsp.readdir(LIBRARY, { withFileTypes: true }).catch(() => []);
    const list = [];
    for (const e of entries) {
      if (e.isDirectory() && !SKIP(e.name)) list.push({ name: e.name, books: await hasBooks(path.join(LIBRARY, e.name)) });
    }
    folders = { list: list.sort((a, b) => a.name.localeCompare(b.name)), at: Date.now() };
  }
  return folders.list;
}

/**
 * Les dossiers prêtés : ceux que le propriétaire a cochés. Tant qu'il n'a rien choisi,
 * rien n'est prêté, à personne : un dossier de films ou de téléchargements ne part jamais
 * par défaut.
 */
const lentNames = () => new Set(state.shared ?? []);

async function heartbeat() {
  try {
    state = await rpc('foldbook_server_heartbeat', {
      p_id: identity.id,
      p_secret: identity.secret,
      p_version: VERSION,
      p_libraries: await libraries(),
    });
  } catch (error) {
    // Un serveur effacé (compte supprimé, serveur retiré) repart de zéro avec un nouveau code.
    if (String(error).includes('unknown server')) {
      await fsp.rm(identityFile, { force: true });
      await loadIdentity();
      relay?.terminate();
    } else log('Battement manqué :', String(error));
  }
}

/* ——————————————————— Les comptes et les droits ——————————————————— */

const tokens = new Map();

/** Le compte derrière un jeton, vérifié par Supabase et gardé 5 minutes. */
async function userOf(header) {
  const token = header?.match(/^Bearer\s+(.+)$/i)?.[1];
  if (!token) return null;
  // Pour les essais en local seulement : un jeton fixe, si la variable est posée.
  if (process.env.FOLDBOOK_DEV_TOKEN && token === process.env.FOLDBOOK_DEV_TOKEN) return process.env.FOLDBOOK_DEV_USER;
  const known = tokens.get(token);
  if (known && known.until > Date.now()) return known.id;
  const res = await fetch(`${SUPABASE_URL}/auth/v1/user`, {
    headers: { apikey: SUPABASE_KEY, authorization: `Bearer ${token}` },
  });
  if (!res.ok) return null;
  const { id } = await res.json();
  if (tokens.size > 500) tokens.clear();
  tokens.set(token, { id, until: Date.now() + 5 * 60_000 });
  return id;
}

/**
 * Ce qu'une personne peut voir : tout pour le propriétaire ; pour un invité, ses
 * bibliothèques, sans ses titres masqués. `rel` est le chemin depuis la racine, sans barre.
 */
function allowed(uid, rel) {
  if (!uid) return false;
  // Le propriétaire lit tout, chez lui comme dehors : ce sont ses livres.
  if (uid === state.owner) return true;
  // Pour les autres, un dossier de la racine que le propriétaire n'a pas coché n'est prêté à personne.
  if (rel && !lentNames().has(rel.split('/')[0])) return false;
  const m = state.members?.find((x) => x.user_id === uid);
  if (!m) return false;
  if (!rel) return true;
  const top = rel.split('/')[0];
  if (m.libraries && !m.libraries.includes(top)) return false;
  return !(m.hidden ?? []).some((h) => {
    const p = h.replace(/^\/+|\/+$/g, '');
    return rel === p || rel.startsWith(p + '/');
  });
}

/* ——————————————————— Le petit WebDAV ——————————————————— */

/** Le chemin sur le disque, jamais en dehors de la bibliothèque. */
function resolve(rel) {
  const full = path.resolve(LIBRARY, rel);
  if (full !== LIBRARY && !full.startsWith(LIBRARY + path.sep)) return null;
  return full;
}

const xml = (s) => s.replace(/&/g, '&amp;').replace(/</g, '&lt;').replace(/>/g, '&gt;');

function propResponse(href, stat) {
  const dir = stat.isDirectory();
  return `<d:response><d:href>${xml(href)}</d:href><d:propstat><d:prop>${
    dir ? '<d:resourcetype><d:collection/></d:resourcetype>' : `<d:resourcetype/><d:getcontentlength>${stat.size}</d:getcontentlength>`
  }<d:getlastmodified>${stat.mtime.toUTCString()}</d:getlastmodified></d:prop><d:status>HTTP/1.1 200 OK</d:status></d:propstat></d:response>`;
}

/**
 * Une requête, d'où qu'elle vienne (réseau local ou relais). Rend `{ status, headers, body }`,
 * où `body` est un texte, un Buffer ou un flux.
 */
async function handle({ method, url, headers, local }) {
  const { pathname } = new URL(url, 'http://server');
  if (pathname === '/' && method === 'GET') {
    // La page d'état, seulement sur le réseau local : elle montre le code d'association.
    if (!local) return { status: 404, headers: {}, body: '' };
    return { status: 200, headers: { 'content-type': 'text/html; charset=utf-8' }, body: statusPage() };
  }
  if (pathname === '/health') {
    return { status: 200, headers: { 'content-type': 'application/json' }, body: JSON.stringify({ ok: true, version: VERSION, paired: !!state.owner, id: local ? identity?.id ?? null : undefined }) };
  }
  if (!pathname.startsWith('/dav')) return { status: 404, headers: {}, body: '' };

  const uid = await userOf(headers.authorization);
  if (!uid) return { status: 401, headers: { 'www-authenticate': 'Bearer' }, body: '' };
  const rel = decodeURIComponent(pathname.slice(4)).replace(/^\/+|\/+$/g, '');
  if (rel.split('/').some((part) => SKIP(part))) return { status: 404, headers: {}, body: '' };
  if (!allowed(uid, rel)) return { status: uid === state.owner || state.members?.some((m) => m.user_id === uid) ? 404 : 403, headers: {}, body: '' };
  const full = resolve(rel);
  if (!full) return { status: 404, headers: {}, body: '' };
  const stat = await fsp.stat(full).catch(() => null);
  if (!stat) return { status: 404, headers: {}, body: '' };

  if (method === 'PROPFIND') {
    const parts = [propResponse('./', stat)];
    if (stat.isDirectory() && headers.depth !== '0') {
      const entries = await fsp.readdir(full, { withFileTypes: true });
      for (const e of entries) {
        if (SKIP(e.name)) continue;
        const childRel = rel ? `${rel}/${e.name}` : e.name;
        if (!allowed(uid, childRel)) continue;
        const s = await fsp.stat(path.join(full, e.name)).catch(() => null);
        if (!s || (!s.isDirectory() && !s.isFile())) continue;
        parts.push(propResponse(encodeURIComponent(e.name) + (s.isDirectory() ? '/' : ''), s));
      }
    }
    return {
      status: 207,
      headers: { 'content-type': 'application/xml; charset=utf-8' },
      body: `<?xml version="1.0" encoding="utf-8"?><d:multistatus xmlns:d="DAV:">${parts.join('')}</d:multistatus>`,
    };
  }

  if (method === 'GET' || method === 'HEAD') {
    if (!stat.isFile()) return { status: 405, headers: {}, body: '' };
    const base = { 'accept-ranges': 'bytes', 'last-modified': stat.mtime.toUTCString(), 'content-type': 'application/octet-stream' };
    const range = headers.range?.match(/^bytes=(\d*)-(\d*)$/);
    if (range && method === 'GET') {
      let start = range[1] ? Number(range[1]) : Math.max(0, stat.size - Number(range[2]));
      let end = range[1] && range[2] ? Math.min(Number(range[2]), stat.size - 1) : stat.size - 1;
      if (start > end || start >= stat.size) {
        return { status: 416, headers: { 'content-range': `bytes */${stat.size}` }, body: '' };
      }
      return {
        status: 206,
        headers: { ...base, 'content-range': `bytes ${start}-${end}/${stat.size}`, 'content-length': String(end - start + 1) },
        body: fs.createReadStream(full, { start, end, highWaterMark: 256 * 1024 }),
      };
    }
    return {
      status: 200,
      headers: { ...base, 'content-length': String(stat.size) },
      body: method === 'HEAD' ? '' : fs.createReadStream(full, { highWaterMark: 256 * 1024 }),
    };
  }

  // La bibliothèque est en lecture seule.
  return { status: 405, headers: { allow: 'PROPFIND, HEAD, GET' }, body: '' };
}

function statusPage() {
  const code = state.pair_code;
  const body = state.owner
    ? `<h1>Associé</h1><p>Ce serveur est associé à un compte Foldbook. Les personnes invitées lisent ses bibliothèques depuis l’app.</p>`
    : code
      ? `<p class="k">Code d’association</p><p class="code">${code.slice(0, 3)} ${code.slice(3)}</p><p>Dans Foldbook : Réglages › Bibliothèque partagée › Associer un serveur, puis saisis ce code. Il change toutes les 30 minutes.</p>`
      : `<p>Connexion à Foldbook en cours…</p>`;
  return `<!doctype html><html lang="fr"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<meta http-equiv="refresh" content="${state.owner ? 60 : 10}"><title>Foldbook Server</title>
<style>body{margin:0;min-height:100vh;display:grid;place-items:center;background:#05070f;color:#fff8ec;font:17px/1.5 system-ui,sans-serif;text-align:center;padding:24px}
.b{font:800 28px Impact,sans-serif;letter-spacing:.04em}.b span{color:#e8202a}.k{color:#a7a3b3;text-transform:uppercase;letter-spacing:.2em;font-size:12px;margin-top:32px}
.code{font:700 64px ui-monospace,monospace;letter-spacing:.08em;margin:6px 0 18px}main{max-width:460px}small{color:#a7a3b3;display:block;margin-top:28px}</style></head>
<body><main><div class="b">FOLD<span>BOOK</span> SERVER</div>${body}
<small>${xml(state.name ?? NAME)} · ${(state.members?.length ?? 0)} invité(s) · relais ${relayUp ? 'connecté' : 'hors ligne'} · v${VERSION}</small></main></body></html>`;
}

/* ——————————————————— Le réseau local ——————————————————— */

function send(res, out) {
  res.writeHead(out.status, out.headers);
  if (out.body instanceof Readable) out.body.pipe(res);
  else res.end(out.body);
}

http
  .createServer(async (req, res) => {
    const chunks = [];
    for await (const c of req) chunks.push(c);
    try {
      send(res, await handle({ method: req.method, url: req.url, headers: req.headers, local: true }));
    } catch (error) {
      log('Erreur :', String(error));
      res.writeHead(500).end();
    }
  })
  .listen(PORT, () => log(`Foldbook Server ${VERSION} sur le port ${PORT}, bibliothèque ${LIBRARY}`));

/* ——————————————————— Le relais ——————————————————— */

let relay = null;
let relayUp = false;
let retry = 1000;

function connectRelay() {
  const ws = new WebSocket(RELAY, {
    headers: { 'x-foldbook-server': identity.id, 'x-foldbook-secret': identity.secret },
  });
  relay = ws;
  const streams = new Map();
  ws.on('open', () => {
    relayUp = true;
    retry = 1000;
    log('Relais connecté');
  });
  ws.on('message', async (data) => {
    let m;
    try {
      m = JSON.parse(data.toString());
    } catch {
      return;
    }
    if (m.t === 'cancel') streams.get(m.id)?.destroy();
    if (m.t === 'pause') streams.get(m.id)?.pause();
    if (m.t === 'resume') streams.get(m.id)?.resume();
    if (m.t !== 'req') return;
    let out;
    try {
      out = await handle({ method: m.method, url: m.path, headers: m.headers ?? {}, local: false });
    } catch (error) {
      log('Erreur :', String(error));
      out = { status: 500, headers: {}, body: '' };
    }
    ws.send(JSON.stringify({ t: 'head', id: m.id, status: out.status, headers: out.headers }));
    const prefix = Buffer.alloc(4);
    prefix.writeUInt32BE(m.id);
    if (out.body instanceof Readable) {
      streams.set(m.id, out.body);
      out.body.on('data', (chunk) => ws.readyState === ws.OPEN && ws.send(Buffer.concat([prefix, chunk])));
      out.body.on('close', () => {
        streams.delete(m.id);
        if (ws.readyState === ws.OPEN) ws.send(JSON.stringify({ t: 'end', id: m.id }));
      });
      out.body.on('error', () => out.body.destroy());
    } else {
      if (out.body) ws.send(Buffer.concat([prefix, Buffer.from(out.body)]));
      ws.send(JSON.stringify({ t: 'end', id: m.id }));
    }
  });
  ws.on('close', () => {
    relayUp = false;
    for (const s of streams.values()) s.destroy();
    setTimeout(connectRelay, retry);
    retry = Math.min(retry * 2, 60_000);
  });
  ws.on('error', (error) => log('Relais :', String(error.message ?? error)));
}

await loadIdentity();
await heartbeat();
setInterval(heartbeat, 30_000);
connectRelay();
