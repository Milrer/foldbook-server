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

const VERSION = '1.0.4';
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

// Le logo Foldbook Server, en contours (assets/brand/foldbook-server.svg dans le dépôt de l'app).
const LOGO = `<svg role="img" aria-label="Foldbook Server" class="b" xmlns="http://www.w3.org/2000/svg" viewBox="-10 -797 7495 824"><path d="M478.0 -618.0Q413.0 -618.0 390.0 -596.5Q367.0 -575.0 367.0 -530.0V0.0H138.0V-520.0Q138.0 -607.0 174.0 -666.5Q210.0 -726.0 280.0 -756.5Q350.0 -787.0 451.0 -787.0Q494.0 -787.0 529.5 -781.5Q565.0 -776.0 606.0 -765.0V-599.0Q575.0 -608.0 544.0 -613.0Q513.0 -618.0 478.0 -618.0ZM23.0 -355.0V-523.0H591.0V-355.0Z" fill="#FFF8EC"/><path d="M966.0 17.0Q855.0 17.0 771.0 -21.0Q687.0 -59.0 640.0 -127.5Q593.0 -196.0 593.0 -286.0Q593.0 -378.0 640.0 -446.5Q687.0 -515.0 771.0 -552.5Q855.0 -590.0 967.0 -590.0Q1078.0 -590.0 1162.0 -552.5Q1246.0 -515.0 1292.5 -446.5Q1339.0 -378.0 1339.0 -286.0Q1339.0 -196.0 1292.5 -127.5Q1246.0 -59.0 1162.0 -21.0Q1078.0 17.0 966.0 17.0ZM967.0 -151.0Q1012.0 -151.0 1043.5 -167.0Q1075.0 -183.0 1091.5 -213.5Q1108.0 -244.0 1108.0 -286.0Q1108.0 -329.0 1091.5 -359.5Q1075.0 -390.0 1043.5 -406.0Q1012.0 -422.0 967.0 -422.0Q922.0 -422.0 890.0 -406.0Q858.0 -390.0 841.5 -359.5Q825.0 -329.0 825.0 -287.0Q825.0 -244.0 841.5 -213.5Q858.0 -183.0 890.0 -167.0Q922.0 -151.0 967.0 -151.0Z" fill="#FFF8EC"/><path d="M1379.0 -770.0H1608.0V0.0H1379.0Z" fill="#FFF8EC"/><path d="M2189.0 0.0 2161.0 -214.0 2198.0 -284.0 2161.0 -377.0 2211.0 -770.0H2449.0L2388.0 -305.0L2427.0 0.0ZM2235.0 -286.0Q2223.0 -195.0 2182.5 -126.5Q2142.0 -58.0 2078.5 -20.5Q2015.0 17.0 1933.0 17.0Q1850.0 17.0 1786.0 -21.0Q1722.0 -59.0 1686.5 -127.5Q1651.0 -196.0 1651.0 -286.0Q1651.0 -378.0 1686.5 -446.5Q1722.0 -515.0 1786.0 -552.5Q1850.0 -590.0 1933.0 -590.0Q2015.0 -590.0 2078.5 -553.0Q2142.0 -516.0 2183.0 -447.5Q2224.0 -379.0 2235.0 -286.0ZM1882.0 -286.0Q1882.0 -248.0 1897.5 -219.0Q1913.0 -190.0 1941.0 -173.0Q1969.0 -156.0 2005.0 -156.0Q2042.0 -156.0 2076.0 -173.0Q2110.0 -190.0 2136.0 -219.0Q2162.0 -248.0 2174.0 -286.0Q2162.0 -324.0 2136.0 -353.5Q2110.0 -383.0 2076.0 -400.0Q2042.0 -417.0 2005.0 -417.0Q1969.0 -417.0 1941.0 -400.0Q1913.0 -383.0 1897.5 -353.5Q1882.0 -324.0 1882.0 -286.0Z" fill="#FFF8EC"/><path d="M2717.0 0.0 2745.0 -214.0 2708.0 -284.0 2745.0 -377.0 2695.0 -770.0H2457.0L2518.0 -305.0L2479.0 0.0ZM2671.0 -286.0Q2683.0 -195.0 2723.5 -126.5Q2764.0 -58.0 2827.5 -20.5Q2891.0 17.0 2973.0 17.0Q3056.0 17.0 3120.0 -21.0Q3184.0 -59.0 3219.5 -127.5Q3255.0 -196.0 3255.0 -286.0Q3255.0 -378.0 3219.5 -446.5Q3184.0 -515.0 3120.0 -552.5Q3056.0 -590.0 2973.0 -590.0Q2891.0 -590.0 2827.5 -553.0Q2764.0 -516.0 2723.0 -447.5Q2682.0 -379.0 2671.0 -286.0ZM3024.0 -286.0Q3024.0 -248.0 3008.5 -219.0Q2993.0 -190.0 2965.0 -173.0Q2937.0 -156.0 2901.0 -156.0Q2864.0 -156.0 2830.0 -173.0Q2796.0 -190.0 2770.0 -219.0Q2744.0 -248.0 2732.0 -286.0Q2744.0 -324.0 2770.0 -353.5Q2796.0 -383.0 2830.0 -400.0Q2864.0 -417.0 2901.0 -417.0Q2937.0 -417.0 2965.0 -400.0Q2993.0 -383.0 3008.5 -353.5Q3024.0 -324.0 3024.0 -286.0Z" fill="#E8202A"/><path d="M3645.0 17.0Q3534.0 17.0 3450.0 -21.0Q3366.0 -59.0 3319.0 -127.5Q3272.0 -196.0 3272.0 -286.0Q3272.0 -378.0 3319.0 -446.5Q3366.0 -515.0 3450.0 -552.5Q3534.0 -590.0 3646.0 -590.0Q3757.0 -590.0 3841.0 -552.5Q3925.0 -515.0 3971.5 -446.5Q4018.0 -378.0 4018.0 -286.0Q4018.0 -196.0 3971.5 -127.5Q3925.0 -59.0 3841.0 -21.0Q3757.0 17.0 3645.0 17.0ZM3646.0 -151.0Q3691.0 -151.0 3722.5 -167.0Q3754.0 -183.0 3770.5 -213.5Q3787.0 -244.0 3787.0 -286.0Q3787.0 -329.0 3770.5 -359.5Q3754.0 -390.0 3722.5 -406.0Q3691.0 -422.0 3646.0 -422.0Q3601.0 -422.0 3569.0 -406.0Q3537.0 -390.0 3520.5 -359.5Q3504.0 -329.0 3504.0 -287.0Q3504.0 -244.0 3520.5 -213.5Q3537.0 -183.0 3569.0 -167.0Q3601.0 -151.0 3646.0 -151.0Z" fill="#E8202A"/><path d="M4406.0 17.0Q4295.0 17.0 4211.0 -21.0Q4127.0 -59.0 4080.0 -127.5Q4033.0 -196.0 4033.0 -286.0Q4033.0 -378.0 4080.0 -446.5Q4127.0 -515.0 4211.0 -552.5Q4295.0 -590.0 4407.0 -590.0Q4518.0 -590.0 4602.0 -552.5Q4686.0 -515.0 4732.5 -446.5Q4779.0 -378.0 4779.0 -286.0Q4779.0 -196.0 4732.5 -127.5Q4686.0 -59.0 4602.0 -21.0Q4518.0 17.0 4406.0 17.0ZM4407.0 -151.0Q4452.0 -151.0 4483.5 -167.0Q4515.0 -183.0 4531.5 -213.5Q4548.0 -244.0 4548.0 -286.0Q4548.0 -329.0 4531.5 -359.5Q4515.0 -390.0 4483.5 -406.0Q4452.0 -422.0 4407.0 -422.0Q4362.0 -422.0 4330.0 -406.0Q4298.0 -390.0 4281.5 -359.5Q4265.0 -329.0 4265.0 -287.0Q4265.0 -244.0 4281.5 -213.5Q4298.0 -183.0 4330.0 -167.0Q4362.0 -151.0 4407.0 -151.0Z" fill="#E8202A"/><path d="M5048.0 -180.0 4986.0 -201.0 5288.0 -573.0H5524.0L5023.0 0.0H4819.0V-770.0H5048.0ZM5125.0 -278.0 5300.0 -395.0 5539.0 0.0H5283.0Z" fill="#E8202A"/><path d="M6098.12 -75.17999999999999Q6098.12 -49.559999999999995 6083.21 -31.08Q6068.3 -12.6 6038.9 -3.15Q6009.5 6.3 5967.08 6.3Q5923.4 6.3 5891.0599999999995 -4.41Q5858.72 -15.12 5840.66 -34.86Q5822.6 -54.6 5821.34 -80.64H5892.74Q5895.26 -68.88 5905.34 -60.269999999999996Q5915.42 -51.66 5931.59 -47.04Q5947.76 -42.42 5969.18 -42.42Q5999.0 -42.42 6014.33 -49.14Q6029.66 -55.86 6029.66 -69.3Q6029.66 -78.96 6018.95 -84.0Q6008.24 -89.03999999999999 5979.68 -91.56L5935.16 -94.92Q5895.26 -98.28 5871.74 -108.36Q5848.22 -118.44 5838.35 -133.76999999999998Q5828.48 -149.1 5828.48 -167.57999999999998Q5828.48 -193.2 5844.44 -210.42Q5860.4 -227.64 5889.17 -236.45999999999998Q5917.94 -245.28 5955.74 -245.28Q5994.38 -245.28 6024.62 -234.78Q6054.86 -224.28 6073.129999999999 -205.8Q6091.4 -187.32 6093.92 -162.96H6022.52Q6020.84 -171.78 6013.07 -179.55Q6005.3 -187.32 5990.6 -192.36Q5975.9 -197.4 5952.38 -197.4Q5925.92 -197.4 5911.639999999999 -191.31Q5897.36 -185.22 5897.36 -173.04Q5897.36 -164.64 5906.18 -159.39Q5915.0 -154.14 5938.94 -152.04L5996.9 -147.42Q6035.54 -144.06 6057.8 -134.61Q6080.06 -125.16 6089.09 -110.03999999999999Q6098.12 -94.92 6098.12 -75.17999999999999Z" fill="#A7A3B3"/><path d="M6273.259999999999 6.3Q6230.839999999999 6.3 6197.449999999999 -9.45Q6164.0599999999995 -25.2 6144.949999999999 -53.76Q6125.839999999999 -82.32 6125.839999999999 -120.11999999999999Q6125.839999999999 -157.5 6144.109999999999 -185.64Q6162.379999999999 -213.78 6194.299999999999 -229.53Q6226.219999999999 -245.28 6266.959999999999 -245.28Q6308.54 -245.28 6338.36 -227.01Q6368.179999999999 -208.73999999999998 6384.349999999999 -175.56Q6400.5199999999995 -142.38 6400.5199999999995 -97.44H6182.12V-144.06H6362.719999999999L6335.419999999999 -127.25999999999999Q6333.74 -148.26 6325.129999999999 -162.95999999999998Q6316.5199999999995 -177.66 6301.82 -185.64Q6287.12 -193.62 6267.379999999999 -193.62Q6245.959999999999 -193.62 6230.209999999999 -185.01Q6214.459999999999 -176.4 6205.849999999999 -161.07Q6197.24 -145.73999999999998 6197.24 -125.16Q6197.24 -100.38 6208.16 -82.74Q6219.08 -65.1 6240.08 -55.86Q6261.08 -46.62 6290.9 -46.62Q6317.78 -46.62 6345.08 -54.39Q6372.379999999999 -62.16 6394.219999999999 -76.44V-29.4Q6370.7 -12.6 6339.41 -3.15Q6308.12 6.3 6273.259999999999 6.3Z" fill="#A7A3B3"/><path d="M6430.339999999999 -238.98H6501.32L6515.599999999999 -152.88V0.0H6445.459999999999V-160.44ZM6631.94 -241.5V-181.01999999999998Q6621.0199999999995 -183.54 6610.099999999999 -184.8Q6599.179999999999 -186.06 6589.5199999999995 -186.06Q6568.5199999999995 -186.06 6551.719999999999 -177.24Q6534.919999999999 -168.42 6525.259999999999 -149.94Q6515.599999999999 -131.46 6515.599999999999 -103.32L6500.9 -120.96Q6503.419999999999 -146.57999999999998 6510.139999999999 -169.04999999999998Q6516.86 -191.51999999999998 6528.83 -208.52999999999997Q6540.799999999999 -225.54 6558.439999999999 -235.41Q6576.079999999999 -245.28 6600.44 -245.28Q6608.419999999999 -245.28 6616.609999999999 -244.23Q6624.799999999999 -243.17999999999998 6631.94 -241.5Z" fill="#A7A3B3"/><path d="M6807.5 -32.339999999999996 6782.299999999999 -32.76 6872.599999999999 -238.98H6947.36L6833.12 0.0H6753.74L6639.92 -238.98H6717.2Z" fill="#A7A3B3"/><path d="M7110.74 6.3Q7068.32 6.3 7034.93 -9.45Q7001.54 -25.2 6982.43 -53.76Q6963.32 -82.32 6963.32 -120.11999999999999Q6963.32 -157.5 6981.59 -185.64Q6999.86 -213.78 7031.78 -229.53Q7063.7 -245.28 7104.44 -245.28Q7146.02 -245.28 7175.84 -227.01Q7205.66 -208.73999999999998 7221.83 -175.56Q7238.0 -142.38 7238.0 -97.44H7019.6V-144.06H7200.2L7172.9 -127.25999999999999Q7171.22 -148.26 7162.610000000001 -162.95999999999998Q7154.0 -177.66 7139.3 -185.64Q7124.6 -193.62 7104.86 -193.62Q7083.44 -193.62 7067.69 -185.01Q7051.94 -176.4 7043.33 -161.07Q7034.72 -145.73999999999998 7034.72 -125.16Q7034.72 -100.38 7045.64 -82.74Q7056.56 -65.1 7077.56 -55.86Q7098.56 -46.62 7128.38 -46.62Q7155.26 -46.62 7182.5599999999995 -54.39Q7209.86 -62.16 7231.7 -76.44V-29.4Q7208.18 -12.6 7176.89 -3.15Q7145.6 6.3 7110.74 6.3Z" fill="#A7A3B3"/><path d="M7267.82 -238.98H7338.8L7353.08 -152.88V0.0H7282.94V-160.44ZM7469.42 -241.5V-181.01999999999998Q7458.5 -183.54 7447.58 -184.8Q7436.66 -186.06 7427.0 -186.06Q7406.0 -186.06 7389.2 -177.24Q7372.4 -168.42 7362.74 -149.94Q7353.08 -131.46 7353.08 -103.32L7338.38 -120.96Q7340.9 -146.57999999999998 7347.62 -169.04999999999998Q7354.34 -191.51999999999998 7366.3099999999995 -208.52999999999997Q7378.28 -225.54 7395.92 -235.41Q7413.5599999999995 -245.28 7437.92 -245.28Q7445.9 -245.28 7454.09 -244.23Q7462.28 -243.17999999999998 7469.42 -241.5Z" fill="#A7A3B3"/></svg>`;

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
.b{height:30px;width:auto;display:block;margin:0 auto}.k{color:#a7a3b3;text-transform:uppercase;letter-spacing:.2em;font-size:12px;margin-top:32px}
.code{font:700 64px ui-monospace,monospace;letter-spacing:.08em;margin:6px 0 18px}main{max-width:460px}small{color:#a7a3b3;display:block;margin-top:28px}</style></head>
<body><main>${LOGO}${body}
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
