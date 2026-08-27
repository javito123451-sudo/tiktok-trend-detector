/**
 * TikTok Trend Detector — conector para detección de tendencias (sonidos + hashtags)
 * de cara a planificar Reels/TikToks. Pensado para correr como Render Cron Job diario
 * y avisar por Telegram cuando algo hace breakout.
 *
 * Usa TikLiveAPI (https://api.tikliveapi.com) porque TikTok NO ofrece API pública
 * gratuita de "trending" — solo endpoints de lookup por ID/nombre/keyword.
 * Por eso el detector combina DOS mecanismos:
 *
 *   1. DESCUBRIMIENTO: /search-challenge/?keyword=X devuelve hashtags relacionados
 *      con reach real (view_count, user_count).
 *   2. VELOCIDAD: guarda snapshots en Postgres (Neon) y compara contra la ejecución
 *      anterior para calcular crecimiento (%/hora). Un salto grande = trend emergente.
 *
 * IMPORTANTE: el estado se guarda en Postgres, no en disco — los Render Cron Jobs
 * son efímeros y no soportan discos persistentes.
 *
 * Variables de entorno requeridas:
 *   TIKLIVEAPI_KEY     — API key de tikliveapi.com (100 créditos gratis al registrarte)
 *   DATABASE_URL       — connection string de tu Neon Postgres
 *   TELEGRAM_BOT_TOKEN — token del bot (se consigue hablando con @BotFather en Telegram)
 *   TELEGRAM_CHAT_ID   — tu chat id (envíale un mensaje al bot y mira
 *                        https://api.telegram.org/bot<TOKEN>/getUpdates)
 *
 * Uso local:
 *   npm i pg
 *   npx tsx tiktok-trend-detector.ts
 */

import { Client } from "pg";

const API_KEY = process.env.TIKLIVEAPI_KEY ?? "";
const DATABASE_URL = process.env.DATABASE_URL ?? "";
const TELEGRAM_BOT_TOKEN = process.env.TELEGRAM_BOT_TOKEN ?? "";
const TELEGRAM_CHAT_ID = process.env.TELEGRAM_CHAT_ID ?? "";
const BASE_URL = "https://api.tikliveapi.com";
const BREAKOUT_THRESHOLD_PCT_PER_HOUR = 0.5; // ajustable

// ── Watchlist: palabras clave por serie de contenido ────────────────────────
const HASHTAG_WATCHLIST: Record<string, string[]> = {
  "El Gigante": ["comedia absurda", "pov gigante", "street comedy"],
  "LA FRACTURA": ["thriller pov", "plot twist", "suspense corto"],
  "Repartidor": ["delivery guy", "superhuman feats", "pov reparto"],
  "Animales IA": ["cat noir", "capybara anxious", "ai animal"],
};

const MUSIC_WATCHLIST: { id: string; label: string }[] = [
  // { id: "6747377620087819014", label: "ejemplo" },
];

// ── Tipos ────────────────────────────────────────────────────────────────────
interface Snapshot {
  timestamp: number;
  view_count?: number;
  user_count?: number;
  video_count?: number;
}
type State = Record<string, Snapshot>;

interface TrendResult {
  key: string;
  label: string;
  series?: string;
  metric: number;
  metricName: string;
  growthPctPerHour: number | null;
  breakout: boolean;
}

// ── Helpers HTTP ─────────────────────────────────────────────────────────────
async function tikliveGet<T>(path: string, params: Record<string, string>): Promise<T> {
  if (!API_KEY) throw new Error("Falta TIKLIVEAPI_KEY en el entorno");
  const url = new URL(BASE_URL + path);
  Object.entries(params).forEach(([k, v]) => url.searchParams.set(k, v));
  const res = await fetch(url.toString(), { headers: { "X-Api-Key": API_KEY } });
  if (!res.ok) throw new Error(`${path} -> HTTP ${res.status}: ${await res.text()}`);
  return res.json() as Promise<T>;
}

// ── Estado en Postgres (Neon) ─────────────────────────────────────────────────
async function getDbClient(): Promise<Client> {
  if (!DATABASE_URL) throw new Error("Falta DATABASE_URL en el entorno");
  const client = new Client({ connectionString: DATABASE_URL, ssl: { rejectUnauthorized: false } });
  await client.connect();
  await client.query(`
    CREATE TABLE IF NOT EXISTS trend_snapshots (
      key TEXT PRIMARY KEY,
      ts BIGINT NOT NULL,
      view_count BIGINT,
      user_count BIGINT,
      video_count BIGINT
    )
  `);
  return client;
}

async function loadState(client: Client): Promise<State> {
  const { rows } = await client.query(`SELECT key, ts, view_count, user_count, video_count FROM trend_snapshots`);
  const state: State = {};
  for (const row of rows) {
    state[row.key] = {
      timestamp: Number(row.ts),
      view_count: row.view_count != null ? Number(row.view_count) : undefined,
      user_count: row.user_count != null ? Number(row.user_count) : undefined,
      video_count: row.video_count != null ? Number(row.video_count) : undefined,
    };
  }
  return state;
}

async function upsertSnapshot(client: Client, key: string, snap: Snapshot) {
  await client.query(
    `INSERT INTO trend_snapshots (key, ts, view_count, user_count, video_count)
     VALUES ($1, $2, $3, $4, $5)
     ON CONFLICT (key) DO UPDATE SET ts = $2, view_count = $3, user_count = $4, video_count = $5`,
    [key, snap.timestamp, snap.view_count ?? null, snap.user_count ?? null, snap.video_count ?? null]
  );
}

function computeGrowth(prev: Snapshot | undefined, current: Snapshot, metric: keyof Snapshot): number | null {
  if (!prev || prev[metric] == null || current[metric] == null) return null;
  const hours = (current.timestamp - prev.timestamp) / 3_600_000;
  if (hours < 0.5) return null;
  const delta = (current[metric] as number) - (prev[metric] as number);
  const pct = (delta / Math.max(prev[metric] as number, 1)) * 100;
  return pct / hours;
}

// ── Descubrimiento de hashtags ───────────────────────────────────────────────
async function discoverHashtags(
  keyword: string,
  series: string,
  state: State,
  results: TrendResult[],
  client: Client
) {
  try {
    const data = await tikliveGet<{ challenge_list: { cha_name: string; view_count: number; user_count: number }[] }>(
      "/search-challenge/",
      { keyword, count: "10" }
    );
    for (const tag of data.challenge_list ?? []) {
      const key = `hashtag:${tag.cha_name}`;
      const current: Snapshot = { timestamp: Date.now(), view_count: tag.view_count, user_count: tag.user_count };
      const growth = computeGrowth(state[key], current, "view_count");
      results.push({
        key,
        label: `#${tag.cha_name}`,
        series,
        metric: tag.view_count,
        metricName: "views",
        growthPctPerHour: growth,
        breakout: growth != null && growth > BREAKOUT_THRESHOLD_PCT_PER_HOUR,
      });
      await upsertSnapshot(client, key, current);
    }
  } catch (err) {
    console.error(`⚠️  Error buscando "${keyword}":`, (err as Error).message);
  }
}

// ── Seguimiento de sonidos concretos ─────────────────────────────────────────
async function trackMusic(id: string, label: string, state: State, results: TrendResult[], client: Client) {
  try {
    const data = await tikliveGet<{ video_count: number; title: string; author: string }>("/music-info/", {
      music_id: id,
    });
    const key = `music:${id}`;
    const current: Snapshot = { timestamp: Date.now(), video_count: data.video_count };
    const growth = computeGrowth(state[key], current, "video_count");
    results.push({
      key,
      label: `🎵 ${data.title} — ${data.author} (${label})`,
      metric: data.video_count,
      metricName: "videos usando el sonido",
      growthPctPerHour: growth,
      breakout: growth != null && growth > BREAKOUT_THRESHOLD_PCT_PER_HOUR,
    });
    await upsertSnapshot(client, key, current);
  } catch (err) {
    console.error(`⚠️  Error consultando sonido "${label}":`, (err as Error).message);
  }
}

// ── Aviso por Telegram ────────────────────────────────────────────────────────
async function sendTelegramDigest(results: TrendResult[]) {
  if (!TELEGRAM_BOT_TOKEN || !TELEGRAM_CHAT_ID) {
    console.log("ℹ️  TELEGRAM_BOT_TOKEN / TELEGRAM_CHAT_ID no configurados — se omite el envío.");
    return;
  }
  const breakouts = results.filter((r) => r.breakout).slice(0, 10);
  const top = (breakouts.length > 0 ? breakouts : results.slice(0, 10));

  const lines = top.map((r) => {
    const growth = r.growthPctPerHour != null ? `${r.growthPctPerHour.toFixed(2)}%/h` : "sin histórico";
    const flag = r.breakout ? "🔥 " : "• ";
    return `${flag}${r.label} — ${r.metric.toLocaleString("es-ES")} ${r.metricName} (${growth})${r.series ? ` [${r.series}]` : ""}`;
  });

  const header = breakouts.length > 0
    ? `🔥 *${breakouts.length} trend(s) en breakout hoy*\n\n`
    : `📊 *Trend digest de hoy* (sin breakouts, top por volumen)\n\n`;

  const text = header + lines.join("\n");

  const res = await fetch(`https://api.telegram.org/bot${TELEGRAM_BOT_TOKEN}/sendMessage`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ chat_id: TELEGRAM_CHAT_ID, text, parse_mode: "Markdown" }),
  });
  if (!res.ok) {
    console.error("⚠️  Error enviando a Telegram:", await res.text());
  } else {
    console.log("✅ Digest enviado por Telegram.");
  }
}

// ── Main ──────────────────────────────────────────────────────────────────────
async function main() {
  const client = await getDbClient();
  const state = await loadState(client);
  const results: TrendResult[] = [];

  try {
    for (const [series, keywords] of Object.entries(HASHTAG_WATCHLIST)) {
      for (const kw of keywords) {
        await discoverHashtags(kw, series, state, results, client);
      }
    }
    for (const m of MUSIC_WATCHLIST) {
      await trackMusic(m.id, m.label, state, results, client);
    }
  } finally {
    await client.end();
  }

  results.sort((a, b) => {
    if (a.breakout !== b.breakout) return a.breakout ? -1 : 1;
    const ga = a.growthPctPerHour ?? -Infinity;
    const gb = b.growthPctPerHour ?? -Infinity;
    if (ga !== gb) return gb - ga;
    return b.metric - a.metric;
  });

  console.log("\n📊 TREND DIGEST —", new Date().toLocaleString("es-ES"), "\n");
  for (const r of results.slice(0, 25)) {
    const flag = r.breakout ? "🔥 BREAKOUT" : "";
    const growth = r.growthPctPerHour != null ? `${r.growthPctPerHour.toFixed(2)}%/h` : "sin histórico aún";
    console.log(
      `${flag.padEnd(12)}${r.label.padEnd(45)} ${r.metric.toLocaleString("es-ES")} ${r.metricName} — ${growth}${r.series ? `  [${r.series}]` : ""}`
    );
  }

  await sendTelegramDigest(results);
  console.log(`\n✅ Digest completo (${results.length} entradas). Estado guardado en Postgres.`);
}

main().catch((err) => {
  console.error("Error fatal:", err);
  process.exit(1);
});
