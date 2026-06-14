/**
 * LeadForge — WhatsApp Bulk Messaging server
 *
 * One whatsapp-web.js Client per signed-in user (keyed by their Supabase user id).
 * Frontend authenticates every call with the user's Supabase access token.
 *
 * Env vars (set on Railway / Render):
 *   SUPABASE_URL                  https://<ref>.supabase.co
 *   SUPABASE_ANON_KEY             used to validate user JWTs & for per-user DB ops (RLS)
 *   PORT                          provided by host
 *
 * NOTE: We do NOT use SUPABASE_SERVICE_ROLE_KEY. All database writes go through
 * a per-request authed client using the user's own JWT, which means RLS policies
 * (auth.uid() = user_id) apply correctly.
 */

const express = require("express");
const cors = require("cors");
const http = require("http");
const qrcode = require("qrcode");
const { Server } = require("socket.io");
const { Client, LocalAuth } = require("whatsapp-web.js");
const { createClient } = require("@supabase/supabase-js");
const WebSocket = require("ws");

function env(name, ...aliases) {
  for (const key of [name, ...aliases]) {
    const value = process.env[key];
    if (typeof value === "string" && value.trim()) return value.trim();
  }
  return undefined;
}

const SUPABASE_URL = env("SUPABASE_URL");
const SUPABASE_ANON_KEY = env(
  "SUPABASE_ANON_KEY",
  "SUPABASE_PUBLISHABLE_KEY",
  "VITE_SUPABASE_ANON_KEY",
  "VITE_SUPABASE_PUBLISHABLE_KEY"
);
const PORT = env("PORT") || 3000;
const SERVER_VERSION = "2026-06-13-safer-send-v5";
const MIN_DELAY_BETWEEN_MESSAGES_MS = 30_000;
const SEND_RETRY_LIMIT = 2;

const missing = [
  ["SUPABASE_URL", SUPABASE_URL],
  ["SUPABASE_ANON_KEY", SUPABASE_ANON_KEY],
].filter(([, v]) => !v).map(([k]) => k);

const bootId = `${Date.now()}-${Math.random().toString(16).slice(2, 8)}`;
const visibleSupabaseKeys = Object.keys(process.env)
  .filter((key) => key.includes("SUPABASE") || key.includes("PUBLIC") || key.includes("VITE"))
  .sort();

function safeUrlFingerprint(url) {
  try {
    const u = new URL(url);
    const [projectRef] = u.hostname.split(".");
    return {
      host: u.hostname,
      projectRefStart: projectRef ? projectRef.slice(0, 6) : null,
      projectRefEnd: projectRef ? projectRef.slice(-6) : null,
    };
  } catch {
    return { host: "invalid-url", projectRefStart: null, projectRefEnd: null };
  }
}

console.log(`[boot:${bootId}] LeadForge WhatsApp server starting`);
console.log(
  `[boot:${bootId}] Env presence: ` +
    JSON.stringify({
      SUPABASE_URL: Boolean(SUPABASE_URL),
      SUPABASE_ANON_KEY: Boolean(SUPABASE_ANON_KEY),
    })
);
console.log(
  `[boot:${bootId}] Visible Supabase-related env keys: ` +
    (visibleSupabaseKeys.length ? visibleSupabaseKeys.join(", ") : "none")
);

if (missing.length) {
  console.error(`[boot:${bootId}] CONFIG ERROR missing env vars: ${missing.join(", ")}`);
  console.error(`[boot:${bootId}] Server will stay online in diagnostic mode until variables are fixed.`);
} else {
  console.log(`[boot:${bootId}] Environment check passed`);
}

const app = express();
app.use(cors({ origin: "*" }));
app.use(express.json({ limit: "2mb" }));
const server = http.createServer(app);
const io = new Server(server, { cors: { origin: "*" } });

// ---------- helpers ----------
function makeUserClient(token) {
  return createClient(SUPABASE_URL, SUPABASE_ANON_KEY, {
    global: { headers: { Authorization: `Bearer ${token}` } },
    auth: { persistSession: false },
    realtime: { transport: WebSocket },
  });
}

async function userFromToken(token) {
  if (!token) return null;
  const userClient = makeUserClient(token);
  const { data, error } = await userClient.auth.getUser();
  if (error || !data?.user) return null;
  return { user: data.user, db: userClient };
}

async function authMiddleware(req, res, next) {
  if (missing.length) {
    return res.status(503).json({
      error: "WhatsApp server is missing deployment environment variables",
      missing,
      visibleSupabaseKeys,
    });
  }
  const token = (req.headers.authorization || "").replace(/^Bearer\s+/i, "");
  const result = await userFromToken(token);
  if (!result) return res.status(401).json({ error: "Unauthorized" });
  req.user = result.user;
  req.db = result.db;
  req.token = token;
  next();
}

// ---------- per-user WhatsApp clients ----------
/** Map<userId, { client, ready, phone, qr, controls, token }> */
const sessions = new Map();
const activeWorkers = new Set();

function getSession(userId) {
  let s = sessions.get(userId);
  if (!s) {
    s = { client: null, ready: false, phone: null, qr: null, controls: {}, token: null };
    sessions.set(userId, s);
  }
  return s;
}

function emitToUser(userId, event, payload) {
  io.to(`user:${userId}`).emit(event, payload);
}

function buildClient(userId) {
  const client = new Client({
    authStrategy: new LocalAuth({ clientId: `user-${userId}` }),
    puppeteer: {
      headless: true,
      protocolTimeout: 180_000,
      args: [
        "--no-sandbox",
        "--disable-setuid-sandbox",
        "--disable-dev-shm-usage",
        "--disable-accelerated-2d-canvas",
        "--no-first-run",
        "--no-zygote",
        "--disable-gpu",
      ],
    },
  });

  client.on("qr", async (qr) => {
    const dataUrl = await qrcode.toDataURL(qr);
    const s = getSession(userId);
    s.qr = dataUrl;
    s.ready = false;
    emitToUser(userId, "qr", dataUrl);
    emitToUser(userId, "status", { connected: false, state: "qr" });
  });

  client.on("ready", () => {
    const s = getSession(userId);
    s.ready = true;
    s.qr = null;
    s.phone = client.info?.wid?.user ? "+" + client.info.wid.user : null;
    emitToUser(userId, "ready", { phone: s.phone });
    emitToUser(userId, "status", { connected: true, phone: s.phone });
  });

  client.on("disconnected", () => {
    const s = getSession(userId);
    s.ready = false; s.qr = null; s.phone = null;
    emitToUser(userId, "disconnected", {});
    emitToUser(userId, "status", { connected: false });
  });

  client.on("auth_failure", (m) => console.error(`[${userId}] auth_failure`, m));

  return client;
}

// ---------- socket.io auth ----------
io.use(async (socket, next) => {
  if (missing.length) return next(new Error(`Missing env vars: ${missing.join(", ")}`));
  const token = socket.handshake.auth?.token;
  const result = await userFromToken(token);
  if (!result) return next(new Error("Unauthorized"));
  socket.data.userId = result.user.id;
  socket.join(`user:${result.user.id}`);
  next();
});

io.on("connection", (socket) => {
  const s = getSession(socket.data.userId);
  if (s.qr) socket.emit("qr", s.qr);
  socket.emit("status", { connected: s.ready, phone: s.phone });
});

// ---------- REST endpoints ----------
app.get("/", (_req, res) =>
  res.status(missing.length ? 503 : 200).json({
    ok: !missing.length,
    service: "leadforge-whatsapp",
    version: SERVER_VERSION,
    mode: missing.length ? "diagnostic" : "ready",
    missing,
    supabaseUrl: SUPABASE_URL ? safeUrlFingerprint(SUPABASE_URL) : null,
    visibleSupabaseKeys,
  })
);

app.get("/api/whatsapp/status", authMiddleware, (req, res) => {
  const s = getSession(req.user.id);
  res.json({ connected: s.ready, phone: s.phone, state: s.qr ? "qr" : s.ready ? "ready" : "idle" });
});

app.post("/api/whatsapp/connect", authMiddleware, async (req, res) => {
  const s = getSession(req.user.id);
  s.token = req.token; // store latest token for background campaign worker
  if (s.client && s.ready) return res.json({ status: "already_connected" });
  if (!s.client) {
    s.client = buildClient(req.user.id);
    s.client.initialize().catch((e) => console.error("init", e));
  }
  res.json({ status: "qr_pending" });
});

app.post("/api/whatsapp/disconnect", authMiddleware, async (req, res) => {
  const s = getSession(req.user.id);
  try { if (s.client) await s.client.logout(); } catch {}
  try { if (s.client) await s.client.destroy(); } catch {}
  s.client = null; s.ready = false; s.qr = null; s.phone = null;
  res.json({ ok: true });
});

app.post("/api/whatsapp/control", authMiddleware, async (req, res) => {
  const { campaign_id, action } = req.body || {};
  if (!campaign_id) return res.status(400).json({ error: "campaign_id required" });
  const s = getSession(req.user.id);
  s.controls[campaign_id] = s.controls[campaign_id] || {};
  if (action === "pause") s.controls[campaign_id].paused = true;
  if (action === "resume") s.controls[campaign_id].paused = false;
  if (action === "stop") s.controls[campaign_id].stopped = true;
  res.json({ ok: true });
});

app.post("/api/whatsapp/send-bulk", authMiddleware, async (req, res) => {
  const { campaign_id, retry_failed } = req.body || {};
  if (!campaign_id) return res.status(400).json({ error: "campaign_id required" });

  const s = getSession(req.user.id);
  if (!s.client || !s.ready) return res.status(400).json({ error: "WhatsApp not connected" });

  // keep latest token for the worker
  s.token = req.token;

  // verify ownership (RLS-scoped)
  const { data: campaign, error } = await req.db
    .from("wa_campaigns")
    .select("*")
    .eq("id", campaign_id)
    .single();
  if (error || !campaign) return res.status(404).json({ error: "Campaign not found" });

  if (retry_failed) {
    await req.db
      .from("wa_messages")
      .update({ status: "pending", error_msg: null, sent_at: null })
      .eq("campaign_id", campaign.id)
      .eq("status", "failed");
    campaign.failed_count = 0;
    await req.db.from("wa_campaigns").update({ failed_count: 0, status: "running" }).eq("id", campaign.id);
  }

  const workerKey = `${req.user.id}:${campaign.id}`;
  if (activeWorkers.has(workerKey)) return res.json({ ok: true, status: "already_running" });
  activeWorkers.add(workerKey);

  res.json({ ok: true, status: "started" });

  // fire-and-forget worker
  runCampaign(req.user.id, campaign, req.token)
    .catch((e) => console.error(`[${req.user.id}] campaign ${campaign.id}`, e))
    .finally(() => activeWorkers.delete(workerKey));
});

// ---------- campaign worker ----------
async function runCampaign(userId, campaign, token) {
  const s = getSession(userId);
  s.controls[campaign.id] = s.controls[campaign.id] || { paused: false, stopped: false };
  const db = makeUserClient(token);

  if (!s.client || !s.ready) {
    await db.from("wa_campaigns").update({ status: "paused" }).eq("id", campaign.id);
    console.error(`[${userId}] campaign ${campaign.id} cannot start: WhatsApp not connected`);
    return;
  }

  // daily-limit count (today, this user, status=sent)
  const startOfDay = new Date(); startOfDay.setHours(0, 0, 0, 0);
  const { count: sentToday } = await db
    .from("wa_messages")
    .select("id", { count: "exact", head: true })
    .eq("status", "sent")
    .gte("sent_at", startOfDay.toISOString());
  let dailyCount = sentToday || 0;

  let campaignPaused = false;
  await db.from("wa_campaigns").update({ status: "running" }).eq("id", campaign.id);

  // fetch pending messages for this campaign
  const { data: pending } = await db
    .from("wa_messages")
    .select("*")
    .eq("campaign_id", campaign.id)
    .eq("status", "pending")
    .order("created_at", { ascending: true });

  let consecutiveFailures = 0;
  let sent = campaign.sent_count || 0;
  let failed = campaign.failed_count || 0;

  for (const msg of pending || []) {
    const ctl = s.controls[campaign.id] || {};
    if (ctl.stopped) break;
    while (ctl.paused && !ctl.stopped) {
      await sleep(2000);
    }
    if (ctl.stopped) break;

    if (dailyCount >= campaign.daily_limit) {
      console.log(`[${userId}] daily limit reached`);
      await db.from("wa_campaigns").update({ status: "paused" }).eq("id", campaign.id);
      campaignPaused = true;
      break;
    }

    // duplicate prevention: skip if same phone successfully messaged in last 30 days
    const thirtyDaysAgo = new Date(Date.now() - 30 * 86400_000).toISOString();
    const { data: dup } = await db
      .from("wa_messages")
      .select("id")
      .eq("phone", msg.phone)
      .eq("status", "sent")
      .gte("sent_at", thirtyDaysAgo)
      .neq("campaign_id", campaign.id)
      .limit(1);
    if (dup && dup.length) {
      await db.from("wa_messages").update({
        status: "failed", error_msg: "Duplicate (sent in last 30 days)", sent_at: new Date().toISOString(),
      }).eq("id", msg.id);
      failed++;
      await db.from("wa_campaigns").update({ failed_count: failed }).eq("id", campaign.id);
      continue;
    }

    try {
      const normalizedPhone = normalizePhone(msg.phone);
      const text = msg.rendered_message || campaign.message_template;
      const sentMessage = await sendTextMessage(s.client, normalizedPhone, text, userId);
      if (!sentMessage) throw new Error("WhatsApp could not confirm this message was sent");
      sent++; dailyCount++; consecutiveFailures = 0;
      await db.from("wa_messages").update({
        status: "sent", sent_at: new Date().toISOString(),
      }).eq("id", msg.id);
      await db.from("wa_campaigns").update({ sent_count: sent }).eq("id", campaign.id);
    } catch (err) {
      failed++; consecutiveFailures++;
      await db.from("wa_messages").update({
        status: "failed", error_msg: String(err.message || err).slice(0, 250), sent_at: new Date().toISOString(),
      }).eq("id", msg.id);
      await db.from("wa_campaigns").update({ failed_count: failed }).eq("id", campaign.id);
      if (consecutiveFailures >= 3) {
        console.log(`[${userId}] 3 consecutive failures — pausing`);
        await db.from("wa_campaigns").update({
          status: "paused",
        }).eq("id", campaign.id);
        campaignPaused = true;
        break;
      }
    }

    const base = Math.max(MIN_DELAY_BETWEEN_MESSAGES_MS, (Number(campaign.delay_seconds) || 15) * 1000);
    const wait = base + Math.floor(Math.random() * 15_000);
    await sleep(wait);
  }

  const { count: remaining } = await db
    .from("wa_messages")
    .select("id", { count: "exact", head: true })
    .eq("campaign_id", campaign.id)
    .eq("status", "pending");
  if (!campaignPaused && !remaining) {
    await db.from("wa_campaigns").update({ status: "completed" }).eq("id", campaign.id);
  }
}

function sleep(ms) { return new Promise((r) => setTimeout(r, ms)); }

async function sendTextMessage(client, phone, text, userId) {
  const messageRisk = inspectMessageRisk(text);
  let lastError;
  for (let attempt = 1; attempt <= SEND_RETRY_LIMIT; attempt++) {
    try {
      return await sendTextMessageOnce(client, phone, text, userId);
    } catch (err) {
      lastError = normalizeSendError(err, messageRisk);
      if (!shouldRetrySend(err, attempt, messageRisk)) throw lastError;
      console.warn(`[${userId}] send attempt ${attempt} failed; recovering WhatsApp Web before retry: ${lastError.message}`);
      await recoverWhatsAppWeb(client, userId);
      await sleep(18_000 + Math.floor(Math.random() * 12_000));
    }
  }
  throw lastError || new Error("WhatsApp send failed");
}

async function sendTextMessageOnce(client, phone, text, userId) {
  const normalizedPhone = normalizePhone(phone);
  if (!/^8801\d{9}$/.test(normalizedPhone)) throw new Error("Invalid Bangladesh mobile number");

  let targetId = `${normalizedPhone}@c.us`;
  try {
    const numberId = await withTimeout(client.getNumberId(normalizedPhone), 10_000, "number-check-timeout");
    if (numberId === null) throw new Error("Not on WhatsApp");
    if (numberId?._serialized) targetId = numberId._serialized;
  } catch (e) {
    if (e.message !== "number-check-timeout") throw e;
    console.warn(`[${userId}] getNumberId timed out for ${normalizedPhone}; trying direct chat id`);
  }

  let sentMessage;
  try {
    sentMessage = await withTimeout(
      client.sendMessage(targetId, text, { linkPreview: false, sendSeen: false, waitUntilMsgSent: true }),
      60_000,
      "WhatsApp message send timed out before server confirmation"
    );
  } catch (e) {
    const message = String(e?.message || e || "WhatsApp send failed");
    console.warn(`[${userId}] confirmed library send failed for ${normalizedPhone}: ${message}`);
    throw makeSendError(message.includes("Waiting for selector")
      ? "WhatsApp Web UI changed or chat did not open; message was not confirmed sent"
      : message, message.includes("Waiting for selector") ? "WA_SELECTOR" : "WA_SEND_FAILED");
  }

  await confirmMessageServerAck(client, sentMessage, targetId, userId, normalizedPhone);
  return sentMessage;
}

async function confirmMessageServerAck(client, sentMessage, targetId, userId, normalizedPhone) {
  const messageId = sentMessage?.id?._serialized;
  if (!messageId) throw new Error("WhatsApp did not return a message id");

  const initialAck = Number.isFinite(sentMessage.ack) ? sentMessage.ack : -99;
  if (initialAck >= 1) return;
  if (initialAck < 0) throw makeSendError("WhatsApp rejected the message", "WA_ACK_ERROR");

  const ack = await waitForMessageAck(client, messageId, 45_000);
  if (ack >= 1) return;

  const pageAck = await readMessageAckFromStore(client, messageId);
  if (pageAck >= 1) return;

  const state = await getClientConnectionState(client);
  console.warn(`[${userId}] no server ack for ${normalizedPhone} (${targetId}, ${messageId}); state=${state}; ack=${ack}; pageAck=${pageAck}`);
  throw makeSendError("WhatsApp did not confirm delivery to server; message not marked sent", "WA_ACK_TIMEOUT");
}

function makeSendError(message, code) {
  const error = new Error(message);
  error.code = code;
  return error;
}

function inspectMessageRisk(text) {
  const message = String(text || "");
  const urls = (message.match(/https?:\/\/|www\.|\.com|\.net|\.org|\.io|\.app/gi) || []).length;
  const emoji = (message.match(/[\u{1F300}-\u{1FAFF}]/gu) || []).length;
  const lines = message.split(/\r?\n/).filter((line) => line.trim()).length;
  const isHighRisk = message.length > 500 || urls > 1 || emoji > 6 || lines > 10;
  return { isHighRisk, length: message.length, urls, emoji, lines };
}

function normalizeSendError(err, risk) {
  const message = String(err?.message || err || "WhatsApp send failed");
  const code = err?.code || "WA_SEND_FAILED";
  if (code === "WA_ACK_ERROR") {
    return makeSendError(
      risk.isHighRisk
        ? `WhatsApp rejected the message before server delivery. Message looks too promotional/long (${risk.length} chars, ${risk.emoji} emoji, ${risk.urls} links); shorten it and use a slower delay.`
        : "WhatsApp rejected the message before server delivery. This usually means the account, number, or content was rate-limited by WhatsApp.",
      code
    );
  }
  return makeSendError(message, code);
}

function shouldRetrySend(err, attempt, risk) {
  if (attempt >= SEND_RETRY_LIMIT) return false;
  const code = err?.code;
  if (code === "WA_SELECTOR" || code === "WA_ACK_TIMEOUT") return true;
  if (code === "WA_ACK_ERROR") return !risk.isHighRisk;
  return /timeout|detached|navigation|protocol|execution context|target closed/i.test(String(err?.message || err || ""));
}

async function recoverWhatsAppWeb(client, userId) {
  try {
    if (typeof client.resetState === "function") await withTimeout(client.resetState(), 20_000, "resetState-timeout");
  } catch (e) {
    console.warn(`[${userId}] resetState failed: ${String(e?.message || e)}`);
  }
  try {
    const page = client.pupPage;
    if (page && !page.isClosed()) await page.goto("https://web.whatsapp.com/", { waitUntil: "domcontentloaded", timeout: 45_000 });
  } catch (e) {
    console.warn(`[${userId}] WhatsApp Web reload failed: ${String(e?.message || e)}`);
  }
}

function waitForMessageAck(client, messageId, timeoutMs) {
  return new Promise((resolve) => {
    let done = false;
    let bestAck = -99;
    const finish = (ack) => {
      if (done) return;
      done = true;
      clearTimeout(timer);
      client.off("message_ack", onAck);
      resolve(ack);
    };
    const onAck = (msg, ack) => {
      if (msg?.id?._serialized !== messageId) return;
      bestAck = Math.max(bestAck, Number(ack));
      if (bestAck >= 1 || bestAck < 0) finish(bestAck);
    };
    const timer = setTimeout(() => finish(bestAck), timeoutMs);
    client.on("message_ack", onAck);
  });
}

async function readMessageAckFromStore(client, messageId) {
  try {
    return await client.pupPage.evaluate(async (id) => {
      const store = window.require?.("WAWebCollections")?.Msg;
      const msg = store?.get(id) || (await store?.getMessagesById?.([id]))?.messages?.[0];
      return typeof msg?.ack === "number" ? msg.ack : -99;
    }, messageId);
  } catch {
    return -99;
  }
}

async function getClientConnectionState(client) {
  try {
    return await client.getState();
  } catch {
    return "unknown";
  }
}

const COMPOSER_SELECTORS = [
  'footer div[contenteditable="true"][role="textbox"]',
  'footer div[contenteditable="true"][data-lexical-editor="true"]',
  'footer div[contenteditable="true"][data-tab]',
  'footer div[contenteditable="true"]',
  'div[contenteditable="true"][role="textbox"]',
  'div[contenteditable="true"][data-lexical-editor="true"]',
  'div[contenteditable="true"][data-tab]',
  'div[contenteditable="true"]',
];

const SEND_BUTTON_SELECTORS = [
  'button[aria-label="Send"]',
  'button[aria-label*="Send"]',
  'div[role="button"][aria-label="Send"]',
  'div[role="button"][aria-label*="Send"]',
  'span[data-icon="send"]',
  '[data-icon="send"]',
];

async function sendViaBrowserCompose(client, normalizedPhone, text, userId) {
  const page = client.pupPage;
  if (!page || page.isClosed()) throw new Error("WhatsApp browser page is not available");

  const chatUrl = `https://web.whatsapp.com/send?phone=${normalizedPhone}&text=${encodeURIComponent(text)}&app_absent=0`;
  await page.goto(chatUrl, { waitUntil: "domcontentloaded", timeout: 45_000 });

  const composerSelector = await waitForComposeOrFailure(page, normalizedPhone, 60_000);
  await page.evaluate((selectors, preferredSelector) => {
    const preferred = preferredSelector ? document.querySelector(preferredSelector) : null;
    const fallback = selectors.map((selector) => document.querySelector(selector)).find(Boolean);
    const box = preferred || fallback;
    if (box) {
      box.scrollIntoView({ block: "center", inline: "nearest" });
      box.focus();
    }
  }, COMPOSER_SELECTORS, composerSelector);
  await sleep(750);

  await ensureComposerHasText(page, text);
  const before = await countOutgoingMarkers(page);
  const clickedSend = await clickSendButton(page);
  if (!clickedSend) await page.keyboard.press("Enter");

  const sentOrCleared = await page.waitForFunction(
    ({ selectors, previousCount }) => {
      const boxes = selectors.map((selector) => document.querySelector(selector)).filter(Boolean);
      const box = boxes[boxes.length - 1];
      const stillHasText = Boolean(box?.textContent?.trim());
      const markerCount = document.querySelectorAll('[data-icon="msg-check"], [data-icon="msg-dblcheck"], [data-icon="msg-time"]').length;
      return !stillHasText || markerCount > previousCount;
    },
    { timeout: 20_000 },
    { selectors: COMPOSER_SELECTORS, previousCount: before }
  ).then(() => true).catch(() => false);

  if (!sentOrCleared) {
    const state = await getWhatsAppPageState(page);
    console.warn(`[${userId}] browser compose did not confirm send for ${normalizedPhone}: ${JSON.stringify(state)}`);
    throw new Error(state.reason || "WhatsApp browser compose did not send the message");
  }

  return { id: { _serialized: `browser-compose-${Date.now()}` } };
}

async function waitForComposeOrFailure(page, normalizedPhone, timeoutMs) {
  const started = Date.now();
  let lastState = null;
  while (Date.now() - started < timeoutMs) {
    lastState = await getWhatsAppPageState(page);
    if (lastState.composerSelector || lastState.hasSendButton) return lastState.composerSelector;
    if (lastState.reason) throw new Error(lastState.reason);
    await sleep(1000);
  }
  throw new Error(lastState?.summary || `WhatsApp chat composer did not open for ${normalizedPhone}`);
}

async function getWhatsAppPageState(page) {
  return await page.evaluate(({ composerSelectors, sendButtonSelectors }) => {
    const bodyText = document.body?.innerText || "";
    const composerSelector = composerSelectors.find((selector) => document.querySelector(selector)) || null;
    const hasSendButton = sendButtonSelectors.some((selector) => document.querySelector(selector));
    const lower = bodyText.toLowerCase();
    let reason = null;
    if (lower.includes("phone number shared via url is invalid") || lower.includes("invalid phone number")) {
      reason = "WhatsApp says this phone number is invalid";
    } else if (lower.includes("not a whatsapp user") || lower.includes("couldn't look up phone number")) {
      reason = "This phone number is not on WhatsApp";
    } else if ((lower.includes("scan this qr code") || lower.includes("link with phone number")) && !composerSelector) {
      reason = "WhatsApp session expired. Reconnect WhatsApp";
    } else if ((lower.includes("computer not connected") || lower.includes("trying to reach phone")) && !composerSelector) {
      reason = "WhatsApp Web is offline. Keep the phone and server connected";
    }
    return {
      url: location.href,
      composerSelector,
      hasSendButton,
      reason,
      summary: `WhatsApp composer not found. URL=${location.href}; text=${bodyText.slice(0, 180).replace(/\s+/g, " ")}`,
    };
  }, { composerSelectors: COMPOSER_SELECTORS, sendButtonSelectors: SEND_BUTTON_SELECTORS });
}

async function ensureComposerHasText(page, text) {
  const hasText = await page.evaluate((selectors) => {
    const boxes = selectors.map((selector) => document.querySelector(selector)).filter(Boolean);
    const box = boxes[boxes.length - 1];
    if (!box) return false;
    box.focus();
    return Boolean(box.textContent?.trim());
  }, COMPOSER_SELECTORS);

  if (!hasText) {
    await page.keyboard.type(text, { delay: 0 });
  }
}

async function clickSendButton(page) {
  return await page.evaluate((selectors) => {
    for (const selector of selectors) {
      const el = document.querySelector(selector);
      if (!el) continue;
      const clickable = el.closest('button, div[role="button"], span[role="button"]') || el;
      clickable.click();
      return true;
    }
    return false;
  }, SEND_BUTTON_SELECTORS);
}

async function countOutgoingMarkers(page) {
  return await page.evaluate(() => document.querySelectorAll('[data-icon="msg-check"], [data-icon="msg-dblcheck"], [data-icon="msg-time"]').length);
}

function normalizePhone(phone) {
  return String(phone || "").replace(/[^0-9]/g, "");
}
function withTimeout(promise, ms, message) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

server.listen(PORT, () => console.log(`LeadForge WA server listening on :${PORT}`));
