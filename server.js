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
      const chatId = msg.phone.replace(/^\+/, "") + "@c.us";

      // Try to resolve the real WhatsApp ID. If it returns null → not on WhatsApp.
      // If it times out → skip the check and try sending anyway (whatsapp-web.js
      // sometimes hangs on getNumberId even for valid numbers).
      let numberId = null;
      try {
        numberId = await withTimeout(
          s.client.getNumberId(chatId),
          15_000,
          "number-check-timeout"
        );
        if (numberId === null) throw new Error("Not on WhatsApp");
      } catch (e) {
        if (e.message !== "number-check-timeout") throw e;
        // fall through — attempt send anyway
      }

      const targetId = numberId ? numberId._serialized : chatId;

      const sentMessage = await withTimeout(
        s.client.sendMessage(targetId, msg.rendered_message || campaign.message_template, {
          linkPreview: false,
          sendSeen: false,
          waitUntilMsgSent: true,
        }),
        90_000,
        "Sending WhatsApp message timed out"
      );
      if (!sentMessage) throw new Error("WhatsApp could not open this chat");
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

    const base = campaign.delay_seconds * 1000;
    const wait = Math.max(8000, base + (Math.random() * 10000 - 5000));
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
function withTimeout(promise, ms, message) {
  return Promise.race([
    promise,
    new Promise((_, reject) => setTimeout(() => reject(new Error(message)), ms)),
  ]);
}

server.listen(PORT, () => console.log(`LeadForge WA server listening on :${PORT}`));
