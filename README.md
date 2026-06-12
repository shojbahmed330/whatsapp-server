# LeadForge — WhatsApp Bulk Messaging Server

This is a small Node.js worker that runs `whatsapp-web.js`. It cannot live inside Lovable's serverless runtime (whatsapp-web.js needs a long-lived Puppeteer/Chromium process), so it ships as its own deployable folder.

## What it does

- One WhatsApp client per user (keyed by Supabase user id)
- Streams the QR code to the frontend via Socket.io
- Sends bulk campaigns with random delays, typing indicator, duplicate prevention, daily-limit enforcement, and auto-pause on 3 consecutive failures
- Writes message status straight to Supabase (`wa_messages` / `wa_campaigns`)

## Required env vars

| Name | Where to find |
| --- | --- |
| `SUPABASE_URL` | LeadForge → Lovable Cloud / Backend |
| `SUPABASE_SERVICE_ROLE_KEY` | Lovable Cloud → service role (server-only) |
| `SUPABASE_ANON_KEY` | Lovable Cloud → publishable/anon key |
| `PORT` | provided by host (Railway/Render set automatically) |

## Deploy to Railway (recommended, free)

1. Push this `whatsapp-server/` folder to its own GitHub repo (or use Railway's "Deploy from local" CLI).
2. New Project → "Deploy from GitHub" → pick the repo.
3. Variables tab: paste the 3 secrets above.
4. Settings → Networking → Generate Domain. Copy the URL (e.g. `https://leadforge-wa.up.railway.app`).
5. In LeadForge → Settings → **WhatsApp Server**, paste that URL → Save → Test.
6. Go to **WA Connect** → Generate QR → scan from your phone (WhatsApp → Linked Devices → Link a Device).

Railway's default Node image already includes the libs Puppeteer needs.

## Deploy to Render (alternative)

1. New → Web Service → connect repo
2. Runtime: Node · Build `npm install` · Start `npm start`
3. Add env vars; add this Build Command to ensure Chromium libs are present:
   ```
   apt-get update && apt-get install -y libnss3 libatk-bridge2.0-0 libgtk-3-0 libxss1 libasound2 && npm install
   ```

## Run locally

```bash
cd whatsapp-server
npm install
SUPABASE_URL=... SUPABASE_SERVICE_ROLE_KEY=... SUPABASE_ANON_KEY=... npm start
```

The server listens on `http://localhost:3000`. Paste that into LeadForge → Settings.

## API endpoints (all auth: `Authorization: Bearer <supabase access token>`)

| Method | Path | Body |
| --- | --- | --- |
| GET  | `/api/whatsapp/status` | — |
| POST | `/api/whatsapp/connect` | — |
| POST | `/api/whatsapp/disconnect` | — |
| POST | `/api/whatsapp/send-bulk` | `{ campaign_id }` |
| POST | `/api/whatsapp/control` | `{ campaign_id, action: "pause"\|"resume"\|"stop" }` |

Socket.io events: `qr`, `ready`, `disconnected`, `status`.

## Safety features baked in

- Random delay (`delay_seconds ± 5s`, floor 8s)
- 2–3s typing indicator before each message
- Daily-limit enforcement per user (counted from `wa_messages.sent_at`)
- Skips numbers messaged successfully in the last 30 days
- `isRegisteredUser` check before sending → marks "Not on WhatsApp" instead of crashing
- Auto-pauses campaign after 3 consecutive failures
