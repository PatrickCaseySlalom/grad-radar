# Pump.fun Radar (Early + Graduated)

Free Telegram alerts for Pump.fun coins in two lanes, both behind a RugCheck safety gate:

- **🌱 Early:** low-cap coins still on the bonding curve ($8K to $45K) with broad, growing buyer demand. Earliest entry, highest risk.
- **🎓 Graduated:** coins that just moved to PumpSwap ($20K to $150K) with real momentum. Later entry, but liquidity and LP lock are verifiable.

A coin can alert in both lanes. An Early alert followed by a Graduated alert is confirmation. It runs on GitHub, scanning every 60 seconds. No servers, API keys or paid services.

**Setup time:** about 15 minutes. **Cost:** $0.

---

## Step 1: Create the Telegram bot (3 min)

1. In Telegram, search for **@BotFather** (blue check mark) and tap **Start**.
2. Send `/newbot`. Give it any name (e.g. `Grad Radar`), then a username ending in `bot` (e.g. `patrick_gradradar_bot`).
3. BotFather replies with a **token** that looks like `8123456789:AAH...`. Copy it somewhere safe. This is your `TG_BOT_TOKEN`.
4. Tap the link BotFather gives you to open your new bot, then tap **Start** and send it any message (e.g. `hi`). This step matters: a bot cannot message you until you message it first.

## Step 2: Get your chat ID (2 min)

1. In a browser, open this address with your token pasted in (no angle brackets):
   `https://api.telegram.org/bot<YOUR_TOKEN>/getUpdates`
2. Find `"chat":{"id":` and copy the number after it (e.g. `512345678`). This is your `TG_CHAT_ID`.
3. If the page shows `"result":[]`, send your bot another message and refresh.

## Step 3: Create the GitHub repo (5 min)

1. Sign in at **github.com** (create a free account if needed).
2. Top right **+** > **New repository**. Name: `grad-radar`. Choose **Public** (this keeps Actions free; your secrets stay private). Tick nothing else. Click **Create repository**.
3. On the new repo page, click **uploading an existing file**.
4. Unzip `grad-radar.zip` on your computer. Open the unzipped folder, select **everything inside it**, and drag it onto the GitHub upload page. Click **Commit changes**.
5. **Check the workflow file arrived.** Click into the repo's file list. You should see a `.github` folder. If it is missing (some computers hide folders that start with a dot):
   - Click **Add file > Create new file**.
   - In the name box type exactly `.github/workflows/grad-radar.yml` (the slashes create the folders).
   - Open `grad-radar.yml` from the unzipped folder in a text editor (on Mac: press Cmd+Shift+. in Finder to see hidden folders), copy everything, paste it in, and click **Commit changes**.

## Step 4: Add your two secrets (2 min)

1. Repo > **Settings** > left menu **Secrets and variables** > **Actions** > **New repository secret**.
2. Name `TG_BOT_TOKEN`, value: your bot token. **Add secret**.
3. Repeat: name `TG_CHAT_ID`, value: your chat ID number.

## Step 5: Turn it on and test (3 min)

1. Repo > **Actions** tab. If asked, click **I understand my workflows, go ahead and enable them**.
2. Click **Graduation radar** in the left list > **Run workflow** > set mode to **test** > **Run workflow**.
3. Within about a minute your bot should send: *"Pump.fun radar is connected."* If it does, you're live. It now scans every 60 seconds on its own.
4. Optional: run it once more with mode **scan** and open the run. The log should show `discovery: +N watched` and `scan done`.

A quiet chat is normal. Alerts only fire when a coin passes every rule, which may be a few times a day.

---

## What triggers an alert

| Check | 🌱 Early (bonding curve) | 🎓 Graduated (PumpSwap) |
|---|---|---|
| Timing | Launched in the last 2 hours | Graduated in the last 45 min |
| Market cap | $8K to $45K | $20K to $150K |
| Demand | 60+ unique buyers in 1h, 20+ in last 15m, buyers 1.3x sellers, and buyers rising since the last scan | 150+ buys since graduation, buys 1.15x sells |
| Volume / depth | $5K+ volume in 1h | Liquidity $8K+ and 10%+ of market cap |
| Not dumping | 5-min change better than -15% | Same |
| Holders | Top 10 at most 25%, no wallet over 8%, dev holds 5% or less | Top 10 at most 30%, no wallet over 10%, LP 90%+ locked |
| Always required | Mint and freeze revoked, no transfer fee or dangerous extensions, not flagged rugged, no danger-level risk, zero linked insider wallets | Same |
| Shown as flags | Similar-size holder cluster (possible bundle), dev already exited, dev's other launches | Same |

**How the original criteria map:** volume, holders, price appreciation and buyer traction are scored automatically. Rug and scam risk is the RugCheck gate. Developer track record is flagged when RugCheck lists the dev's other launches, and every alert links to GMGN's Dev tab for the full history. Social traction is optional (`require_socials`), since a linked X account is easy to fake.

Each coin alerts once per lane. Every alert links to GMGN, RugCheck, Bubblemaps and the chart.

## Monitoring

- **Daily:** read each alert, open the GMGN link, check Bundle % and dev history before acting.
- **If alerts seem to stop:** Actions tab. A healthy radar shows a run in progress (yellow) or recently finished (green); each run lasts about 5.5 hours. A red run: click it and read the `scan error:` line. Occasional `rate-limited, skipped` lines are normal.
- **Keep it alive:** GitHub pauses scheduled workflows in a public repo after 60 days with no commits. Edit any file (even this README) once a month, or re-enable it from the Actions tab if you get the pause email.
- **Score a coin manually** (needs Python on your computer): `python -m pipeline.grad_radar --check <CONTRACT_ADDRESS>` shows exactly which rules it passes or fails.

## Tuning (edit `config.yaml` in GitHub, commit, done)

| If you see | Change |
|---|---|
| Too many Early alerts that die | `early_min_unique_buyers_h1: 100`, or `block_on_cluster: true` |
| Early alerts arrive too late | `early_max_market_cap: 30000` |
| Too many Graduated alerts that fade | `min_buys_since_graduation: 250` |
| Graduated alerts after most of the move | `max_minutes_since_graduation: 25` and `max_market_cap: 100000` |
| Too much noise overall | `early_enabled: false` to run the Graduated lane only |
| Almost no alerts after 3 days | Lower the buyer minimums by about a third |

The scorecard that matters: of your Early alerts, how many went on to graduate? Run `python -m pipeline.grad_radar --summary` (it reports `early_graduated` and 2x hit rates) and tune toward that number.

## Known limits

- **Speed:** each GitHub run scans every 60 seconds for about 5.5 hours. GitHub starts new runs unreliably, so there can be gaps of up to a few hours between runs. If that costs you Early alerts, move to an always-on host (~$5/month) with `python -m pipeline.grad_radar --loop 30`.
- **Coverage:** free GeckoTerminal feeds favor coins already showing momentum. It will not see every graduation.
- **Safety:** RugCheck rules out contract-level traps, not holders dumping. Most Pump.fun coins never graduate, so treat Early alerts as small-size bets. Every alert is a lead to check, not advice.

## Security

- `TG_BOT_TOKEN` only controls this alert bot. If it leaks, send `/revoke` to @BotFather and update the secret.
- GitHub never shows secrets in logs or to people who copy your repo.
