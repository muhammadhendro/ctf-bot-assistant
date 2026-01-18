# 🚩 CTF Telegram Bot

A powerful Telegram bot built on [Cloudflare Workers](https://workers.cloudflare.com/) to assist CTF teams in managing events, tracking challenges, and monitoring solves in real-time.

## ✨ Features

- **Event Management**: Add, list, archive, and manage multiple CTF events.
- **CTFd Integration**: Connects to CTFd-based platforms to sync challenges and solves.
- **Real-time Notifications**: Automatic alerts for new challenges and solves (via Cron triggers).
- **Leaderboard**: Track team/user progress with `/top`.
- **Writeup Library**: Store and retrieve writeups for challenges.
- **Auto-Sync**: Automatically fetches event start/finish times from CTFtime.
- **Countdown**: Visual countdown for upcoming events.

## 🛠 Prerequisites

- [Node.js](https://nodejs.org/)
- [Cloudflare Wrangler CLI](https://developers.cloudflare.com/workers/wrangler/install-and-update/) (`npm install -g wrangler`)
- A Telegram Bot Token (from [@BotFather](https://t.me/BotFather))
- A Cloudflare Account

## 🚀 Installation & Setup

1. **Clone the Repository**
   ```bash
   git clone <your-repo-url>
   cd bot-tele
   ```

2. **Install Dependencies**
   ```bash
   npm install
   ```

3. **Configure & Deploy**
   
   The project uses `wrangler.toml` for configuration.
   
   **Step 1: Authenticate Wrangler**
   ```bash
   npx wrangler login
   ```

   **Step 2: Create KV Namespace**
   The bot uses Cloudflare KV for storage. Create a namespace:
   ```bash
   npx wrangler kv:namespace create "CTFD_STORE"
   ```
   *Copy the `id` from the output and update `wrangler.toml`:*
   ```toml
   [[kv_namespaces]]
   binding = "CTFD_STORE"
   id = "YOUR_KV_ID_HERE"
   ```

   **Step 3: Set Secrets**
   Set your Telegram Bot Token securely:
   ```bash
   npx wrangler secret put TELEGRAM_BOT_TOKEN
   ```
   *(Paste your token when prompted)*

   **Step 4: Deploy**
   ```bash
   npx wrangler deploy
   ```

4. **Set Webhook**
   After deployment, connect your bot to Telegram using the provided script or manually:
   ```powershell
   # Using the script (Windows Powershell)
   ./set-webhook.ps1
   ```
   *Or manually:*
   `https://api.telegram.org/bot<YOUR_TOKEN>/setWebhook?url=<YOUR_WORKER_URL>`

## 🤖 Commands

### 📅 Event Management
- `/list_events` - List active events & countdowns.
- `/add_event <Name> <URL>` - Add a new CTF event (Auto-syncs time if found on CTFtime).
- `/join_event <id> <token>` - Join an event (Private Chat).
- `/set_event_time <id> <YYYY-MM-DD HH:mm>` - Manually set event start time.
- `/archive_event <id>` - Archive an ended event.
- `/archived_events` - List archived events.
- `/delete_event <id>` - Permanently delete an event.

### ⚔️ Challenges & Solves
- `/challenges` or `/chal` - List challenges for the current event.
- `/chal <id>` - View challenge details (e.g. `/chal web-1`).
- `/sync_solves` - Manually sync solves.
- `/top` - View the leaderboard.
- `/profile` - View your stats.
- `/team` - View team info and members.

### 📝 Writeups
- `/add_writeup <Challenge> <URL>` - Save a writeup.
- `/writeups` - List all writeups.
- `/delete_writeup <Name> [URL]` - Delete a writeup.

### ⚙️ Settings
- `/set_event <id>` - Set default event for the current group/chat.
- `/set_notify` - Enable notifications in the current channel/group.
- `/unset_notify` - Disable notifications.

## 🤝 Contributing

Pull requests are welcome! For major changes, please open an issue first to discuss what you would like to change.

## 📄 License

[MIT](https://choosealicense.com/licenses/mit/)
