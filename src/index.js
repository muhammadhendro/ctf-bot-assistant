// In-memory store for rate limiting (Note: Isolate-specific, not global across all edge nodes)
const userLastRequest = new Map();

export default {
  async fetch(request, env, ctx) {
    if (request.method === "POST") {
      try {
        const payload = await request.json();
        console.log("Received payload:", JSON.stringify(payload, null, 2));

        if (payload.message) {
          const chatId = payload.message.chat.id;
          // Extract chat type to distinguish private (DM) vs group/supergroup
          const chatType = payload.message.chat.type;
          const userId = payload.message.from.id;
          const telegramName = (payload.message.from.username) ? `@${payload.message.from.username}` : payload.message.from.first_name;
          const text = (payload.message.text || "").trim();

          // --- RATE LIMIT CHECK ---
          const now = Date.now();
          const lastRequestTime = userLastRequest.get(userId) || 0;
          const RATE_LIMIT_MS = 500; // 0.5 seconds

          if (now - lastRequestTime < RATE_LIMIT_MS) {
            console.log(`Rate limit hit for user ${userId}. Ignoring request.`);
            // Silent ignore to prevent spam
            return new Response("OK");
          }
          // Update last request time
          userLastRequest.set(userId, now);

          // Cleanup old entries periodically (optional optimization to prevent leaking memory)
          if (userLastRequest.size > 10000) userLastRequest.clear();
          // ------------------------

          // 1. Check Membership (Skip for groups)
          const channelUsername = "@CTF_Group";
          let isMember = true; // Default allow for groups

          if (chatType === 'private') {
            try {
              isMember = await this.checkMembership(env.TELEGRAM_BOT_TOKEN, channelUsername, userId);
              console.log(`[MEMBERSHIP] User ${userId} check result: ${isMember}`);
            } catch (e) {
              console.error("Membership Check Failed", e);
              isMember = true;
            }

            if (!isMember) {
              console.log(`User ${userId} is NOT a member of ${channelUsername}. Blocking.`);
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId,
                `⚠️ **Access Denied**\n\nYou must join our channel @CTF\\_Group to use this bot.\n\nPlease join here: [t.me/CTF\\_Group](https://t.me/CTF_Group)`,
                true
              );
              return new Response("OK");
            }
          }



          // 2. Process Message
          if (text) {
            console.log(`[CMD] Processing: ${text}`);
            if (text.startsWith("/help")) {
              console.log("[CMD] Executing /help");
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId,
                `🛠 <b>Bantuan Bot</b>

🌐 <b>Event Management</b>
<code>/ctf [running|upcoming]</code> - List Event from CTFTime
<code>/list_events</code> - List Event Terdaftar
<code>/add_event "Name" &lt;url&gt;</code> - Add New Event
<code>/archive_event &lt;id&gt;</code> - Archive Event 📦
<code>/archived_events</code> - List Arsip
<code>/unarchive_event &lt;id&gt;</code> - Restore Event
<code>/delete_event &lt;id&gt;</code> - Hapus Event Permanen 🗑
<code>/set_event &lt;id&gt;</code> - Set Event Aktif

🔐 <b>Auth & Context (DM Only)</b>
<code>/join_event &lt;id&gt; &lt;token&gt;|&lt;user&gt; &lt;pass&gt;</code> - Join Event 

👥 <b>Team & Stats</b>
<code>/profile</code> - Cek Profile & Register Score 👤
<code>/team</code> - Cek Info Tim 🛡
<code>/leaderboard</code> - Rank Member dalam Tim (Points + Solves) 📊
<code>/scoreboard [limit]</code> - Top Team (CTFd Global)
<code>/sync_solves</code> - Manual Sync Solves 🔄

📊 <b>Challenge & Monitor</b>
<code>/challenges [all|solved]</code> - List Soal (Cache)
<code>/chal &lt;name|id&gt;</code> - Detail Soal
<code>/set_notify [all|id]</code> - Set Notif ke Group ini 📢
<code>/unset_notify</code> - Stop Notif Group 🔕
<code>/init_challenges &lt;id&gt;</code> - Init Cache (Resumable)
<code>/refresh_challenges &lt;id&gt;</code> - Refresh Cache
<code>/delete_challenges &lt;id&gt;</code> - Clear Cache
<code>/add_writeup &lt;chall&gt; &lt;url&gt;</code> - Simpan Writeup 📝
<code>/delete_writeup &lt;chall&gt; &lt;url&gt;</code> - Hapus Writeup 🗑️
<code>/writeups [event_id]</code> - List Writeup 📚

Silakan bergabung di @CTF_Group untuk info lebih lanjut.`,
                "HTML"
              );
            } else if (text.startsWith("/ping")) {
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "Pong! 🏓 (v4 - CLEAN)", true);

            } else if (text.startsWith("/start")) {
              // Handle deep links: /start chal_<eventId>_<chalId>
              // Wrap in waitUntil to prevent timeouts
              ctx.waitUntil((async () => {
                const args = text.trim().split(/\s+/);

                if (args.length > 1 && args[1].startsWith("chal_")) {
                  // Format: chal_<eventId>_<chalId>
                  // Challenge ID is always the last part (numeric)
                  // Event ID is everything between "chal_" and "_<chalId>"

                  const payload = args[1];
                  const lastUnderscoreIndex = payload.lastIndexOf("_");

                  if (lastUnderscoreIndex > 4) { // "chal_" len is 5, need at least 1 char for eventId
                    const chalIdStr = payload.substring(lastUnderscoreIndex + 1);
                    const eventId = payload.substring(5, lastUnderscoreIndex); // 5 is len of "chal_"
                    const chalId = parseInt(chalIdStr);

                    if (!isNaN(chalId) && eventId) {
                      try {
                        // Load challenge from cache
                        const cacheKey = `CHALLENGES_${eventId}`;
                        const cached = await env.CTFD_STORE.get(cacheKey);

                        if (!cached) {
                          await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Cache challenges tidak ditemukan.", true);
                          return;
                        }

                        const challenges = JSON.parse(cached);
                        const challenge = challenges.find(c => c.id === chalId);

                        if (!challenge) {
                          await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Challenge dengan ID ${chalId} tidak ditemukan.`, true);
                          return;
                        }

                        // Format challenge details (reuse existing /chal formatting logic)
                        const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

                        let msg = `🎯 <b>${escapeHtml(challenge.name)}</b>\n\n`;
                        msg += `📁 Category: <b>${escapeHtml(challenge.category || "N/A")}</b>\n`;
                        msg += `💎 Value: <b>${challenge.value || 0} pts</b>\n`;

                        if (challenge.solves !== undefined) {
                          msg += `✅ Solves: <b>${challenge.solves}</b>\n`;
                        }

                        msg += `\n`;

                        // Check Solved Status (from Subscription Cache)
                        let solvedBy = null;
                        try {
                          // Try get solved status
                          // We need to look up SUBSCRIPTIONS to find if this user/team solved it
                          const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                          if (sStored) {
                            const subs = JSON.parse(sStored);
                            // Find subscription for this user & event
                            const sub = subs.find(s => s.userId === chatId && s.eventId === eventId);
                            if (sub && sub.lastSolves) {
                              const solvedEntry = sub.lastSolves.find(s => s.challenge_id === chalId);
                              if (solvedEntry) {
                                // Solved!
                                // API usually just gives "user_id" in team mode, or "team_id" in user mode depending on endpoint.
                                // If we have user name in future, we can display it. For now, checking if we can get name.
                                // If solvedEntry has 'user' object (some CTFd versions), use it.
                                if (solvedEntry.user && solvedEntry.user.name) {
                                  solvedBy = solvedEntry.user.name;
                                } else if (solvedEntry.username) {
                                  solvedBy = solvedEntry.username;
                                } else {
                                  solvedBy = "Team / You"; // Default if name missing
                                }
                              }
                            }
                          }
                        } catch (e) { }

                        if (solvedBy) {
                          msg += `✅ <b>SOLVED</b> by <b>${escapeHtml(solvedBy)}</b>\n\n`;
                        }

                        msg += `📝 <b>Description:</b>\n${escapeHtml(challenge.description || "No description")}\n`;

                        if (challenge.files && challenge.files.length > 0) {
                          msg += `\n📎 <b>Files:</b>\n`;

                          // Find base URL from Events list
                          let baseUrl = "";
                          if (eventId) {
                            try {
                              const eStored = await env.CTFD_STORE.get("EVENTS");
                              if (eStored) {
                                const allEvents = JSON.parse(eStored);
                                const ev = allEvents.find(e => e.id === eventId);
                                if (ev) baseUrl = ev.url;
                              }
                            } catch (e) { }
                          }

                          challenge.files.forEach(f => {
                            const fileStr = (typeof f === 'string') ? f : f.url;
                            const fullUrl = fileStr.startsWith("http") ? fileStr : `${baseUrl}${fileStr}`;
                            const fileName = fileStr.split('/').pop().split('?')[0];
                            msg += `• <a href="${fullUrl}">${escapeHtml(fileName)}</a>\n`;
                          });
                        }

                        if (challenge.tags && challenge.tags.length > 0) {
                          msg += `\n🏷 <b>Tags:</b> ${challenge.tags.map(t => `<code>${escapeHtml(t)}</code>`).join(", ")}\n`;
                        }

                        await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");

                      } catch (e) {
                        console.error("Deep Link Handler Error:", e);
                        await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
                      }

                      return;
                    }
                  }
                }

                // Default /start message if no deep link or valid payload
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "👋 Selamat datang! Ketik /help untuk melihat daftar command.", true);
              })());

              return new Response("OK");

            } else if (text.startsWith("/add_writeup")) {
              try {
                // Usage: /add_writeup <chall_name> <url>
                const parts = text.trim().split(/\s+/);
                if (parts.length < 3) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/add_writeup <nama_challenge> <url_writeup>`", true);
                  return new Response("OK");
                }

                const url = parts[parts.length - 1];
                let chalName = parts.slice(1, -1).join(" ");

                // Strip quotes if present
                chalName = chalName.replace(/^["']|["']$/g, "");

                if (!url.startsWith("http")) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ URL tidak valid.", true);
                  return new Response("OK");
                }

                // Detect Event ID
                let eventId = null;
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }

                if (!eventId) {
                  let subs = [];
                  try { const s = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (s) subs = JSON.parse(s); } catch (e) { }
                  const mySub = subs.find(s => String(s.userId) === String(userId));
                  if (mySub) eventId = mySub.eventId;
                }

                if (!eventId) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada event aktif. Gunakan `/set_notify <id>` di grup atau `/join_event` di DM.", true);
                  return new Response("OK");
                }

                // Load Writeups
                const WRITEUP_KEY = `WRITEUPS_${eventId}`;
                let writeups = {};
                try { const wStr = await env.CTFD_STORE.get(WRITEUP_KEY); if (wStr) writeups = JSON.parse(wStr); } catch (e) { }

                // Normalize key
                const key = chalName.toLowerCase().replace(/\s+/g, "_");

                if (!writeups[key]) writeups[key] = [];

                // Prevent duplicates
                const exists = writeups[key].some(w => w.url === url);
                if (!exists) {
                  writeups[key].push({
                    author: telegramName, // Use @username if available
                    userId: payload.message.from.id,
                    url: url,
                    date: Date.now()
                  });
                  await env.CTFD_STORE.put(WRITEUP_KEY, JSON.stringify(writeups));
                  const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                  const msg = `✅ <b>Writeup Saved!</b>\n\n` +
                    `🛡 Challenge: <b>${escapeHtml(chalName)}</b>\n` +
                    `📍 Event: ${escapeHtml(eventId)}\n` +
                    `🔗 Link: ${escapeHtml(url)}`;
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");

                  // Broadcast to CTF Channel
                  try {
                    const broadcastMsg = `📝 <b>New Writeup Added!</b>\n\n` +
                      `🛡 Challenge: <b>${escapeHtml(chalName)}</b>\n` +
                      `👤 Author: ${escapeHtml(telegramName)}\n` +
                      `📍 Event: ${escapeHtml(eventId)}\n` +
                      `🔗 Link: ${escapeHtml(url)}`;
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, "@CTF_Channel", broadcastMsg, "HTML");
                  } catch (e) {
                    console.error("Broadcast Writeup Error:", e);
                  }
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Writeup untuk link tersebut sudah ada.`, true);
                }
              } catch (err) {
                console.error("Add Writeup Error:", err);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${err.message}`, true);
              }

            } else if (text.startsWith("/add_event")) {
              // PROCESS IN BACKGROUND to prevent Timeout/Retry-Loop
              ctx.waitUntil((async () => {
                try {
                  // Usage: /add_event <Event Name> <URL>
                  const args = text.replace(/^\/add_event\s+/, "").trim();
                  const parts = args.split(/\s+/);

                  if (parts.length < 2) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/add_event <Nama Event> <URL>`", true);
                    return;
                  }

                  const url = parts[parts.length - 1];
                  let eventName = args.substring(0, args.lastIndexOf(url)).trim();

                  // Strip quotes
                  eventName = eventName.replace(/^["']|["']$/g, "");

                  if (!url.startsWith("http")) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ URL tidak valid. Pastikan diawali http/https.", true);
                    return;
                  }

                  // Save to KV (Persist)
                  let events = [];
                  try {
                    const eStored = await env.CTFD_STORE.get("EVENTS");
                    if (eStored) events = JSON.parse(eStored);
                  } catch (e) { }

                  // Generate ID (e.g. evt_1a2b)
                  const newId = "evt_" + Math.random().toString(16).substr(2, 6);

                  // Check duplicate URL? Optional but good.

                  // Try to Auto-Discover Time from CTFtime (using URL match)
                  let autoStart = null;
                  let autoEnd = null;
                  let extraNote = "";

                  try {
                    // Since we don't have a direct "search by URL" in getCTFTimeEvents, we fetch upcoming/running and look for match
                    // Or query CTFtime API directly for a broader range if strictly needed, but reusing logic is safer.
                    // CTFtime API: events/?limit=100&start=... 
                    const now = Math.floor(Date.now() / 1000);
                    const rangeStart = now - (30 * 24 * 60 * 60); // Check last month too
                    const rangeEnd = now + (90 * 24 * 60 * 60);   // Check next 3 months

                    const ctftimeRes = await fetch(`https://ctftime.org/api/v1/events/?limit=100&start=${rangeStart}&finish=${rangeEnd}`, {
                      headers: { "User-Agent": "TelegramBot/1.0" }
                    });

                    if (ctftimeRes.ok) {
                      const ctfList = await ctftimeRes.json();
                      // Fuzzy match URL
                      // CTFtime URL might be "http" vs "https" or have trailing slash
                      const norm = (u) => (u || "").toLowerCase().replace(/^https?:\/\//, "").replace(/\/$/, "");
                      const targetNorm = norm(url);

                      const match = ctfList.find(c => norm(c.url) === targetNorm || norm(c.ctf_id) === targetNorm); // sometimes url is ctf_id? unlikely. just url.

                      if (match) {
                        autoStart = match.start;
                        autoEnd = match.finish;
                        extraNote = `\n✅ **Auto-Sync:** Waktu event diambil dari CTFtime.`;
                      }
                    }
                  } catch (e) { console.error("Auto Sync CTFtime Error:", e); }

                  // Just push.
                  events.push({
                    id: newId,
                    name: eventName,
                    url: url.endsWith("/") ? url.slice(0, -1) : url,
                    archived: false,
                    addedBy: chatId,
                    start: autoStart,
                    finish: autoEnd
                  });

                  await env.CTFD_STORE.put("EVENTS", JSON.stringify(events));

                  // Broadcast
                  const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                  const broadcastMsg = `📢 <b>New CTF Event!</b>\n\n` +
                    `📛 <b>${escapeHtml(eventName)}</b>\n` +
                    `🔗 ${escapeHtml(url)}\n\n` +
                    `🆔 ID: <code>${newId}</code>\n` +
                    `👉 Join: <code>/join_event ${newId}</code>\n\n` +
                    `<i>Shared by ${escapeHtml(telegramName)}</i>`;

                  // Send to Channel
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, "@CTF_Channel", broadcastMsg, "HTML");

                  // Reply User
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Event Announced!**\nID: \`${newId}\`\nSent to @CTF_Channel`, true);

                } catch (err) {
                  console.error("Add Event Error:", err);
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${err.message}`, true);
                }
              })());

              return new Response("OK");

            } else if (text.startsWith("/delete_writeup")) {
              try {
                // Usage: 
                // 1. /delete_writeup <chall_name> <url> (Delete specific)
                // 2. /delete_writeup <chall_name>       (Delete ALL for that chall)

                const args = text.replace(/^\/delete_writeup\s+/, "").trim();
                if (!args) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/delete_writeup <nama_challenge> [url]`", true);
                  return new Response("OK");
                }

                const parts = args.split(/\s+/);
                const lastPart = parts[parts.length - 1];

                let url = null;
                let chalName = args;

                // Check if last part looks like a URL
                if (lastPart.startsWith("http://") || lastPart.startsWith("https://")) {
                  url = lastPart;
                  // chalName is everything before the URL
                  chalName = args.substring(0, args.lastIndexOf(url)).trim();
                }

                // Strip quotes
                const chalNameStripped = chalName.replace(/^["']|["']$/g, "");

                // Detect Event ID
                let eventId = null;
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  let subs = [];
                  try { const s = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (s) subs = JSON.parse(s); } catch (e) { }
                  const mySub = subs.find(s => String(s.userId) === String(userId));
                  if (mySub) eventId = mySub.eventId;
                }

                if (!eventId) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada event aktif.", true);
                  return new Response("OK");
                }

                const WRITEUP_KEY = `WRITEUPS_${eventId}`;
                let writeups = {};
                try { const wStr = await env.CTFD_STORE.get(WRITEUP_KEY); if (wStr) writeups = JSON.parse(wStr); } catch (e) { }

                // Try to find key (Stripped vs Raw)
                const keyStripped = chalNameStripped.toLowerCase().replace(/\s+/g, "_");
                const keyRaw = chalName.toLowerCase().replace(/\s+/g, "_");

                let key = null;
                if (writeups[keyStripped]) key = keyStripped;
                else if (writeups[keyRaw]) key = keyRaw;

                if (key) {
                  if (url) {
                    // Delete Specific
                    const initialLen = writeups[key].length;
                    writeups[key] = writeups[key].filter(w => w.url !== url);

                    if (writeups[key].length < initialLen) {
                      // If empty after delete, remove key?
                      if (writeups[key].length === 0) delete writeups[key];

                      await env.CTFD_STORE.put(WRITEUP_KEY, JSON.stringify(writeups));
                      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🗑️ **Writeup Deleted.**\nLink: ${url}`, true);
                    } else {
                      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Writeup tidak ditemukan dengan link tersebut.`, true);
                    }
                  } else {
                    // Delete ALL for this challenge
                    delete writeups[key];
                    await env.CTFD_STORE.put(WRITEUP_KEY, JSON.stringify(writeups));
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🗑️ **All Writeups Deleted** for challenge: ${chalName}`, true);
                  }
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Challenge name tidak ditemukan: ${chalName}`, true);
                }
              } catch (err) {
                console.error("Delete Writeup Error:", err);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${err.message}`, true);
              }

            } else if (text.startsWith("/writeups")) {
              try {
                // Usage: /writeups [event_id]
                const parts = text.trim().split(/\s+/);
                let eventId = parts.length > 1 ? parts[1] : null;

                if (!eventId) {
                  try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                }
                if (!eventId) {
                  let subs = [];
                  try { const s = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (s) subs = JSON.parse(s); } catch (e) { }
                  const mySub = subs.find(s => String(s.userId) === String(userId));
                  if (mySub) eventId = mySub.eventId;
                }

                if (!eventId) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Spesifikasikan ID Event: `/writeups <event_id>`", true);
                  return new Response("OK");
                }

                const WRITEUP_KEY = `WRITEUPS_${eventId}`;
                let writeups = {};
                try { const wStr = await env.CTFD_STORE.get(WRITEUP_KEY); if (wStr) writeups = JSON.parse(wStr); } catch (e) { }

                if (Object.keys(writeups).length === 0) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📚 **Writeups: ${eventId}**\n\nBelum ada writeup tersimpan.`, true);
                  return new Response("OK");
                }

                // Helper to escape HTML special chars
                const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

                let msg = `📚 <b>Writeups: ${escapeHtml(eventId)}</b>\n\n`;
                Object.keys(writeups).forEach(key => {
                  const display = key.split("_").map(w => w.charAt(0).toUpperCase() + w.slice(1)).join(" ");
                  msg += `🛡 <b>${escapeHtml(display)}</b>\n`;
                  writeups[key].forEach(w => {
                    msg += `   • <a href="${escapeHtml(w.url)}">${escapeHtml(w.author)}</a>\n`;
                  });
                  msg += "\n";
                });

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");
              } catch (err) {
                console.error("List Writeups Error:", err);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${err.message}`, true);
              }

            } else if (text.startsWith("/start")) {
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId,
                `👋 **Selamat Datang!**\n\nTerima kasih sudah bergabung di channel CTF.\nSaya adalah bot yang siap membantu anda.\n\nKetik pesan apa saja untuk memulai!`,
                true
              );
            } else if (text.startsWith("/list_events")) {
              let events = [];
              try {
                const stored = await env.CTFD_STORE.get("EVENTS");
                if (stored) events = JSON.parse(stored);
              } catch (e) { }

              // Filter active only
              const activeEvents = events.filter(e => !e.archived);

              if (activeEvents.length === 0) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "📭 Belum ada event yang aktif.", true);
              } else {
                // Use helper to format list with countdown
                // Reuse getStoredEvents logic essentially but we have the array
                // Or just call getStoredEvents?
                // getStoredEvents handles fetching.
                // Let's keep existing logic but enhance it or delegate to getStoredEvents if suitable.
                // Actually existing logic (lines 541-544) is very simple. I should probably use getStoredEvents here for consistency?
                // getStoredEvents (line 3529) handles formatting nicely.
                // Let's use getStoredEvents.
                const msg = await this.getStoredEvents(env, null);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");
              }

            } else if (text.startsWith("/set_event_time")) {
              // Usage: /set_event_time <event_id> <YYYY-MM-DD HH:mm> (WIB usually)
              // Security: Private Chat
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              // parts[0] = /set_event_time
              // parts[1] = event_id
              // parts[2..] = date time

              if (parts.length < 3) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/set_event_time <id> <YYYY-MM-DD HH:mm>`", true);
                return new Response("OK");
              }

              const eventId = parts[1];
              const dateStr = parts.slice(2).join(" ");

              // Validate Date
              const startTime = new Date(dateStr);
              if (isNaN(startTime.getTime())) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Format tanggal tidak valid. Gunakan: YYYY-MM-DD HH:mm", true);
                return new Response("OK");
              }

              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }

              const idx = events.findIndex(e => String(e.id) === String(eventId));
              if (idx === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Event ID tidak ditemukan.", true);
                return new Response("OK");
              }

              events[idx].start = startTime.toISOString();
              await env.CTFD_STORE.put("EVENTS", JSON.stringify(events));

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Waktu Event Diupdate!**\n\nEvent: ${events[idx].name}\nStart: ${dateStr}\n\nCek countdown di \`/list_events\`.`, true);

            } else if (text.startsWith("/init_challenges") || text.startsWith("/refresh_challenges")) {
              // Usage: /init_challenges <event_id> OR /refresh_challenges [event_id]
              // Security: Private Chat
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // Auto-detect Event ID if missing
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  // Try from subs
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === chatId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/init_challenges <event_id>` atau `/refresh_challenges` (jika sudah set event).", true);
                return new Response("OK");
              }

              // 1. Get Event & Credentials (from Subscription)
              let events = [];
              let subs = [];
              try {
                const eS = await env.CTFD_STORE.get("EVENTS");
                const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (eS) events = JSON.parse(eS);
                if (sS) subs = JSON.parse(sS);
              } catch (e) { }

              // Fuzzy Match for init/refresh/update
              const normalize = (str) => (str || "").replace(/_/g, "").toLowerCase();
              const eventIdClean = normalize(eventId);

              const event = events.find(e => normalize(e.id) === eventIdClean);
              if (!event) {
                const availableIds = events.map(e => e.id).join(", ");
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Event ID tidak ditemukan.\nDebug: Input=${eventId}\nAvailable=[${availableIds}]`, true);
                return new Response("OK");
              }

              const mySub = subs.find(s => (s.userId === chatId || s.userId === userId) && normalize(s.eventId) === eventIdClean);
              if (!mySub) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Anda belum join event ini. Gunakan `/join_event` dulu.", true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🚀 **Request Received:** Init ${event.name}\nMemulai proses background...`, true);

              // Offload to background using ctx.waitUntil to prevent Telegram timeout
              ctx.waitUntil(this.processInitChallenges(env, chatId, event, mySub, 0));

              return new Response("OK");

            } else if (text.startsWith("/continue_init") || text.startsWith("/continueinit")) {
              // Usage: /continue_init <event_id> <offset>
              const parts = text.trim().split(/\s+/);
              if (parts.length < 3) return new Response("OK");

              const eventId = parts[1];
              const offset = parseInt(parts[2]);

              // 1. Get Event & Credentials
              let events = [];
              let subs = [];
              try {
                const eS = await env.CTFD_STORE.get("EVENTS");
                const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (eS) events = JSON.parse(eS);
                if (sS) subs = JSON.parse(sS);
              } catch (e) { }

              // Fuzzy Match: Ignore underscores to handle Telegram Markdown swalllowing them
              const normalize = (str) => (str || "").replace(/_/g, "").toLowerCase();
              const eventIdClean = normalize(eventId);

              const event = events.find(e => normalize(e.id) === eventIdClean);
              // Allow finding sub by userId OR chatId (to support groups if user joined privately)
              const mySub = subs.find(s => (s.userId === chatId || s.userId === userId) && normalize(s.eventId) === eventIdClean);

              if (!event || !mySub) {
                const availableIds = events.map(e => e.id).join(", ");
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Invalid Context.\nDebug: EID=${eventId}\nFound: Event=${!!event}, Sub=${!!mySub}\nStore: Count=${events.length}, IDs=[${availableIds}]`, true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🚀 **Resuming Initialization:** ${event.name}\nStart from: ${offset}...`, true);
              ctx.waitUntil(this.processInitChallenges(env, chatId, event, mySub, offset));
              return new Response("OK");
            } else if (text.startsWith("/set_event")) {
              // Usage: /set_event <event_id>
              const parts = text.trim().split(/\s+/);
              if (parts.length < 2) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/set_event <event_id>`", true);
                return new Response("OK");
              }
              const eventId = parts[1];

              // Validate Event Exists
              let events = [];
              try {
                const stored = await env.CTFD_STORE.get("EVENTS");
                if (stored) events = JSON.parse(stored);
              } catch (e) { }

              if (!events.find(e => e.id === eventId)) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Event ID tidak valid. Cek `/list_events`.", true);
                return new Response("OK");
              }

              // Save Preference
              await env.CTFD_STORE.put(`CHAT_PREF_${chatId}`, eventId);

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Default Event Updated!**\n\nEvent aktif untuk chat ini: **${eventId}**.\nSekarang Anda bisa menggunakan \`/challenges\` atau \`/chal\` tanpa mengetik ID event lagi.`, true);

            } else if (text.startsWith("/sync_solves")) {
              // Usage: /sync_solves [event_id]
              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // Auto-detect Event ID
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  // Try from subs
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  // Filter subs where userId matches
                  const mySubs = subs.filter(s => s.userId === userId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/sync_solves <event_id>` (atau set default event dulu).", true);
                return new Response("OK");
              }

              // Load Subscriptions
              let subs = [];
              try {
                const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (sStored) subs = JSON.parse(sStored);
              } catch (e) { }

              const subIndex = subs.findIndex(s => s.userId === userId && s.eventId === eventId);
              if (subIndex === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Anda belum join event ${eventId}. Gunakan /join_event.`, true);
                return new Response("OK");
              }
              const sub = subs[subIndex];

              // Load Event Config for URL
              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }
              const event = events.find(e => e.id === eventId);
              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Data event tidak ditemukan.", true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Syncing solves for **${event.name}**...`, true);

              // Create headers
              const headers = {
                "User-Agent": "TelegramBot/1.0",
                "Content-Type": "application/json"
              };
              if (sub.credentials.mode === 'token') headers["Authorization"] = `Token ${sub.credentials.value}`;
              else headers["Cookie"] = sub.credentials.value;

              // Fetch Solves (Use Team Endpoint usually as it covers user/team scope in CTFd)
              // But if mode is 'users', we might need user endpoint? CTFd standard: /teams/me/solves usually works for effective solves.
              // Let's safe bet: Try teams/me/solves. If 404/error, try users/me/solves.
              let solvesData = [];
              try {
                let res = await fetch(`${event.url}/api/v1/teams/me/solves`, { headers });
                if (!res.ok) {
                  // Fallback to user
                  res = await fetch(`${event.url}/api/v1/users/me/solves`, { headers });
                }

                if (res.ok) {
                  const json = await res.json();
                  if (json.success) solvesData = json.data || [];
                }
              } catch (e) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Fetch error: ${e.message}`, true);
                return new Response("OK");
              }


              // Identify New Solves
              const oldSolves = sub.lastSolves || [];
              const oldSolvedIds = new Set(oldSolves.map(s => s.challenge_id));
              const newSolves = solvesData.filter(s => !oldSolvedIds.has(s.challenge_id));

              // Update Sub
              subs[subIndex].lastSolves = solvesData;
              subs[subIndex].lastCheck = Date.now(); // Mark checked

              await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subs));

              let msg = `✅ **Sync Complete!**\n\nTotal Solved: ${solvesData.length} challenges.\n`;

              if (newSolves.length > 0) {
                msg += `\n🔥 **${newSolves.length} NEW Challenges Solved:**\n`;
                const esc = (t) => String(t || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                newSolves.forEach(s => {
                  const name = s.challenge ? s.challenge.name : "Unknown";
                  const cat = s.challenge ? s.challenge.category : "Misc";
                  const pts = s.challenge ? s.challenge.value : "?";
                  msg += `• <b>${esc(name)}</b> (${esc(cat)} | ${pts})\n`;
                });
              } else {
                msg += `(No new solves detected since last check)`;
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");

            } else if (text.startsWith("/delete_challenges")) {
              // Usage: /delete_challenges [event_id]
              // Security: Private Chat
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // Auto-detect Event ID
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === chatId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/delete_challenges <event_id>` (atau set default event dulu).", true);
                return new Response("OK");
              }

              // Execute
              await env.CTFD_STORE.delete(`CHALLENGES_${eventId}`);

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🗑 **Database Deleted!**\n\nData challenge untuk event **${eventId}** telah dihapus dari cache bot.`, true);

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🗑 **Database Deleted!**\n\nData challenge untuk event **${eventId}** telah dihapus dari cache bot.`, true);

            } else if (text.startsWith("/archive_event")) {
              // Usage: /archive_event <event_id>
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }
              const parts = text.split(" ");
              const eventId = parts[1];
              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/archive_event <event_id>`", true);
                return new Response("OK");
              }

              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }

              const idx = events.findIndex(e => e.id === eventId);
              if (idx === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Event ID \`${eventId}\` tidak ditemukan.`, true);
                return new Response("OK");
              }

              events[idx].archived = true;
              await env.CTFD_STORE.put("EVENTS", JSON.stringify(events));
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📦 **Event Di-Archive!**\n\nEvent \`${events[idx].name}\` dipindahkan ke arsip.\nMonitoring dihentikan untuk event ini.\nGunakan \`/archived_events\` untuk melihat.`, true);

            } else if (text.startsWith("/unarchive_event")) {
              // Usage: /unarchive_event <event_id>
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }
              const parts = text.split(" ");
              const eventId = parts[1];
              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/unarchive_event <event_id>`", true);
                return new Response("OK");
              }

              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }

              const idx = events.findIndex(e => e.id === eventId);
              if (idx === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Event ID \`${eventId}\` tidak ditemukan.`, true);
                return new Response("OK");
              }

              events[idx].archived = false;
              delete events[idx].archived;
              await env.CTFD_STORE.put("EVENTS", JSON.stringify(events));
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📂 **Event Di-Aktifkan!**\n\nEvent \`${events[idx].name}\` kembali aktif.`, true);

            } else if (text === "/archived_events") {
              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }

              const archived = events.filter(e => e.archived);
              if (archived.length === 0) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "📭 Tidak ada event yang di-archive.", true);
              } else {
                let msg = "📦 **Arsip Alur CTF:**\n\n";
                archived.forEach(e => {
                  msg += `🆔 \`${e.id}\`\n📛 **${e.name}**\n🔗 ${e.url}\n\n`;
                });
                msg += "Gunakan `/unarchive_event <id>` untuk mengembalikan.";
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);
              }

            } else if (text.startsWith("/delete_event")) {
              // Usage: /delete_event <event_id>
              // Security: Private Chat
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Command ini hanya via **Private Chat**.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              if (parts.length < 2) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/delete_event <event_id>`", true);
                return new Response("OK");
              }
              const eventId = parts[1];

              // 1. Remove from EVENTS
              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }
              const newEvents = events.filter(e => e.id !== eventId);

              if (events.length === newEvents.length) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Event ID tidak ditemukan.", true);
                return new Response("OK");
              }
              await env.CTFD_STORE.put("EVENTS", JSON.stringify(newEvents));

              // 2. Remove Cache
              await env.CTFD_STORE.delete(`CHALLENGES_${eventId}`);

              // 3. Remove Subscriptions
              let subs = [];
              try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
              const newSubs = subs.filter(s => s.eventId !== eventId);
              await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(newSubs));

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🗑 **Event DELETED!**\n\nEvent **${eventId}** telah dihapus permanen dari sistem bot (termasuk challenge dan subscription).`, true);

            } else if (text.startsWith("/join_event")) {
              // Usage: /join_event <event_id> <token> OR /join_event <event_id> <user> <pass>

              // SECURITY CHECK: Private Chat Only
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Demi keamanan, command ini hanya bisa via **Private Chat (DM)**.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              if (parts.length < 3) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan:\n`/join_event <event_id> <token>`\nATAU\n`/join_event <event_id> <username> <password>`", true);
                return new Response("OK");
              }

              const eventId = parts[1];
              const isTokenMode = parts.length === 3;

              // 1. Find Event
              let events = [];
              try {
                const stored = await env.CTFD_STORE.get("EVENTS");
                if (stored) events = JSON.parse(stored);
              } catch (e) { }

              const event = events.find(e => e.id === eventId);
              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Event ID tidak ditemukan. Cek `/list_events`.", true);
                return new Response("OK");
              }

              let userData = null;
              let authValue = null;
              let authMode = 'token';

              if (isTokenMode) {
                // TOKEN LOGIN
                const token = parts[2];
                authValue = token;
                authMode = 'token';
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Validasi token ke **${event.name}**...`, true);

                try {
                  const testRes = await fetch(`${event.url}/api/v1/users/me`, {
                    headers: {
                      "Authorization": `Token ${token}`,
                      "Content-Type": "application/json",
                      "User-Agent": "TelegramBot/1.0"
                    }
                  });
                  if (testRes.ok) {
                    const json = await testRes.json();
                    if (json.success) userData = json.data;
                  }
                } catch (e) {
                  console.error("Token validation error:", e);
                }

              } else {
                // USER/PASS LOGIN
                const password = parts[parts.length - 1];
                const username = parts.slice(2, parts.length - 1).join(" ").replace(/['"]/g, "");
                authMode = 'auth';

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Login ke **${event.name}** as **${username}**...`, true);

                const loginResult = await this.loginCTFd(event.url, username, password);
                if (loginResult.success) {
                  authValue = loginResult.cookie;
                  // Verify Cookie & Get User Data
                  try {
                    const meRes = await fetch(`${event.url}/api/v1/users/me`, {
                      headers: { "Cookie": authValue, "User-Agent": "TelegramBot/1.0" }
                    }).then(r => r.json());
                    if (meRes.success) userData = meRes.data;
                  } catch (e) { }
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Login Gagal: ${loginResult.error}`, true);
                  return new Response("OK");
                }
              }

              if (!userData) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Gagal memvalidasi user. (Cek token/credentials)`, true);
                return new Response("OK");
              }

              // 3. Init Solves
              let initialSolves = [];
              try {
                const headers = { "Content-Type": "application/json", "User-Agent": "TelegramBot/1.0" };
                if (authMode === 'token') headers["Authorization"] = `Token ${authValue}`;
                else headers["Cookie"] = authValue;

                const sRes = await fetch(`${event.url}/api/v1/teams/me/solves`, { headers });
                if (sRes.ok) {
                  const sJson = await sRes.json();
                  initialSolves = sJson.data || [];
                }
              } catch (e) { }

              // 4. Save Subscription
              let subscriptions = [];
              try {
                const subStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (subStored) subscriptions = JSON.parse(subStored);
              } catch (e) { }

              // Remove existing sub for this user+event if any
              subscriptions = subscriptions.filter(s => !(s.userId === chatId && s.eventId === eventId));

              const newSub = {
                id: "sub_" + Math.random().toString(36).substr(2, 6),
                eventId: event.id,
                userId: chatId,
                userName: userData.name,
                credentials: { mode: authMode, value: authValue },
                lastSolves: initialSolves,
                lastCheck: Date.now()
              };

              subscriptions.push(newSub);
              await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subscriptions));

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Berhasil Join!**\n\n🆔 User: ${userData.name}\n🏆 Event: ${event.name}\n🔐 Mode: ${authMode.toUpperCase()}`, true); // No extra lines
            } else if (text.startsWith("/login_ctf")) {
              // Usage: /login_ctf https://ctf.example.com user pass

              // SECURITY CHECK: Private Chat Only
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Demi keamanan, perintah login hanya bisa dilakukan via **Private Chat (DM)** ke bot ini.\n\nSilakan DM bot untuk login, lalu fitur lain bisa digunakan di grup.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              if (parts.length < 4) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/login_ctf <url> <username> <password>`", true);
              } else {
                let url = parts[1];
                if (url.endsWith("/")) url = url.slice(0, -1);
                if (!url.startsWith("http")) url = "https://" + url;

                const password = parts[parts.length - 1];
                // Join middle parts for username
                const username = parts.slice(2, parts.length - 1).join(" ").replace(/['"]/g, "");

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "🔄 Mencoba login...", true);

                const loginResult = await this.loginCTFd(url, username, password);

                if (loginResult.success) {
                  const config = { url, cookie: loginResult.cookie, chatId, mode: 'auth' };
                  await env.CTFD_STORE.put("MONITOR_CONFIG", JSON.stringify(config));

                  // Fetch initial solves to preventing spamming old ones
                  console.log("Fetching initial solves to sync state...");
                  let initialSolves = [];
                  try {
                    const checkRes = await fetch(`${url}/api/v1/teams/me/solves`, {
                      headers: {
                        "Content-Type": "application/json",
                        "Cookie": loginResult.cookie,
                        "User-Agent": "TelegramBot/1.0"
                      }
                    });
                    if (checkRes.ok) {
                      const data = await checkRes.json();
                      initialSolves = data.data || [];
                    }
                  } catch (e) {
                    console.error("Failed to fetch initial solves:", e);
                  }

                  await env.CTFD_STORE.put("LAST_SOLVES", JSON.stringify(initialSolves));

                  let realName = username;
                  // Update Leaderboard & Get Real Name
                  try {
                    const meRes = await fetch(`${url}/api/v1/users/me`, { headers: { "Cookie": loginResult.cookie } }).then(r => r.json());
                    if (meRes.success) {
                      realName = meRes.data.name;
                      await this.updateLeaderboard(env, userId, telegramName, meRes.data.name, meRes.data.score);
                    }
                  } catch (e) { }

                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Login Berhasil!**\n👤 Akun: **${realName}**\n\n💡 _Tips: Mau notifikasi muncul di Grup? Invite bot ke grup, lalu ketik_ \`/set_notify\` _di sana._`, true);
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ **Login Gagal!**\n\nError: ${loginResult.error}`, true);
                }
              }
            } else if (text.startsWith("/login_token")) {
              // Usage: /login_token https://ctf.example.com <token>

              // SECURITY CHECK: Private Chat Only
              if (chatType !== 'private') {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Demi keamanan, perintah login hanya bisa dilakukan via **Private Chat (DM)** ke bot ini.\n\nSilakan DM bot untuk login, lalu fitur lain bisa digunakan di grup.", true);
                return new Response("OK");
              }

              const parts = text.trim().split(/\s+/);
              if (parts.length !== 3) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/login_token <url> <token>`", true);
              } else {
                let url = parts[1];
                if (url.endsWith("/")) url = url.slice(0, -1);
                if (!url.startsWith("http")) url = "https://" + url;
                const token = parts[2];

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "🔄 Memvalidasi token...", true);

                // Validate token by testing API call
                try {
                  const testRes = await fetch(`${url}/api/v1/users/me`, {
                    headers: {
                      "Authorization": `Token ${token}`,
                      "Content-Type": "application/json",
                      "User-Agent": "TelegramBot/1.0"
                    }
                  });

                  if (testRes.ok) {
                    const userData = await testRes.json();
                    const userName = userData.data ? userData.data.name : "Unknown";

                    // Fetch initial solves to prevent spam
                    console.log("Fetching initial solves to sync state...");
                    let initialSolves = [];
                    try {
                      const checkRes = await fetch(`${url}/api/v1/teams/me/solves`, {
                        headers: {
                          "Authorization": `Token ${token}`,
                          "Content-Type": "application/json",
                          "User-Agent": "TelegramBot/1.0"
                        }
                      });
                      if (checkRes.ok) {
                        const data = await checkRes.json();
                        initialSolves = data.data || [];
                      }
                    } catch (e) {
                      console.error("Failed to fetch initial solves:", e);
                    }

                    const config = { url, token, chatId, mode: 'token' };
                    await env.CTFD_STORE.put("MONITOR_CONFIG", JSON.stringify(config));
                    await env.CTFD_STORE.put("LAST_SOLVES", JSON.stringify(initialSolves));

                    // Update Leaderboard
                    if (userData.data) {
                      await this.updateLeaderboard(env, userId, telegramName, userData.data.name, userData.data.score);
                      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Login Berhasil!**\n👤 Akun: **${userData.data.name}**\n\n💡 _Tips: Mau notifikasi muncul di Grup? Invite bot ke grup, lalu ketik_ \`/set_notify\` _di sana._`, true);
                    } else {
                      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Login Berhasil!**\n👤 Akun: **Unknown**`, true);
                    }

                  } else {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ **Token Tidak Valid!**\n\nStatus: ${testRes.status}\nPastikan token Anda benar dan masih aktif.`, true);
                  }
                } catch (e) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ **Gagal Validasi Token!**\n\nError: ${e.message}`, true);
                }
              }
            } else if (text.startsWith("/register_team")) {
              // Usage: /register_team <url> <name> <email> <pass>
              // Handle spaces in Team Name: 
              // logic: URL is first, Password is last, Email is second to last. Everything in between is Team Name.
              const args = text.trim().split(/\s+/);
              // args[0] = /register_team

              // Robust Parsing Strategy:
              // 1. URL is always args[1]
              // 2. Email is the anchor (contains "@")
              // 3. Team Name is everything between URL and Email
              // 4. Password is immediately after Email
              // 5. Custom Fields are everything after Password

              let emailIdx = -1;
              for (let i = 2; i < args.length; i++) {
                if (args[i].includes("@")) {
                  emailIdx = i;
                  break;
                }
              }

              if (args.length < 5 || emailIdx === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah atau Email tidak ditemukan.\nGunakan: `/register_team <url> <nama_tim> <email> <password> [fields=..]`", true);
              } else {
                let url = args[1];
                if (url.endsWith("/")) url = url.slice(0, -1);
                if (!url.startsWith("http")) url = "https://" + url;

                const email = args[emailIdx];
                const teamName = args.slice(2, emailIdx).join(" ").replace(/['"]/g, "");
                const password = args[emailIdx + 1];

                // Parse Custom Fields (key=value)
                const customData = {};
                const customArgs = args.slice(emailIdx + 2);
                customArgs.forEach(arg => {
                  const parts = arg.split("=");
                  if (parts.length === 2) {
                    customData[parts[0]] = parts[1];
                  }
                });

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📝 Mendaftarkan tim: **${teamName}**\nURL: ${url}\nCustom Fields: ${JSON.stringify(customData)}`, true);

                const regResult = await this.registerCTFd(url, teamName, email, password, customData, env, chatId);

                if (regResult.success) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Registrasi Berhasil!**\n\nTim: ${teamName}\nEmail: ${email}\n\nSilakan cek email untuk verifikasi (jika ada).`, true);
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ **Registrasi Gagal!**\n\nError:\n\`\`\`\n${regResult.error}\n\`\`\``, true);
                }
              }
            } else if (text.startsWith("/create_team")) {
              // Command: /create_team <team_name> <password> [fields=...]
              // Requires Auth
              const configStr = await env.CTFD_STORE.get("MONITOR_CONFIG");
              if (!configStr) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Anda belum login. Gunakan `/login_ctf` terlebih dahulu.", true);
                return new Response("OK");
              }
              const config = JSON.parse(configStr);
              if (config.mode !== 'auth' || !config.cookie) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Sesi login tidak valid. Silakan login ulang.", true);
                return new Response("OK");
              }

              // Parse Args
              // /create_team "My Team" pass123 fields2=val
              const args = text.trim().split(/\s+/);
              if (args.length < 3) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/create_team <nama_tim> <password> [fields..]`", true);
                return new Response("OK");
              }

              // Heuristic Parsing:
              // 1. Team name starts at index 1.
              // 2. We need to find where Team Name ends and Password begins.
              // Assumption: Password is the LAST argument (before custom fields). 
              // But Custom fields create ambiguity if present.
              // Let's use Quote detection for Team Name.

              let passwordIdx = -1;
              // If user uses quotes: /create_team "Team Name" pass
              // We can rejoin and re-split? Or just iterate.

              // Simple strategy:
              // If args have custom fields (containing "="), they are at the end.
              // The item BEFORE the first custom field is the password.
              // Everything BEFORE that is Team Name.

              let firstCustomIdx = -1;
              for (let i = 1; i < args.length; i++) {
                if (args[i].includes("=") && i > 1) { // i>1 to avoid treating team name as custom field if it has =? Unlikely.
                  firstCustomIdx = i;
                  break;
                }
              }

              let password = "";
              let teamName = "";
              let customData = {};

              if (firstCustomIdx !== -1) {
                passwordIdx = firstCustomIdx - 1;
                const customArgs = args.slice(firstCustomIdx);
                customArgs.forEach(arg => {
                  const [k, v] = arg.split("=");
                  if (k && v) customData[k] = v;
                });
              } else {
                passwordIdx = args.length - 1;
              }

              if (passwordIdx < 1) {
                // Logic fail?
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah. Pastikan ada password.", true);
                return new Response("OK");
              }

              password = args[passwordIdx];
              teamName = args.slice(1, passwordIdx).join(" ").replace(/['"]/g, "");

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔨 Membuat Tim: **${teamName}**\nTarget: ${config.url}`, true);

              const createResult = await this.createTeamCTFd(config.url, teamName, password, customData, config.cookie, env, chatId);

              if (createResult.success) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Tim Berhasil Dibuat!**\n\nNama: ${teamName}\nPassword: ${password}`, true);
              } else {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ **Gagal Membuat Tim.**\n\nError: ${createResult.error}`, true);
              }
            } else if (text.startsWith("/profile")) {
              // Usage: /profile [event_id]
              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // 1. Auto-detect Event ID
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  // Try from subs (User context)
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === userId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/profile [event_id]` (atau set default event dulu).", true);
                return new Response("OK");
              }

              // 2. Load Subscription & Event
              let subs = [];
              try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }

              const sub = subs.find(s => s.userId === userId && s.eventId === eventId);
              if (!sub) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Anda belum join event ${eventId}. Gunakan /join_event.`, true);
                return new Response("OK");
              }

              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }
              const event = events.find(e => e.id === eventId);
              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Data event tidak ditemukan.", true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Mengambil data profile di **${event.name}**...`, true);

              try {
                const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                if (sub.credentials.mode === 'token') headers["Authorization"] = `Token ${sub.credentials.value}`;
                else headers["Cookie"] = sub.credentials.value;

                const resMe = await fetch(`${event.url}/api/v1/users/me`, { headers }).then(r => r.json()).catch(e => null);

                let msg = "";
                if (resMe && resMe.success && resMe.data) {
                  const u = resMe.data;
                  msg += `👤 **Profile Akun**\n`;
                  msg += `Nama: **${u.name}**\n`;
                  msg += `Email: ${u.email || "-"}\n`;
                  msg += `ID: ${u.id}\n`;
                  msg += `🌐 Event: ${event.name}`;

                  // Fetch solve count for leaderboard
                  let solveCount = 0;
                  try {
                    const solvesRes = await fetch(`${event.url}/api/v1/users/me/solves`, { headers });
                    if (solvesRes.ok) {
                      const solvesJson = await solvesRes.json();
                      if (solvesJson.success && solvesJson.data) {
                        solveCount = solvesJson.data.length;
                      }
                    }
                  } catch (e) {
                    console.error("Failed to fetch solve count:", e);
                  }

                  // Update Leaderboard with score and solve count
                  await this.updateLeaderboard(env, eventId, userId, telegramName, u.name, u.score, solveCount);

                } else {
                  msg += `❌ Gagal mengambil profile. Sesi mungkin kadaluarsa.`;
                }

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);

              } catch (e) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
              }

            } else if (text.startsWith("/team")) {
              // Usage: /team [event_id]
              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // 1. Auto-detect Event ID
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  // Try from subs (User context)
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === userId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/team [event_id]` (atau set default event dulu).", true);
                return new Response("OK");
              }

              // 2. Load Subscription & Event
              let subs = [];
              try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }

              // Try finding sub for THIS user first
              let sub = subs.find(s => s.userId === userId && s.eventId === eventId);

              // If not found, try finding ANY sub for this event (Shared Context in Group)
              if (!sub) {
                sub = subs.find(s => s.eventId === eventId);
              }

              if (!sub) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Belum ada data sesi untuk event ${eventId}. Admin perlu /join_event dulu.`, true);
                return new Response("OK");
              }

              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }
              const event = events.find(e => e.id === eventId);
              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Data event tidak ditemukan.", true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Mengambil data team di **${event.name}**...`, true);

              // Offload heavy fetch to background to prevent timeout
              ctx.waitUntil((async () => {
                try {
                  const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                  if (sub.credentials.mode === 'token') headers["Authorization"] = `Token ${sub.credentials.value}`;
                  else headers["Cookie"] = sub.credentials.value;

                  const resTeam = await fetch(`${event.url}/api/v1/teams/me`, { headers }).then(r => r.json()).catch(e => null);

                  let msg = "";
                  // Team Info
                  if (resTeam && resTeam.success && resTeam.data) {
                    const t = resTeam.data;
                    if (t) {
                      msg += `🛡 **Informasi Tim**\n`;
                      msg += `Nama: **${t.name}**\n`;
                      msg += `ID: ${t.id}\n`;
                      msg += `🏆 Rank: ${t.place || "Unranked"}\n`;
                      msg += `💎 Score: ${t.score}\n`;
                      msg += `🌐 Event: ${event.name}\n`;

                      if (t.members && t.members.length > 0) {
                        msg += `\n👥 **Anggota (${t.members.length}):**\n`;

                        const memberPromises = t.members.map(async (m) => {
                          let memberId = (typeof m === 'object') ? (m.id || m.user_id) : m;
                          let memberName = (typeof m === 'object') ? (m.name || m.username) : null;
                          let memberScore = (typeof m === 'object') ? m.score : null;

                          // If we don't have name or score, we fetch it
                          if (!memberName || memberScore === null || memberScore === undefined) {
                            try {
                              const userRes = await fetch(`${event.url}/api/v1/users/${memberId}`, { headers })
                                .then(r => r.json())
                                .catch(() => null);
                              if (userRes && userRes.success && userRes.data) {
                                memberName = userRes.data.name;
                                memberScore = userRes.data.score;
                              } else {
                                memberName = memberName || "Unknown User";
                                memberScore = memberScore !== null ? memberScore : "?";
                              }
                            } catch (e) {
                              memberName = memberName || "Error Fetching";
                              memberScore = "?";
                            }
                          }
                          return `${memberName} (ID: ${memberId}) - 💎 ${memberScore}`;
                        });

                        const memberList = await Promise.all(memberPromises);
                        memberList.forEach((line, i) => {
                          msg += `${i + 1}. ${line}\n`;
                        });
                      }
                    } else {
                      msg += `⚠️ Data tim kosong (Mungkin mode User Mode atau belum join tim).`;
                    }
                  } else {
                    msg += `⚠️ Kamu belum bergabung dengan tim manapun di event ini.`;
                  }
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);

                } catch (e) {
                  console.error("Team Command Error:", e);
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
                }
              })());

              return new Response("OK");
            } else if (text.startsWith("/ctf")) {
              // CTFTime API (Global)
              const args = text.trim().split(/\s+/);
              const filter = args.length > 1 ? args[1].toLowerCase() : null;
              const events = await this.getCTFTimeEvents(filter);
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, events, "HTML");
            } else if (text.startsWith("/list_events")) {
              // Internal DB (Manual)
              const args = text.trim().split(/\s+/);
              const filter = args.length > 1 ? args[1].toLowerCase() : null;
              const events = await this.getStoredEvents(env, filter);
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, events, "HTML");
            } else if (text.startsWith("/broadcast_challenges")) {
              // Usage: /broadcast_challenges <event_id>
              // Wrap in waitUntil to prevent Telegram timeouts/retries
              ctx.waitUntil((async () => {
                const args = text.trim().split(/\s+/);
                if (args.length < 2) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/broadcast_challenges <event_id>`", true);
                  return;
                }

                const eventId = args[1];
                const targetChannel = "@CTF_Channel";

                try {
                  // 1. Load event info
                  let events = [];
                  try {
                    const eStored = await env.CTFD_STORE.get("EVENTS");
                    if (eStored) events = JSON.parse(eStored);
                  } catch (e) { }

                  const event = events.find(e => e.id === eventId);
                  if (!event) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Event ${eventId} tidak ditemukan.`, true);
                    return;
                  }

                  // 2. Load challenges from cache
                  const cacheKey = `CHALLENGES_${eventId}`;
                  const cached = await env.CTFD_STORE.get(cacheKey);
                  if (!cached) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Cache challenges untuk event ${eventId} tidak ditemukan.\nJalankan \`/init_challenges ${eventId}\` terlebih dahulu.`, true);
                    return;
                  }

                  const challenges = JSON.parse(cached);
                  if (challenges.length === 0) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Tidak ada challenges dalam cache.", true);
                    return;
                  }

                  // 3. Group challenges by category
                  const byCategory = {};
                  challenges.forEach(c => {
                    const cat = c.category || "Uncategorized";
                    if (!byCategory[cat]) byCategory[cat] = [];
                    byCategory[cat].push(c);
                  });

                  // 4. Build message with inline buttons
                  const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
                  let message = `🎯 <b>${escapeHtml(event.name)} - Challenges</b>\n\n`;

                  const buttons = [];
                  let totalCount = 0;
                  let currentRow = [];

                  // Sort categories alphabetically
                  const sortedCategories = Object.keys(byCategory).sort();

                  sortedCategories.forEach(category => {
                    const challs = byCategory[category];
                    message += `📁 <b>${escapeHtml(category)}</b> (${challs.length})\n`;

                    challs.forEach(c => {
                      totalCount++;
                      const chalName = escapeHtml(c.name);
                      message += `• ${chalName}\n`;

                      // Create deep link button
                      const deepLink = `https://t.me/Flintz_VerifBot?start=chal_${eventId}_${c.id}`;
                      currentRow.push({
                        text: `📖 ${c.name.length > 20 ? c.name.substring(0, 20) + '...' : c.name}`,
                        url: deepLink
                      });

                      // Add row when we have 2 buttons
                      if (currentRow.length === 2) {
                        buttons.push([...currentRow]);
                        currentRow = [];
                      }
                    });

                    message += `\n`;
                  });

                  // Add remaining button if odd number
                  if (currentRow.length > 0) {
                    buttons.push(currentRow);
                  }

                  message += `\n<b>Total: ${totalCount} challenges</b>`;

                  // 5. Send to channel
                  await this.sendMessageWithButtons(env.TELEGRAM_BOT_TOKEN, targetChannel, message, buttons, "HTML");
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ Berhasil broadcast ${totalCount} challenges ke ${targetChannel}!`, true);

                } catch (e) {
                  console.error("Broadcast Challenges Error:", e);
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
                }
              })());

              return new Response("OK");
            } else if (text.startsWith("/challenges")) {
              // Usage: /challenges [filter] [event_id]
              const args = text.trim().split(/\s+/);
              const filterMode = (args[1] && ["all", "solved", "unsolved", "summary"].includes(args[1].toLowerCase())) ? args[1].toLowerCase() : "summary";

              // Event ID might be 2nd arg (if filter is default) or 3rd arg
              let inputEventId = null;
              if (args.length === 2 && !["all", "solved", "unsolved", "summary"].includes(args[1].toLowerCase())) {
                inputEventId = args[1];
              } else if (args.length >= 3) {
                inputEventId = args[2];
              }

              let eventId = inputEventId;
              let useCache = false;
              let cachedData = null;

              // 1. Determine Event ID logic
              // Check Chat Pref First
              if (!eventId) {
                try {
                  eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`);
                } catch (e) { }
              }

              if (chatType !== 'private') {
                if (!eventId) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Di Group Chat, wajib set event default dulu `/set_event <id>` atau sertakan ID di command.", true);
                  return new Response("OK");
                }
              } else {
                // Private: If still null, try manual auto-detect from subs
                if (!eventId) {
                  let subs = [];
                  try {
                    const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                    if (sStored) subs = JSON.parse(sStored);
                  } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === chatId);

                  if (mySubs.length === 1) {
                    eventId = mySubs[0].eventId;
                  } else if (mySubs.length > 1) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Anda join banyak event. Set default: `/set_event <id>` atau sertakan ID.", true);
                    return new Response("OK");
                  }
                }
              }

              // Enforce Event Selection
              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Silakan pilih event terlebih dahulu.\nGunakan: `/set_event <id_event>` atau `/join_event`.", true);
                return new Response("OK");
              }

              // 2. Try Fetch from Cache
              if (eventId) {
                try {
                  const cStored = await env.CTFD_STORE.get(`CHALLENGES_${eventId}`);
                  if (cStored) {
                    cachedData = JSON.parse(cStored);
                    useCache = true;
                  }
                } catch (e) { }
              }

              // 3. Fallback to Legacy Live Fetch (Only in Private, if no cache/eventId)
              let allChallenges = [];
              let solvedData = [];
              let fromSource = "Cache";

              if (useCache && cachedData) {
                allChallenges = cachedData;
                fromSource = "Database (Offline)";
                // We need 'solved' status.
                // In Cache mode, we might not have live 'solved' status dependent on user? 
                // Cache only stores Challenges. Solves are user-specific.
                // We need to fetch User Solves if possible, OR just show Chall list without solved status if in group?
                // Better: Try to fetch personal solves if private. If group, maybe no solved status?
                // For now: Just show list. Or try to fetch solves from SUBSCRIPTION if available.

                if (eventId) {
                  // Try get solves from sub
                  let subs = [];
                  try {
                    const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                    if (sStored) subs = JSON.parse(sStored);
                  } catch (e) { }
                  // Look for stats for the SENDER (userId), not the CHAT (chatId) which might be a group
                  const sub = subs.find(s => s.userId === userId && s.eventId === eventId);
                  if (sub && sub.lastSolves) {
                    solvedData = sub.lastSolves;
                  }
                }

              } else {
                // LEGACY LIVE FETCH (Private Only)
                const configStr = await env.CTFD_STORE.get("MONITOR_CONFIG");
                if (!configStr) {
                  if (eventId) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Database challenge untuk ${eventId} belum di-init. Gunakan \`/init_challenges ${eventId}\` dulu.`, true);
                  } else {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Belum login. Gunakan `/join_event` atau `/login_ctf`.", true);
                  }
                  return new Response("OK");
                }

                // If we are here, we are likely using old single-user mode or fallback
                const config = JSON.parse(configStr);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "🔄 Mengambil data chall (Live)...", true);
                fromSource = "Live API";

                try {
                  // Setup headers
                  const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                  if (config.mode === 'token' && config.token) {
                    headers["Authorization"] = `Token ${config.token}`;
                  } else if (config.mode === 'auth' && config.cookie) {
                    headers["Cookie"] = config.cookie;
                  }

                  // Parallel Fetch: Challenges & Solves
                  const [challRes, solveRes] = await Promise.all([
                    fetch(`${config.url}/api/v1/challenges`, { headers }).then(r => r.json()).catch(e => ({ success: false, error: e.message })),
                    fetch(config.mode === 'token' ? `${config.url}/api/v1/users/me/solves` : `${config.url}/api/v1/teams/me/solves`, { headers }).then(r => r.json()).catch(e => ({ success: false, error: e.message }))
                  ]);

                  if (!challRes.success || !challRes.data) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Gagal mengambil challenges: ${challRes.error || "Unknown Error"}`, true);
                    return new Response("OK");
                  }

                  allChallenges = challRes.data;
                  solvedData = (solveRes.success && solveRes.data) ? solveRes.data : [];

                } catch (e) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
                  return new Response("OK");
                }
              }

              // Set of Solved Challenge IDs
              const solvedIds = new Set(solvedData.map(s => s.challenge_id));

              // Process & Filter
              let displayList = [];
              const summaryStats = { total: allChallenges.length, solved: solvedIds.size, unsolved: allChallenges.length - solvedIds.size };

              if (filterMode === 'summary') {
                // Just show stats
                const msg = `📊 **Challenge Stats**\n\n` +
                  `🏆 **Total:** ${summaryStats.total}\n` +
                  `✅ **Solved:** ${summaryStats.solved}\n` +
                  `⬜ **Unsolved:** ${summaryStats.unsolved}\n\n` +
                  `Gunakan:\n` +
                  `/challenges all [id]\n` +
                  `/challenges solved [id]\n` +
                  `/challenges unsolved [id]`;
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);
                return new Response("OK");
              }

              allChallenges.forEach(ch => {
                const isSolved = solvedIds.has(ch.id);
                const statusIcon = isSolved ? "✅" : "⬜";

                if (filterMode === 'solved' && !isSolved) return;
                if (filterMode === 'unsolved' && isSolved) return;

                displayList.push({
                  category: ch.category,
                  name: ch.name,
                  value: ch.value,
                  icon: statusIcon
                });
              });

              if (displayList.length === 0) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📂 Tidak ada challenge untuk filter: **${filterMode}**`, true);
                return new Response("OK");
              }

              // Group by Category
              const grouped = {};
              displayList.forEach(item => {
                if (!grouped[item.category]) grouped[item.category] = [];
                grouped[item.category].push(item);
              });

              // Helper to escape Markdown special characters (reused logic)
              const escapeCtfMarkdown = (str) => {
                if (!str) return "";
                return str.toString()
                  .replace(/_/g, "\\_")
                  .replace(/\*/g, "\\*")
                  .replace(/\[/g, "\\[")
                  .replace(/`/g, "\\`");
              };

              // Build Message (Chunking to avoid 4096 limit)
              let messageChunks = [""];
              let currentChunkIndex = 0;

              const titleHeader = `📊 **Challenge List**\n`;
              messageChunks[0] += titleHeader;

              for (const cat of Object.keys(grouped).sort()) {
                let catHeader = `\n📂 **${escapeCtfMarkdown(cat)}**\n`;

                // Check chunk size for header
                if (messageChunks[currentChunkIndex].length + catHeader.length > 4000) {
                  currentChunkIndex++;
                  messageChunks[currentChunkIndex] = "";
                }
                messageChunks[currentChunkIndex] += catHeader;

                for (const item of grouped[cat]) {
                  const line = `${item.icon} [${item.value}] ${escapeCtfMarkdown(item.name)}\n`;
                  if (messageChunks[currentChunkIndex].length + line.length > 4000) {
                    currentChunkIndex++;
                    messageChunks[currentChunkIndex] = "";
                  }
                  messageChunks[currentChunkIndex] += line;
                }
              }

              // Send Chunks
              for (const chunk of messageChunks) {
                if (chunk.trim()) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, chunk, true);
                }
              }
            } else if (text.startsWith("/chal")) {
              // Usage: /chal <name_or_id> [event_id]
              const args = text.trim().split(/\s+/);
              if (args.length < 2) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Gunakan: `/chal <id_atau_nama> [event_id]`\nContoh: `/chal basic` atau `/chal basic evt_123`", true);
                return new Response("OK");
              }

              // Parse Query & Event ID
              // args[0] = /chal
              // args[1] = query
              // args[2] = event_id (optional)

              const query = args[1];
              let inputEventId = args.length >= 3 ? args[2] : null;
              let eventId = inputEventId;
              let cachedData = null;

              // 1. Determine Event ID logic
              // Check Chat Pref First
              if (!eventId) {
                try {
                  eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`);
                } catch (e) { }
              }

              if (chatType !== 'private') {
                if (!eventId) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Di Group Chat, wajib set event default `/set_event <id>` atau sertakan ID.", true);
                  return new Response("OK");
                }
              } else {
                // Private: Try to infer if not provided
                if (!eventId) {
                  let subs = [];
                  try {
                    const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                    if (sStored) subs = JSON.parse(sStored);
                  } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === chatId);

                  if (mySubs.length === 1) {
                    eventId = mySubs[0].eventId;
                  }
                  // If multiple, fallback to null -> error or legacy
                }
              }

              // Enforce Event Selection
              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Silakan pilih event terlebih dahulu.\nGunakan: `/set_event <id_event>` atau `/join_event`.", true);
                return new Response("OK");
              }

              // 2. Try Fetch from Cache (if eventId known)
              if (eventId) {
                try {
                  const cStored = await env.CTFD_STORE.get(`CHALLENGES_${eventId}`);
                  if (cStored) {
                    cachedData = JSON.parse(cStored);
                  }
                } catch (e) { }
              }

              let chal = null;
              let fromSource = "Cache";

              // 3. Search Logic
              if (cachedData) {
                // SEARCH IN CACHE
                fromSource = "Database (Offline)";
                if (/^\d+$/.test(query)) {
                  // ID Match
                  chal = cachedData.find(c => c.id == query);
                } else {
                  // Name Fuzzy Match
                  chal = cachedData.find(c => c.name.toLowerCase().includes(query.toLowerCase()));
                }

                if (!chal) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Challenge "${query}" tidak ditemukan di database event ${eventId}.`, true);
                  return new Response("OK");
                }

              } else {
                // FALLBACK: LIVE FETCH (Private Only)
                if (chatType !== 'private') {
                  // Should have caught above, but safety check
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Data event belum di-init. Gunakan `/init_challenges` di PM.", true);
                  return new Response("OK");
                }

                const configStr = await env.CTFD_STORE.get("MONITOR_CONFIG");
                if (!configStr) {
                  if (eventId) await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Database challenge untuk ${eventId} belum di-init.`, true);
                  else await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Belum login. Gunakan `/join_event`.", true);
                  return new Response("OK");
                }

                const config = JSON.parse(configStr);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔍 Mencari "${query}" (Live)...`, true);
                fromSource = "Live API";

                try {
                  const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                  if (config.mode === 'token' && config.token) headers["Authorization"] = `Token ${config.token}`;
                  else if (config.mode === 'auth' && config.cookie) headers["Cookie"] = config.cookie;

                  let challengeId = null;
                  if (/^\d+$/.test(query)) {
                    challengeId = query;
                  } else {
                    const listRes = await fetch(`${config.url}/api/v1/challenges?view=user`, { headers });
                    if (!listRes.ok) throw new Error("List fetch failed");
                    const listJson = await listRes.json();
                    if (!listJson.success) throw new Error("API Error");

                    const match = (listJson.data || []).find(c => c.name.toLowerCase().includes(query.toLowerCase()));
                    if (match) challengeId = match.id;
                  }

                  if (!challengeId) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Challenge "${query}" tidak ditemukan.`, true);
                    return new Response("OK");
                  }

                  // Fetch Detail
                  const detailRes = await fetch(`${config.url}/api/v1/challenges/${challengeId}`, { headers });
                  if (!detailRes.ok) throw new Error("Detail fetch failed");
                  const detailJson = await detailRes.json();
                  chal = detailJson.data;

                } catch (e) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
                  return new Response("OK");
                }
              }

              if (chal) {
                // 4. DISPLAY
                const escapeHtml = (text) => {
                  if (!text) return "";
                  return String(text).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
                };

                let desc = chal.description || "Tidak ada deskripsi.";
                // Simple HTML Tag cleanup for Telegram
                desc = desc
                  .replace(/<br\s*\/?>/gi, "\n")
                  .replace(/<\/p>/gi, "\n\n")
                  .replace(/<(?!\/?(b|i|u|s|code|pre|a))[^>]+>/g, ""); // Strip non-allowed tags

                let filesMsg = "";
                if (chal.files && chal.files.length > 0) {
                  filesMsg = "\n📂 <b>Files:</b>\n";

                  // Fix file URL handling
                  let baseUrl = "";
                  if (eventId) {
                    try {
                      const eStored = await env.CTFD_STORE.get("EVENTS");
                      if (eStored) {
                        const allEvents = JSON.parse(eStored);
                        const ev = allEvents.find(e => e.id === eventId);
                        if (ev) baseUrl = ev.url;
                      }
                    } catch (e) { }
                  } else {
                    try {
                      const cStr = await env.CTFD_STORE.get("MONITOR_CONFIG");
                      if (cStr) baseUrl = JSON.parse(cStr).url;
                    } catch (e) { }
                  }

                  chal.files.forEach(f => {
                    const fileStr = (typeof f === 'string') ? f : f.url; // Handle if object or string
                    const fullUrl = fileStr.startsWith("http") ? fileStr : `${baseUrl}${fileStr}`;
                    const fileName = fileStr.split('/').pop().split('?')[0];
                    filesMsg += `• <a href="${fullUrl}">${escapeHtml(fileName)}</a>\n`;
                  });
                }

                // Check Solved Status (from Subscription Cache)
                let solvedBy = null;
                try {
                  const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                  if (sStored) {
                    const subs = JSON.parse(sStored);
                    // Find subscription
                    const sub = subs.find(s => s.userId === chatId && s.eventId === eventId);
                    if (sub && sub.lastSolves) {
                      const solvedEntry = sub.lastSolves.find(s => s.challenge_id === chal.id);
                      if (solvedEntry) {
                        if (solvedEntry.user && solvedEntry.user.name) solvedBy = solvedEntry.user.name;
                        else if (solvedEntry.username) solvedBy = solvedEntry.username;
                        else solvedBy = "Team / You";
                      }
                    }
                  }
                } catch (e) { }

                let solvedMsg = "";
                if (solvedBy) {
                  solvedMsg = `\n✅ <b>SOLVED</b> by <b>${escapeHtml(solvedBy)}</b>\n`;
                }

                const msg = `🛡 <b>${escapeHtml(chal.name)}</b>\n` +
                  `📂 ${escapeHtml(chal.category)} | 💎 ${chal.value} pts | ${chal.solves || 0} Solves\n` +
                  solvedMsg +
                  `\n📝 <b>Deskripsi:</b>\n${desc}\n` +
                  filesMsg +
                  `\n👉 <code>/chal ${chal.id} ${eventId || ""}</code>`;

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML");
              } else {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Data challenge kosong.", true);
              }

            } else if (text.startsWith("/scoreboard")) {
              // Scoreboard Command
              const args = text.trim().split(/\s+/);
              // args[0] = /scoreboard
              let limit = 10;
              let inputEventId = null;

              if (args.length >= 2) {
                const parsed = parseInt(args[1]);
                if (!isNaN(parsed)) {
                  limit = Math.min(Math.max(parsed, 1), 25);
                  if (args.length >= 3) inputEventId = args[2];
                } else {
                  inputEventId = args[1];
                }
              }

              let eventId = inputEventId;

              // 1. Auto-detect Event ID
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  let subs = [];
                  try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                  const mySubs = subs.filter(s => s.userId === userId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              if (!eventId) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/scoreboard [limit] [event_id]` (atau set default event).", true);
                return new Response("OK");
              }

              // 2. Get Event Info
              let events = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) events = JSON.parse(eS); } catch (e) { }
              const event = events.find(e => e.id === eventId);
              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Data event tidak ditemukan.", true);
                return new Response("OK");
              }

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🏆 Fetching Scoreboard: **${event.name}**...`, true);

              try {
                // 3. Determine Identity (for highlighting) form Subscriptions
                let myTeamId = null;
                let subs = [];
                try { const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS"); if (sS) subs = JSON.parse(sS); } catch (e) { }
                const sub = subs.find(s => s.userId === userId && s.eventId === eventId);

                const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                // We use public scoreboard usually, but if we have creds, we can check "Me"

                if (sub) {
                  if (sub.credentials.mode === 'token') headers["Authorization"] = `Token ${sub.credentials.value}`;
                  else headers["Cookie"] = sub.credentials.value;

                  // Try to get my team ID
                  // Optimization: We might have stored it in sub? No, we stored name.
                  // Let's fetch quickly.
                  try {
                    const meT = await fetch(`${event.url}/api/v1/teams/me`, { headers }).then(r => r.json());
                    if (meT && meT.data) myTeamId = meT.data.id;
                    else {
                      // Maybe user mode?
                      const meU = await fetch(`${event.url}/api/v1/users/me`, { headers }).then(r => r.json());
                      if (meU && meU.data) myTeamId = meU.data.id; // User ID acting as Team ID in User Mode
                    }
                  } catch (e) { }
                }

                // 4. Fetch Scoreboard
                // Attempt public fetch first (no creds needed usually)
                // Remove auth headers for scoreboard to avoid permission issues if token specific? 
                // CTFd public scoreboard is ... public. 
                // But if private CTF, need auth. Better use auth if available.

                const sbRes = await fetch(`${event.url}/api/v1/scoreboard`, { headers });

                if (!sbRes.ok) {
                  if (sbRes.status === 403 || sbRes.status === 401) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Scoreboard Private/Hidden. (Pastikan Anda sudah join/login)`, true);
                  } else {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Gagal mengambil scoreboard. Status: ${sbRes.status}`, true);
                  }
                  return new Response("OK");
                }

                const sbJson = await sbRes.json();
                const list = sbJson.data;

                if (!list || !Array.isArray(list)) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "❌ Format data scoreboard tidak dikenali/kosong.", true);
                  return new Response("OK");
                }

                const escapeCtfMarkdown = (str) => {
                  if (!str) return "";
                  return str.toString().replace(/_/g, "\\_").replace(/\*/g, "\\*").replace(/\[/g, "\\[").replace(/`/g, "\\`");
                };

                let msg = `🏆 **Scoreboard: ${event.name}**\nTop ${Math.min(limit, list.length)}\n\n`;
                let foundMe = false;

                for (let i = 0; i < list.length; i++) {
                  const item = list[i];
                  const rank = i + 1;
                  if (rank > limit) break;

                  let icon = "▪️";
                  if (rank === 1) icon = "🥇";
                  else if (rank === 2) icon = "🥈";
                  else if (rank === 3) icon = "🥉";

                  // In CTFd Scoreboard, item has account_id usually
                  const isMe = (myTeamId && item.account_id === myTeamId);
                  if (isMe) foundMe = true;

                  const nameSafe = escapeCtfMarkdown(item.name);
                  const nameStr = isMe ? `**${nameSafe} (YOU)**` : nameSafe;

                  msg += `${icon} ${rank}. ${nameStr} — ${item.score} pts\n`;
                }

                // Append Me if not in top N
                if (!foundMe && myTeamId) {
                  const myRankItem = list.find(x => x.account_id === myTeamId);
                  if (myRankItem) {
                    const nameSafe = escapeCtfMarkdown(myRankItem.name);
                    msg += `\n...\n📍 **Rank ${myRankItem.pos || "?"}. ${nameSafe} — ${myRankItem.score} pts**`;
                  }
                }

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);

              } catch (e) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error: ${e.message}`, true);
              }

            } else if (text.startsWith("/top")) {
              // Usage: /top (Global Accumulated)
              const lbStr = await env.CTFD_STORE.get("TELEGRAM_LEADERBOARD");
              if (!lbStr) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "🏆 **Telegram Leaderboard (Global)**\n\nBelum ada data. User harus login atau cek profile dulu.", true);
                return new Response("OK");
              }

              const lb = JSON.parse(lbStr);

              // Load All Events (Active + Archived)
              let allEvents = [];
              try { const eS = await env.CTFD_STORE.get("EVENTS"); if (eS) allEvents = JSON.parse(eS); } catch (e) { }

              // Filter: Only strictly Active or Archived (if deleted events exist they are ignored)
              // Ensure we compare Strings to avoid Type Mismatch
              const validEventIds = new Set(allEvents.map(e => String(e.id)));

              // Calculate Total Score and Solve Count per user (Only from Valid Events)
              const users = Object.values(lb).map(u => {
                let totalScore = 0;
                let totalSolves = 0;
                if (u.events) {
                  totalScore = Object.entries(u.events).reduce((sum, [eid, score]) => {
                    if (validEventIds.has(String(eid))) return sum + score;
                    return sum;
                  }, 0);
                }
                if (u.solves) {
                  totalSolves = Object.entries(u.solves).reduce((sum, [eid, count]) => {
                    if (validEventIds.has(String(eid))) return sum + count;
                    return sum;
                  }, 0);
                }
                return { ...u, totalScore, totalSolves };
              });

              const sorted = users.sort((a, b) => b.totalScore - a.totalScore);
              const top10 = sorted.slice(0, 10);

              let msg = `🏆 **Telegram Leaderboard (Global)**\nTop 10 User\n\n`;
              top10.forEach((u, i) => {
                let medal = "▪️";
                if (i === 0) medal = "🥇";
                if (i === 1) medal = "🥈";
                if (i === 2) medal = "🥉";
                msg += `${medal} **${u.telegram_name}** (${u.ctfd_name})\n   └─ 💎 ${u.totalScore} pts | 🚩 ${u.totalSolves} solves\n`;
              });

              msg += `\n_Rank berdasarkan akumulasi score di seluruh event yang diikuti._`;
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);

            } else if (text.startsWith("/leaderboard")) {
              // Usage: /leaderboard [event_id]

              // 1. Identify Event
              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              let events = [];
              let subs = [];
              try {
                const eS = await env.CTFD_STORE.get("EVENTS");
                const sS = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (eS) events = JSON.parse(eS);
                if (sS) subs = JSON.parse(sS);
              } catch (e) { }

              // Auto-detect Event ID if missing
              if (!eventId) {
                try { eventId = await env.CTFD_STORE.get(`CHAT_PREF_${chatId}`); } catch (e) { }
                if (!eventId) {
                  const mySubs = subs.filter(s => s.userId === chatId);
                  if (mySubs.length === 1) eventId = mySubs[0].eventId;
                }
              }

              // Fuzzy Match Event ID
              const normalize = (str) => (str || "").replace(/_/g, "").toLowerCase();
              const eventIdClean = normalize(eventId);
              const event = events.find(e => normalize(e.id) === eventIdClean);

              if (!event) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format: `/leaderboard <event_id>` (atau set event dulu).", true);
                return new Response("OK");
              }

              // 2. Get Credentials for this Event
              const sub = subs.find(s => (s.userId === chatId || s.userId === userId) && normalize(s.eventId) === eventIdClean);
              if (!sub) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Anda belum join event **${event.name}**.`, true);
                return new Response("OK");
              }

              const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
              if (sub.credentials.mode === 'token') headers["Authorization"] = `Token ${sub.credentials.value}`;
              else headers["Cookie"] = sub.credentials.value;

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔍 Analyzing Team Contributions for **${event.name}**...`, true);

              try {
                // 3. Fetch Team Info (Who am I within the team?)
                const meRes = await fetch(`${event.url}/api/v1/teams/me`, { headers });
                if (!meRes.ok) {
                  throw new Error("Gagal mengambil info Team. Apakah Anda sudah join team?");
                }
                const meJson = await meRes.json();
                const myTeam = meJson.data; // { id, name, members: [ {id, name, ...}, ... ] }

                if (!myTeam) throw new Error("Data team tidak ditemukan.");

                // 4. Fetch Team Solves
                const solvesRes = await fetch(`${event.url}/api/v1/teams/${myTeam.id}/solves`, { headers });
                if (!solvesRes.ok) throw new Error("Gagal mengambil data solves.");

                const solvesJson = await solvesRes.json();
                const solves = solvesJson.data; // [ { challenge_id, user_id, value, ... } ]

                // Map User ID -> Name
                const memberMap = {};
                // Handle mixed response: members might be objects or just IDs
                const memberIds = myTeam.members.map(m => (typeof m === 'object' ? m.id : m));

                // 5. Aggregate Scores AND Solve Counts
                const stats = {}; // { userId: { score: 0, count: 0 } }

                // Init stats
                memberIds.forEach(mid => { stats[mid] = { score: 0, count: 0 }; });

                solves.forEach(s => {
                  // Robust ID check: s.user.id OR s.user_id
                  const uid = s.user ? s.user.id : s.user_id;

                  // Valid User Check
                  if (uid && stats[uid]) {
                    const val = (s.challenge ? s.challenge.value : 0);
                    stats[uid].score += val;
                    stats[uid].count += 1;

                    // Capture Name from Solve Data (Optimization)
                    if (s.user && s.user.name) {
                      memberMap[uid] = s.user.name;
                    }
                  }
                });

                // FETCH MISSING NAMES (for members with 0 solves)
                const missingIds = memberIds.filter(mid => !memberMap[mid]);
                if (missingIds.length > 0) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔄 Resolving names for ${missingIds.length} members...`, true);
                  await Promise.all(missingIds.map(async (mid) => {
                    try {
                      const uRes = await fetch(`${event.url}/api/v1/users/${mid}`, { headers });
                      if (uRes.ok) {
                        const uJson = await uRes.json();
                        if (uJson.data && uJson.data.name) {
                          memberMap[mid] = uJson.data.name;
                        }
                      }
                    } catch (e) { }
                  }));
                }

                // 6. Convert to Array and Sort
                const ranking = Object.keys(stats).map(uid => ({
                  id: uid,
                  name: memberMap[uid] || `User #${uid}`,
                  score: stats[uid].score,
                  count: stats[uid].count
                })).sort((a, b) => b.score - a.score || b.count - a.count); // Sort by Score, then Count

                // 7. Display
                let msg = `👥 **Team Internal Rank: ${myTeam.name}**\nEvent: ${event.name}\n\n`;
                ranking.forEach((m, i) => {
                  let icon = "👤";
                  if (i === 0) icon = "👑 MVP";
                  else if (i === 1) icon = "🥈";
                  else if (i === 2) icon = "🥉";

                  msg += `${icon} **${m.name}**\n   └─ 💎 ${m.score} pts | 🚩 ${m.count} solves\n`;
                });

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, true);

              } catch (e) {
                console.error("Team Rank Error", e);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Gagal mengambil data team: ${e.message}`, true);
              }


            } else if (text.startsWith("/set_notify")) {
              try {
                // Usage: /set_notify [event_id|all] (in group)

                // Clean input: remove @BotName if present
                const cleanText = text.replace(/@[a-zA-Z0-9_]+/, "").trim();
                const parts = cleanText.split(/\s+/);
                let inputId = parts.length >= 2 ? parts[1] : null;

                // 1. Load Subs
                let subs = [];
                try {
                  const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                  if (sStored) subs = JSON.parse(sStored);
                } catch (e) { }

                if (!Array.isArray(subs) || subs.length === 0) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada sesi login aktif. Gunakan `/join_event` via DM dulu.", true);
                  return new Response("OK");
                }

                // Load Events to check Archived status
                let allEvents = [];
                try {
                  const evStr = await env.CTFD_STORE.get("EVENTS");
                  if (evStr) allEvents = JSON.parse(evStr);
                } catch (e) { }

                const archivedIds = new Set(allEvents.filter(e => e.archived).map(e => e.id));

                // 2. Identify Subscription(s) to update
                // Safe string comparison for IDs + Filter Archived
                const mySubs = subs.map((s, i) => ({ ...s, index: i }))
                  .filter(s => String(s.userId) === String(userId) && !archivedIds.has(s.eventId));

                if (mySubs.length === 0) {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Anda belum join event apapun.", true);
                  return new Response("OK");
                }

                if (inputId === "all") {
                  // BULK UPDATE all active events
                  mySubs.forEach(s => {
                    subs[s.index].targetChatId = chatId;
                    subs[s.index].targetChatTitle = payload.message.chat.title || "Group Chat";
                  });

                  await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subs));
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Web Monitoring Updated!**\n\nSemua notifikasi dari **${mySubs.length} event aktif** Anda sekarang diarahkan ke Group ini.`, true);
                  return new Response("OK");
                }

                // SINGLE UPDATE
                let targetSubIndex = -1;

                // If inputId provided, fuzzy match eventId
                if (inputId) {
                  const match = mySubs.find(s => (s.eventId || "").toLowerCase().includes(inputId.toLowerCase()));
                  if (match) targetSubIndex = match.index;
                }
                // Default: if only 1 sub, pick it
                else if (mySubs.length === 1) {
                  targetSubIndex = mySubs[0].index;
                }

                // If still not found
                if (targetSubIndex === -1) {
                  if (mySubs.length > 1) {
                    const eventsList = mySubs.map(s => `- <code>${s.eventId}</code>`).join("\n");
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Anda punya banyak event aktif. Spesifikasikan ID:\n\n${eventsList}\n\nGunakan:\n• <code>/set_notify &lt;name&gt;</code>\n• <code>/set_notify all</code>`, "HTML");
                  } else {
                    // Should imply inputId provided but not found
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ Event "${inputId}" tidak ditemukan di daftar langganan Anda.`, true);
                  }
                  return new Response("OK");
                }

                // 3. Update Target Chat
                subs[targetSubIndex].targetChatId = chatId;
                subs[targetSubIndex].targetChatTitle = payload.message.chat.title || "Group Chat";

                await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subs));

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Notifikasi Dipindahkan!**\n\nEvent: ${subs[targetSubIndex].eventId}\nTarget Baru: **${subs[targetSubIndex].targetChatTitle}**`, true);

              } catch (e) {
                console.error("Set Notify Error:", e);
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Terjadi kesalahan: ${e.message}`, true);
              }

            } else if (text.startsWith("/unset_notify")) {
              // Usage: /unset_notify [event_id]
              const parts = text.trim().split(/\s+/);
              let eventId = parts.length >= 2 ? parts[1] : null;

              // 1. Load Subs
              let subs = [];
              try {
                const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
                if (sStored) subs = JSON.parse(sStored);
              } catch (e) { }

              // 2. Identify Subscription
              let targetSubIndex = -1;
              const mySubs = subs.map((s, i) => ({ ...s, index: i })).filter(s => s.userId === userId || s.userId === payload.message.from.id);

              if (eventId) {
                targetSubIndex = mySubs.find(s => (s.eventId || "").includes(eventId))?.index;
              } else if (mySubs.length === 1) {
                targetSubIndex = mySubs[0].index;
              }

              if (targetSubIndex === -1) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada monitoring group yang aktif untuk Anda.", true);
                return new Response("OK");
              }

              // 3. Clear Target Chat (Revert to DM)
              delete subs[targetSubIndex].targetChatId;
              delete subs[targetSubIndex].targetChatTitle;

              await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subs));

              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🔕 **Notifikasi Group Dimatikan.**\n\nNotifikasi event **${subs[targetSubIndex].eventId}** akan kembali masuk ke Private Chat Anda.`, true);

            } else if (text.startsWith("/monitor")) {
              // Usage: /monitor https://ctf.example.com 123
              const parts = text.split(" ");
              if (parts.length === 1) {
                const configStr = await env.CTFD_STORE.get("MONITOR_CONFIG");
                if (configStr) {
                  const config = JSON.parse(configStr);
                  if (config.mode === 'auth') {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Web Monitoring Aktif!**\n\nTarget: ${config.url}\nMode: Authenticated\n\nBot akan mengecek setiap 1 menit.`, true);
                  } else if (config.teamId) {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Web Monitoring Aktif!**\n\nTarget: ${config.url}\nTeam ID: ${config.teamId}\nMode: Public\n\nBot akan mengecek setiap 1 menit.`, true);
                  } else {
                    await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada monitoring aktif. Gunakan `/monitor <url> <team_id>` atau `/login_ctf`.", true);
                  }
                } else {
                  await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Tidak ada monitoring aktif. Gunakan `/monitor <url> <team_id>` atau `/login_ctf`.", true);
                }
                return new Response("OK");
              } else if (parts.length !== 3) {
                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "⚠️ Format salah.\nGunakan: `/monitor <url_ctfd> <team_id>`\nContoh: `/monitor https://demo.ctfd.io 15`", true);
              } else {
                let url = parts[1];
                if (url.endsWith("/")) url = url.slice(0, -1);
                if (!url.startsWith("http")) url = "https://" + url;
                const teamId = parts[2];
                const config = { url, teamId, chatId, mode: 'public' };
                await env.CTFD_STORE.put("MONITOR_CONFIG", JSON.stringify(config));

                // Fetch initial solves to preventing spamming old ones
                let initialSolves = [];
                try {
                  const checkRes = await fetch(`${url}/api/v1/teams/${teamId}/solves`, {
                    headers: { "Content-Type": "application/json", "User-Agent": "TelegramBot/1.0" }
                  });
                  if (checkRes.ok) {
                    const data = await checkRes.json();
                    initialSolves = data.data || [];
                  }
                } catch (e) {
                  console.error("Failed to fetch initial solves:", e);
                }

                await env.CTFD_STORE.put("LAST_SOLVES", JSON.stringify(initialSolves));

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Web Monitoring Dimulai!**\n\nTarget: ${url}\nTeam ID: ${teamId}\nInitial Solves: ${initialSolves.length}`, true);
              }

            } else if (text.startsWith("/debug_monitor")) {
              const dbgConfig = await env.CTFD_STORE.get("MONITOR_CONFIG");
              const dbgLast = await env.CTFD_STORE.get("LAST_SOLVES");
              let dbgMsg = "🔍 **Detail Debug Info**\n\n";

              if (!dbgConfig) {
                dbgMsg += "❌ Config: Not Found.\n";
              } else {
                const cfg = JSON.parse(dbgConfig);
                dbgMsg += `🔗 URL: ${cfg.url}\n`;
                dbgMsg += `🛠 Mode: ${cfg.mode}\n\n`;

                try {
                  const headers = { "User-Agent": "TelegramBot/1.0", "Content-Type": "application/json" };
                  if (cfg.token) {
                    headers["Authorization"] = `Token ${cfg.token}`;
                  } else if (cfg.cookie) {
                    headers["Cookie"] = cfg.cookie;
                  }

                  // Parallel checks
                  const [resMe, resTeam, resUserSolves, resTeamSolves] = await Promise.all([
                    fetch(`${cfg.url}/api/v1/users/me`, { headers }).then(r => r.json()).catch(() => null),
                    fetch(`${cfg.url}/api/v1/teams/me`, { headers }).then(r => r.json()).catch(() => null),
                    fetch(`${cfg.url}/api/v1/users/me/solves`, { headers }).then(r => r.json()).catch(() => null),
                    fetch(`${cfg.url}/api/v1/teams/me/solves`, { headers }).then(r => r.json()).catch(() => null)
                  ]);

                  if (resMe && resMe.data) {
                    dbgMsg += `👤 **Akun Login:** ${resMe.data.name} (ID: ${resMe.data.id})\n`;
                  } else {
                    dbgMsg += `👤 **Akun Login:** Gagal ambil data (Sesi Valid?)\n`;
                  }

                  if (resTeam && resTeam.data) {
                    dbgMsg += `🛡 **Team:** ${resTeam.data.name} (ID: ${resTeam.data.id})\n`;
                  } else {
                    dbgMsg += `🛡 **Team:** Tidak ada / Mode User Only\n`;
                  }

                  const userCount = (resUserSolves && resUserSolves.data) ? resUserSolves.data.length : 0;
                  const teamCount = (resTeamSolves && resTeamSolves.data) ? resTeamSolves.data.length : 0;

                  dbgMsg += `\n📊 **Statistik Solves:**\n`;
                  dbgMsg += `- Via User Endpoint: ${userCount}\n`;
                  dbgMsg += `- Via Team Endpoint: ${teamCount}\n`;

                  const kvCount = dbgLast ? JSON.parse(dbgLast).length : 0;
                  dbgMsg += `- Disimpan di Bot (KV): ${kvCount}\n`;

                } catch (e) {
                  dbgMsg += `❌ Exception: ${e.message}\n`;
                }

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, dbgMsg, true);
              }
            } else if (text.startsWith("/stop_monitor")) {
              await env.CTFD_STORE.delete("MONITOR_CONFIG");
              await env.CTFD_STORE.delete("LAST_SOLVES");
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, "🛑 Monitoring dihentikan.", true);
            } else if (text.startsWith("/")) {
              // Unknown command -> Ignore silently
              console.log(`Ignored unknown command: ${text}`);
            } else {
              // Ignore non-command messages (do nothing)
              console.log(`Ignored non-command message from ${chatId}: ${text}`);
            }
          } else {
            console.log("Ignored message with no text (possibly a service message like joined/left group).");
          }
        }
      } catch (e) {
        console.error("Error in fetch handler:", e);
        return new Response("Error processing request", { status: 500 });
      }
      return new Response("OK");
    }
    return new Response("Send a POST request to this worker with a Telegram webhook payload.");
  },



  async registerCTFd(url, name, email, password, customData = {}, env = null, chatId = null) {
    try {
      // 1. Get CSRF via GET /register
      console.log(`Fetching register page: ${url}/register`);
      const regPageRes = await fetch(`${url}/register`, {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        }
      });
      const regPageText = await regPageRes.text();

      // Debug: Check title
      const titleMatch = regPageText.match(/<title>(.*?)<\/title>/);
      console.log("Register Page Title:", titleMatch ? titleMatch[1] : "No Title");

      // Extract all input names (inputs, selects, textareas) to check for custom fields
      const inputMatches = [...regPageText.matchAll(/<input[^>]+name=["']([^"']+)["']/g)];
      const selectMatches = [...regPageText.matchAll(/<select[^>]+name=["']([^"']+)["']/g)];
      const textMatches = [...regPageText.matchAll(/<textarea[^>]+name=["']([^"']+)["']/g)];

      const foundFields = [
        ...inputMatches.map(m => m[1]),
        ...selectMatches.map(m => m[1]),
        ...textMatches.map(m => m[1])
      ];
      console.log("Found fields detected on page:", JSON.stringify(foundFields));

      const alwaysStandard = ["nonce", "name", "email", "password", "_submit"];
      const optionalStandard = ["affiliation", "website", "country"];

      let detectedRequired = [];

      // Check optional standard fields for 'required' attribute
      optionalStandard.forEach(field => {
        const regex = new RegExp(`<input[^>]+name=["']${field}["'][^>]*required`, "i");
        if (regex.test(regPageText)) {
          console.log(`[DEBUG] Standard field '${field}' is REQUIRED by this CTF.`);
          detectedRequired.push(field);
        }
      });

      // Also check if any custom fields found are required (usually they are if they exist, but check tag)
      // Actually, assume all strictly custom fields are required for now, or check regex.
      // Let's stick to the list logic but include the detected standard ones.

      // Filter out only the ALWAYS standard ones.
      // If 'affiliation' is NOT required, we still exclude it from mandatory list? 
      // Yes, if it's optional, user doesn't NEED to provide it.

      const requiredCustomFields = foundFields.filter(f => {
        if (alwaysStandard.includes(f)) return false; // Always standard, we handle it
        if (optionalStandard.includes(f)) {
          // Only include if it was detected as required
          return detectedRequired.includes(f);
        }
        return true; // Unknown field -> Assume required custom
      });

      // SMART MAPPING (Forward): keys like "fields1" -> "fields[1]"
      const mappedCustomData = { ...customData };

      Object.keys(customData).forEach(userKey => {
        // Check if userKey is like "fields1", "fields2"
        const match = userKey.match(/^fields(\d+)$/);
        if (match) {
          const num = match[1];
          const correctKey = `fields[${num}]`;
          console.log(`Auto-mapping user input ${userKey} -> ${correctKey}`);
          mappedCustomData[correctKey] = customData[userKey];
        }
      });

      // Check if we are missing any required custom fields
      const missingFields = requiredCustomFields.filter(f => !mappedCustomData.hasOwnProperty(f));

      if (missingFields.length > 0) {
        console.log("Warning: Potential missing custom fields:", missingFields);
        return { success: false, error: `Gagal. Field wajib belum diisi: ${missingFields.join(", ")}.\n\nCoba tambahkan di command:\n/register_team ... ${missingFields[0]}=nilai` };
      }

      // Extract CSRF
      let csrfMatch = regPageText.match(/name=["']nonce["'][\s\S]*?value=["']([a-zA-Z0-9]+)["']/);
      if (!csrfMatch) {
        csrfMatch = regPageText.match(/value=["']([a-zA-Z0-9]+)["'][\s\S]*?name=["']nonce["']/);
      }
      const csrfNonce = csrfMatch ? csrfMatch[1] : null;

      if (!csrfNonce) {
        return { success: false, error: "Gagal mengambil CSRF Token (Halaman register mungkin beda)." };
      }

      // Capture initial cookies
      const initialCookies = regPageRes.headers.get("set-cookie");

      // 2. Perform POST /register
      const formData = new URLSearchParams();
      formData.append("name", name);
      formData.append("email", email);
      // 2. Perform POST /register (Manual Body Construction)
      const params = [
        `name=${encodeURIComponent(name)}`,
        `email=${encodeURIComponent(email)}`,
        `password=${encodeURIComponent(password)}`,
        `nonce=${encodeURIComponent(csrfNonce)}`,
        `_submit=Submit`
      ];

      // Append custom data
      for (const [key, value] of Object.entries(mappedCustomData)) {
        // If we have a 'fieldsX' key, check if we also have 'fields[X]'. If so, skip 'fieldsX'.
        const simpleMatch = key.match(/^fields(\d+)$/);
        if (simpleMatch) {
          const num = simpleMatch[1];
          if (mappedCustomData[`fields[${num}]`]) {
            continue;
          }
        }
        // Manually encode key and value. 
        // IMPORTANT: encode 'fields[1]' -> 'fields%5B1%5D'
        params.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
      }

      const bodyString = params.join("&");
      console.log(`[DEBUG] Raw POST Body: ${bodyString}`);

      const postHeaders = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": `${url}/register`,
        "Origin": url,
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1"
      };

      let cookieHeaderVal = "";

      // Try modern API first (Cloudflare Workers / Node 18+)
      if (typeof regPageRes.headers.getSetCookie === 'function') {
        const cookies = regPageRes.headers.getSetCookie();
        if (cookies && cookies.length > 0) {
          cookieHeaderVal = cookies.map(c => c.split(';')[0].trim()).join('; ');
        }
      }

      // Fallback to manual parsing if API empty or not found
      if (!cookieHeaderVal && initialCookies) {
        // Heuristic split for comma-separated Set-Cookie header
        // Splits on comma only if followed by valid cookie-name=value pattern
        // This avoids splitting on commas inside Date values
        const rawCookies = initialCookies.split(/, (?=[a-zA-Z0-9%!#$%&'*+.^_`|~-]+=)/);

        cookieHeaderVal = rawCookies.map(c => c.split(';')[0].trim()).join('; ');
      }

      if (cookieHeaderVal) {
        postHeaders["Cookie"] = cookieHeaderVal;
        console.log(`[DEBUG] Final Cookie Header: ${cookieHeaderVal}`);
      }

      console.log(`[DEBUG] CSRF Nonce: ${csrfNonce}`);
      console.log("Posting registration...");
      const regPostRes = await fetch(`${url}/register`, {
        method: "POST",
        headers: postHeaders,
        body: bodyString,
        redirect: "manual"
      });

      console.log(`Register POST Status: ${regPostRes.status}`);

      // Success is usually a redirect to /challenges or / (302) or loading the page directly (200)
      // Errors usually stay on 200 but keep same page content with error message.

      if (regPostRes.status === 302) {
        return { success: true };
      } else if (regPostRes.status === 200) {
        const resText = await regPostRes.text();

        // Log short snippet only
        console.log("Register Response Title:", (resText.match(/<title>(.*?)<\/title>/) || [])[1]);

        // Try to scrape specific error message from alert-danger or form errors
        // Standard CTFd error format: <div class="alert alert-danger ..."> Error text </div>
        // Or <small class="form-text text-muted"> Error </small> (sometimes invalid-feedback)

        const alertMatch = resText.match(/<div[^>]*class=["'][^"']*alert-danger[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
        if (alertMatch) {
          // Clean tags
          const errorMsg = alertMatch[1].replace(/<[^>]+>/g, '').trim();
          return { success: false, error: errorMsg };
        }

        // Input specific errors often in sibling small tags or div.invalid-feedback
        const invalidMatch = resText.match(/<div[^>]*class=["'][^"']*invalid-feedback[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
        if (invalidMatch) {
          const errorMsg = invalidMatch[1].replace(/<[^>]+>/g, '').trim();
          return { success: false, error: `Invalid Data: ${errorMsg}` };
        }

        if (resText.includes("User name already taken")) return { success: false, error: "Username sudah terpakai." };
        if (resText.includes("Email already taken")) return { success: false, error: "Email sudah terpakai." };

        if (resText.includes('value="Submit"')) {
          // If we are here, we are on the form page, but couldn't find a standard error message.
          // It might be a general error at the top or inside a script.
          return { success: false, error: "Registrasi gagal (Kembali ke form). Cek data. (Mungkin password kurang kuat atau format salah)" };
        }
        return { success: true }; // Assume success if no form? modifying heuristic risky but try best effort.
      }

      return { success: false, error: `Gagal Register. Status Code: ${regPostRes.status}` };

    } catch (e) {
      console.error("Register Exception:", e);
      return { success: false, error: `Exception: ${e.message}` };
    }
  },

  async createTeamCTFd(url, name, password, customData = {}, cookie, env, chatId) {
    try {
      console.log(`[CreateTeam] Fetching page: ${url}/teams/new`);
      console.log(`[CreateTeam] Using Cookie: ${cookie}`);

      const headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Cookie": cookie
      };

      const pageRes = await fetch(`${url}/teams/new`, { headers });
      console.log(`[CreateTeam] GET Status: ${pageRes.status}`);

      if (pageRes.status !== 200) {
        return { success: false, error: `Gagal akses halaman (Status: ${pageRes.status}). Session mungkin expired/invalid.` };
      }

      // MERGE COOKIES: Capture any new cookies from GET /teams/new
      let finalCookie = cookie;
      let newCookiesStr = "";

      if (typeof pageRes.headers.getSetCookie === 'function') {
        const c = pageRes.headers.getSetCookie();
        if (c && c.length > 0) newCookiesStr = c.map(x => x.split(';')[0].trim()).join('; ');
      } else {
        const raw = pageRes.headers.get("set-cookie");
        if (raw) {
          const rawCookies = raw.split(/, (?=[a-zA-Z0-9%!#$%&'*+.^_`|~-]+=)/);
          newCookiesStr = rawCookies.map(c => c.split(';')[0].trim()).join('; ');
        }
      }



      const pageText = await pageRes.text();

      const titleMatch = pageText.match(/<title>(.*?)<\/title>/);
      const pageTitle = titleMatch ? titleMatch[1] : "No Title";
      console.log(`[CreateTeam] Page Title: ${pageTitle}`);

      if (pageTitle.toLowerCase().includes("login")) {
        return { success: false, error: "Gagal: Sesi tidak valid. Halaman redirect ke Login. Coba login ulang." };
      }

      // --- FIELD DETECTION START ---
      const inputMatches = [...pageText.matchAll(/<input[^>]+name=["']([^"']+)["']/g)];
      const selectMatches = [...pageText.matchAll(/<select[^>]+name=["']([^"']+)["']/g)];
      const textMatches = [...pageText.matchAll(/<textarea[^>]+name=["']([^"']+)["']/g)];
      const foundFields = [
        ...inputMatches.map(m => m[1]),
        ...selectMatches.map(m => m[1]),
        ...textMatches.map(m => m[1])
      ];
      console.log("[CreateTeam] Found fields:", JSON.stringify(foundFields));

      const alwaysStandard = ["nonce", "name", "password", "_submit"];

      const requiredCustomFields = foundFields.filter(f => !alwaysStandard.includes(f));

      // Perform early mapping for check
      const checkMappedData = { ...customData };
      Object.keys(customData).forEach(userKey => {
        const match = userKey.match(/^fields(\d+)$/);
        if (match) {
          const num = match[1];
          const correctKey = `fields[${num}]`;
          checkMappedData[correctKey] = customData[userKey];
        }
      });

      const missingFields = requiredCustomFields.filter(f => !checkMappedData.hasOwnProperty(f));

      if (missingFields.length > 0) {
        console.log("Warning: Potential missing custom fields:", missingFields);
        const cleanFields = missingFields.map(f => f.replace(/\[/g, '_').replace(/\]/g, ''));
        return { success: false, error: `Gagal. Field wajib belum diisi: ${cleanFields.join(", ")}.\n\nCoba tambahkan di command:\n/create_team ... fields[..]=nilai` };
      }
      // --- FIELD DETECTION END ---

      // Debug context if fields not found
      // --- FIELD DETECTION END ---

      // Extract CSRF
      let csrfMatch = pageText.match(/name=["']nonce["'][\s\S]*?value=["']([a-zA-Z0-9]+)["']/);
      if (!csrfMatch) {
        csrfMatch = pageText.match(/value=["']([a-zA-Z0-9]+)["'][\s\S]*?name=["']nonce["']/);
      }
      const csrfNonce = csrfMatch ? csrfMatch[1] : null;

      if (!csrfNonce) {
        return { success: false, error: "Gagal mengambil CSRF Token (Mungkin belum login/session expired)." };
      }

      // SMART MAPPING (Forward) for custom fields
      const mappedCustomData = { ...customData };
      Object.keys(customData).forEach(userKey => {
        const match = userKey.match(/^fields(\d+)$/);
        if (match) {
          const num = match[1];
          const correctKey = `fields[${num}]`;
          mappedCustomData[correctKey] = customData[userKey];
        }
      });

      // Manual Body Construction
      const params = [
        `name=${encodeURIComponent(name)}`,
        `password=${encodeURIComponent(password)}`,
        `nonce=${encodeURIComponent(csrfNonce)}`,
        `_submit=Create`
      ];

      for (const [key, value] of Object.entries(mappedCustomData)) {
        // Dedupe
        const simpleMatch = key.match(/^fields(\d+)$/);
        if (simpleMatch) {
          const num = simpleMatch[1];
          if (mappedCustomData[`fields[${num}]`]) continue;
        }
        params.push(`${encodeURIComponent(key)}=${encodeURIComponent(value)}`);
      }
      const bodyString = params.join("&");

      // Catch any cookie rotation from GET
      let mergedCookie = cookie;
      let rotatedCookiesStr = "";
      if (typeof pageRes.headers.getSetCookie === 'function') {
        const c = pageRes.headers.getSetCookie();
        if (c && c.length > 0) rotatedCookiesStr = c.map(x => x.split(';')[0].trim()).join('; ');
      } else {
        const raw = pageRes.headers.get("set-cookie");
        if (raw) {
          const rawCookies = raw.split(/, (?=[a-zA-Z0-9%!#$%&'*+.^_`|~-]+=)/);
          rotatedCookiesStr = rawCookies.map(c => c.split(';')[0].trim()).join('; ');
        }
      }
      if (rotatedCookiesStr) {
        console.log(`[CreateTeam] Appending new cookies: ${rotatedCookiesStr}`);
        mergedCookie = `${mergedCookie}; ${rotatedCookiesStr}`;
      }

      console.log(`[CreateTeam] Posting to ${url}/teams/new...`);

      const postHeaders = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": `${url}/teams/new`,
        "Origin": url,
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "same-origin",
        "Sec-Fetch-User": "?1",
        "Cookie": mergedCookie
      };

      const createRes = await fetch(`${url}/teams/new`, {
        method: "POST",
        headers: postHeaders,
        body: bodyString,
        redirect: "manual"
      });

      if (createRes.status === 302) {
        return { success: true };
      } else if (createRes.status === 200) {
        const resText = await createRes.text();

        // Scrape errors
        const alertMatch = resText.match(/<div[^>]*class=["'][^"']*alert-danger[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
        if (alertMatch) {
          const errorMsg = alertMatch[1].replace(/<[^>]+>/g, '').trim();
          return { success: false, error: errorMsg };
        }
        const invalidMatch = resText.match(/<div[^>]*class=["'][^"']*invalid-feedback[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
        if (invalidMatch) {
          const errorMsg = invalidMatch[1].replace(/<[^>]+>/g, '').trim();
          return { success: false, error: `Invalid Data: ${errorMsg}` };
        }

        if (resText.includes("Team name already taken")) return { success: false, error: "Nama tim sudah terpakai." };

        return { success: false, error: "Gagal membuat tim (Kembali ke form)." };
      }

      // Read error body for 403/500
      const errorText = await createRes.text();

      // Scrape specific errors even on 403
      const alertMatch = errorText.match(/<div[^>]*class=["'][^"']*alert-danger[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
      if (alertMatch) {
        const errorMsg = alertMatch[1].replace(/<[^>]+>/g, '').trim();
        return { success: false, error: `Status ${createRes.status}: ${errorMsg}` };
      }
      const invalidMatch = errorText.match(/<div[^>]*class=["'][^"']*invalid-feedback[^"']*["'][^>]*>([\s\S]*?)<\/div>/i);
      if (invalidMatch) {
        const errorMsg = invalidMatch[1].replace(/<[^>]+>/g, '').trim();
        return { success: false, error: `Status ${createRes.status}: ${errorMsg}` };
      }

      const titleErr = errorText.match(/<title>(.*?)<\/title>/);
      const h1Err = errorText.match(/<h1>(.*?)<\/h1>/);
      const snippet = h1Err ? h1Err[1] : (titleErr ? titleErr[1] : errorText.substring(0, 200));

      return { success: false, error: `Status ${createRes.status}: ${snippet.replace(/<[^>]+>/g, '')}` };

    } catch (e) {
      return { success: false, error: `Exception: ${e.message}` };
    }
  },


  async scheduled(event, env, ctx) {
    ctx.waitUntil(Promise.all([
      this.checkCTFdSolves(env),
      this.checkUpcomingCTF(env)
    ]));
  },

  async checkCTFdSolves(env) {
    try {
      // 1. Load Subscriptions & Events
      let subs = [];
      try {
        const sStored = await env.CTFD_STORE.get("SUBSCRIPTIONS");
        if (sStored) subs = JSON.parse(sStored);
      } catch (e) { }

      if (subs.length === 0) return;

      let events = [];
      try {
        const eStored = await env.CTFD_STORE.get("EVENTS");
        if (eStored) events = JSON.parse(eStored);
      } catch (e) { }

      let dirty = false;

      // 2. Load Processed Global Solves (for @CTF_Channel)
      let globalProcessed = {};
      try {
        const gpStr = await env.CTFD_STORE.get("PROCESSED_CHANNEL_SOLVES");
        if (gpStr) globalProcessed = JSON.parse(gpStr);
      } catch (e) { }

      let dirtyGlobal = false;
      const nowTs = Date.now();

      // 3. Loop Handlers
      await Promise.all(subs.map(async (sub) => {
        try {
          const event = events.find(e => e.id === sub.eventId);
          if (!event || event.archived) return;

          const headers = {
            "User-Agent": "TelegramBot/1.0",
            "Content-Type": "application/json"
          };

          if (sub.credentials.mode === 'token') {
            headers["Authorization"] = `Token ${sub.credentials.value}`;
          } else if (sub.credentials.mode === 'auth') {
            headers["Cookie"] = sub.credentials.value;
          } else {
            return;
          }

          const fetchUrl = `${event.url}/api/v1/teams/me/solves`;
          const res = await fetch(fetchUrl, { headers });

          if (!res.ok) return;

          const json = await res.json();
          if (!json.success) return;

          const currentSolves = json.data || [];
          const lastSolves = sub.lastSolves || [];

          const seenIds = new Set(lastSolves.map(s => s.challenge_id));
          const newSolves = currentSolves.filter(s => !seenIds.has(s.challenge_id));

          if (newSolves.length > 0) {
            for (const solve of newSolves) {
              const chalName = solve.challenge ? solve.challenge.name : "Unknown Chal";
              const cat = solve.challenge ? solve.challenge.category : "Misc";
              const pts = solve.challenge ? solve.challenge.value : 0;

              // Identify Solver
              const solverName = (solve.user && solve.user.name) ? solve.user.name : sub.userName;
              const solverId = (solve.user && solve.user.id) ? solve.user.id : sub.userId;

              // Inline HTML Escape
              const esc = (t) => String(t || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

              // Format Date to WIB (UTC+7)
              const now = new Date();
              const options = {
                timeZone: "Asia/Jakarta",
                day: "numeric", month: "numeric", year: "numeric",
                hour: "2-digit", minute: "2-digit", second: "2-digit"
              };
              const timeStr = now.toLocaleString("id-ID", options) + " WIB";

              const msgBody = `🛡 <b>Challenge:</b> ${esc(chalName)}\n` +
                `📂 <b>Category:</b> ${esc(cat)}\n` +
                `💎 <b>Points:</b> ${pts}\n` +
                `🕒 ${timeStr}`;

              // 1. Personal/Group Notification
              const msgPersonal = `🚩 <b>SOLVED!</b>\n\n` +
                `👤 <b>User:</b> ${esc(solverName)}\n` +
                msgBody;

              const targetChatId = sub.targetChatId || sub.userId;
              await this.sendMessage(env.TELEGRAM_BOT_TOKEN, targetChatId, msgPersonal, "HTML");

              // 2. Global Channel Notification (@CTF_Channel)
              // Deduplication Key: EventID + ChalID + SolverID
              const globalKey = `${sub.eventId}_${solve.challenge_id}_${solverId}`;

              // Only send if NOT processed globally (First detection across all subs)
              if (!globalProcessed[globalKey]) {
                const msgGlobal = `🚩 <b>SOLVED!</b>\n\n` +
                  `👤 <b>User:</b> ${esc(solverName)}\n` +
                  `🏟 <b>Event:</b> ${esc(event.name)}\n` +
                  msgBody;

                await this.sendMessage(env.TELEGRAM_BOT_TOKEN, "@CTF_Channel", msgGlobal, "HTML");

                globalProcessed[globalKey] = nowTs;
                dirtyGlobal = true;
              }
            }

            sub.lastSolves = currentSolves;
            sub.lastCheck = Date.now();
            dirty = true;
          }

        } catch (e) {
          console.error(`Monitor Error [${sub.id}]:`, e);
        }
      }));

      // Cleanup Old Global Processed Keys (> 24 Hours)
      if (dirtyGlobal) {
        Object.keys(globalProcessed).forEach(k => {
          if (nowTs - globalProcessed[k] > 24 * 60 * 60 * 1000) {
            delete globalProcessed[k];
          }
        });
        await env.CTFD_STORE.put("PROCESSED_CHANNEL_SOLVES", JSON.stringify(globalProcessed));
      }

      // 3. Save Subscriptions if updated
      if (dirty) {
        await env.CTFD_STORE.put("SUBSCRIPTIONS", JSON.stringify(subs));
      }

    } catch (e) {
      console.error("Global Monitor Error:", e);
    }
  },

  async checkUpcomingCTF(env) {
    try {
      // 1. Target Subscribers (Hardcoded to Channel only)
      const subs = ["@CTF_Channel"];

      // 2. Load Processed State & Cache
      let processed = {};
      try {
        const pStr = await env.CTFD_STORE.get("PROCESSED_UPCOMING_NOTIFS");
        if (pStr) processed = JSON.parse(pStr);
      } catch (e) { }

      let cache = null;
      try {
        const cStr = await env.CTFD_STORE.get("CTFTIME_CACHE");
        if (cStr) cache = JSON.parse(cStr);
      } catch (e) { }

      const now = Date.now();
      let events = [];

      // 3. Fetch Data (Cache Strategy: 30 minutes)
      // Only fetch if cache is empty or older than 30 mins
      if (cache && (now - cache.timestamp < 30 * 60 * 1000)) {
        events = cache.data;
      } else {
        const start = Math.floor(now / 1000); // Current timestamp in seconds
        const finish = start + (3 * 24 * 60 * 60); // Look ahead 3 days
        const url = `https://ctftime.org/api/v1/events/?limit=10&start=${start}&finish=${finish}`;

        const res = await fetch(url, { headers: { "User-Agent": "TelegramBot/1.0" } });
        if (res.ok) {
          events = await res.json();
          // Update Cache
          await env.CTFD_STORE.put("CTFTIME_CACHE", JSON.stringify({ timestamp: now, data: events }));
        }
      }

      if (!events || events.length === 0) return;

      let dirty = false;

      // 4. Check Thresholds
      for (const event of events) {
        const startParams = Date.parse(event.start); // ISO format
        const diffMs = startParams - now;
        const diffHours = diffMs / (1000 * 60 * 60);

        let thresholdKey = null;
        let alertTitle = "";

        // Threshold 1: 24 Hours (Range: 23.5 - 24.5h)
        if (diffHours >= 23.5 && diffHours <= 24.5) {
          thresholdKey = `${event.id}_24h`;
          alertTitle = "⏳ **CTF Starts in 24 Hours!**";
        }
        // Threshold 2: 1 Hour (Range: 0 - 1.2h)
        else if (diffHours > 0 && diffHours <= 1.2) {
          thresholdKey = `${event.id}_1h`;
          alertTitle = "🚀 **CTF Starts in 1 Hour!**";
        }

        if (thresholdKey && !processed[thresholdKey]) {
          // SEND NOTIFICATION
          const formatStr = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");
          const timeStr = new Date(startParams).toLocaleString("id-ID", { timeZone: "Asia/Jakarta" }) + " WIB";

          const msg = `${alertTitle}\n\n` +
            `📛 <b>${formatStr(event.title)}</b>\n` +
            `📅 ${timeStr}\n` +
            `🔗 ${event.url}\n` +
            `🏆 Weight: ${event.weight} | Format: ${event.format}`;

          // Broadcast
          await Promise.all(subs.map(chatId => this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, msg, "HTML")));

          processed[thresholdKey] = now;
          dirty = true;
        }
      }

      // Cleanup old processed keys (older than 2 days)
      Object.keys(processed).forEach(k => {
        if (now - processed[k] > 2 * 24 * 60 * 60 * 1000) {
          delete processed[k];
          dirty = true;
        }
      });

      // 5. Save State
      if (dirty) {
        await env.CTFD_STORE.put("PROCESSED_UPCOMING_NOTIFS", JSON.stringify(processed));
      }

    } catch (e) {
      console.error("Upcoming CTF Error:", e);
    }
  },

  async loginCTFd(url, username, password) {
    try {
      // 1. Get CSRF via GET /login
      console.log(`Fetching login page: ${url}/login`);
      const loginPageRes = await fetch(`${url}/login`, {
        headers: {
          "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
          "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8"
        }
      });
      const loginPageText = await loginPageRes.text();

      // Debug: Check title
      const titleMatch = loginPageText.match(/<title>(.*?)<\/title>/);
      console.log("Login Page Title:", titleMatch ? titleMatch[1] : "No Title");

      // Extract CSRF - Support both orders (name...value OR value...name) and quotes
      // Method 1: name="nonce" ... value="..."
      let csrfMatch = loginPageText.match(/name=["']nonce["'][\s\S]*?value=["']([a-zA-Z0-9]+)["']/);
      // Method 2: value="..." ... name="nonce"
      if (!csrfMatch) {
        csrfMatch = loginPageText.match(/value=["']([a-zA-Z0-9]+)["'][\s\S]*?name=["']nonce["']/);
      }

      const csrfNonce = csrfMatch ? csrfMatch[1] : null;

      if (!csrfNonce) {
        // debug: logs snippet
        console.log("Failed to find nonce. Snippet:", loginPageText.substring(0, 500));
        return { success: false, error: "Gagal mengambil CSRF Token. (Mungkin Cloudflare protected atau regex tidak cocok)" };
      }

      // Capture initial cookies (session)
      const initialCookies = loginPageRes.headers.get("set-cookie");

      // 2. Perform POST /login
      const formData = new URLSearchParams();
      formData.append("name", username);
      formData.append("password", password);
      formData.append("nonce", csrfNonce);
      formData.append("_submit", "Submit");

      const postHeaders = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        "Content-Type": "application/x-www-form-urlencoded",
        "Referer": `${url}/login`,
        "Origin": url
      };
      if (initialCookies) postHeaders["Cookie"] = initialCookies;

      console.log("Posting login credentials...");
      const loginPostRes = await fetch(`${url}/login`, {
        method: "POST",
        headers: postHeaders,
        body: formData,
        redirect: "manual"
      });

      console.log(`Login POST Status: ${loginPostRes.status}`);

      if (loginPostRes.status === 302 || loginPostRes.status === 200) {

        let cookieHeaderVal = "";

        // Try modern API first
        if (typeof loginPostRes.headers.getSetCookie === 'function') {
          const cookies = loginPostRes.headers.getSetCookie();
          if (cookies && cookies.length > 0) {
            cookieHeaderVal = cookies.map(c => c.split(';')[0].trim()).join('; ');
          }
        }

        // Fallback
        if (!cookieHeaderVal) {
          const rawSetCookie = loginPostRes.headers.get("set-cookie");
          if (rawSetCookie) {
            // Heuristic split
            const rawCookies = rawSetCookie.split(/, (?=[a-zA-Z0-9%!#$%&'*+.^_`|~-]+=)/);
            cookieHeaderVal = rawCookies.map(c => c.split(';')[0].trim()).join('; ');
          }
        }

        console.log("Parsed Session Cookie:", cookieHeaderVal);

        if (cookieHeaderVal && cookieHeaderVal.includes("session=")) {
          return { success: true, cookie: cookieHeaderVal };
        }

        return { success: false, error: "Login berhasil tapi gagal mengambil Session Cookie. (Mungkin format cookie aneh)" };
      }

      return { success: false, error: `Gagal Login. Status Code: ${loginPostRes.status}` };

    } catch (e) {
      console.error("Login Exception:", e);
      return { success: false, error: `Exception: ${e.message}` };
    }
  },

  async checkMembership(token, channel, userId) {
    try {
      const url = `https://api.telegram.org/bot${token}/getChatMember?chat_id=${channel}&user_id=${userId}`;
      const response = await fetch(url);
      const data = await response.json();

      console.log(`Membership check for ${userId} in ${channel}:`, data);

      if (data.ok && data.result) {
        const status = data.result.status;
        // Allowed statuses: "creator", "administrator", "member", "restricted" (if check permissions)
        // "left", "kicked" are not allowed.
        return ["creator", "administrator", "member", "restricted"].includes(status);
      }
      return false;
    } catch (e) {
      console.error("Error checking membership:", e);
      // Fail open or closed? Safe to fail closed (deny access) to avoid exploits, 
      // but strictly speaking we should probably return false if we can't verify.
      return false;
    }
  },

  async sendMessage(token, chatId, text, isMarkdown = false) {
    if (!token) return;
    const url = `https://api.telegram.org/bot${token}/sendMessage`;
    const body = {
      chat_id: chatId,
      text: text,
    };
    if (isMarkdown) {
      body.parse_mode = (typeof isMarkdown === 'string') ? isMarkdown : "Markdown";
    }

    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error(`SendMessage Error: ${res.status} - ${errText}`);
      throw new Error(`Telegram Send failed: ${res.status} - ${errText}`);
    }
  },

  async sendMessageWithButtons(token, chatId, text, buttons, parseMode = "HTML") {
    if (!token) return;
    const url = `https://api.telegram.org/bot${token}/sendMessage`;

    // Format buttons as inline keyboard
    // buttons = [[{text: "Button 1", url: "https://..."}, {text: "Button 2", url: "..."}], [...row2...]]
    const body = {
      chat_id: chatId,
      text: text,
      parse_mode: parseMode,
      reply_markup: {
        inline_keyboard: buttons
      }
    };

    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(body),
    });

    if (!res.ok) {
      const errText = await res.text();
      console.error(`SendMessageWithButtons Error: ${res.status} - ${errText}`);
      throw new Error(`Telegram Send failed: ${res.status} - ${errText}`);
    }

    return await res.json();
  },

  async getStoredEvents(env, filter = null) {
    try {
      const eStored = await env.CTFD_STORE.get("EVENTS");
      if (!eStored) return "📂 Belum ada event CTF yang tersimpan.";

      const events = JSON.parse(eStored);
      if (events.length === 0) return "📂 Belum ada event CTF yang tersimpan.";

      let msg = "📅 <b>List Event CTF (Manual)</b>\n\n";
      let count = 0;

      // Filter Logic
      // filter 'archived' -> Show OLD only
      // filter 'all' -> Show ALL
      // filter null or other -> Show ACTIVE only

      const showArchivedOnly = (filter === 'archived');
      const showAll = (filter === 'all');

      events.forEach(e => {
        const isArchived = !!e.archived;

        if (showArchivedOnly && !isArchived) return;
        if (!showArchivedOnly && !showAll && isArchived) return;

        const status = isArchived ? "🔒 <b>Archived</b>" : "🟢 <b>Active</b>";
        const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

        msg += `🚩 <b>${escapeHtml(e.name)}</b>\n`;
        msg += `   🆔 <code>${e.id}</code>\n`;
        msg += `   🌐 ${escapeHtml(e.url)}\n`;

        if (e.start) {
          const startTime = new Date(e.start).getTime();
          const now = Date.now();
          const diff = startTime - now;

          if (diff > 0) {
            const days = Math.floor(diff / (1000 * 60 * 60 * 24));
            const hours = Math.floor((diff % (1000 * 60 * 60 * 24)) / (1000 * 60 * 60));
            const minutes = Math.floor((diff % (1000 * 60 * 60)) / (1000 * 60));
            msg += `   ⏳ <b>Starts:</b> ${days}d ${hours}h ${minutes}m\n`;
          } else {
            msg += `   🚀 <b>Status:</b> Running\n`;
          }
        } else {
          msg += `   ${status}\n`;
        }
        msg += `\n`;
        count++;
      });

      if (count === 0) {
        if (showArchivedOnly) return "📂 Tidak ada event archived.";
        return "📂 Tidak ada event aktif saat ini.";
      }

      msg += "💡 <i>Gunakan /join_event &lt;id&gt; untuk masuk.</i>";
      return msg;

    } catch (e) {
      console.error("List Events Error:", e);
      return "❌ Gagal mengambil data event.";
    }
  },

  async getCTFTimeEvents(filter = null) {
    try {
      const now = new Date();
      // Start: 5 days ago (to include currently running)
      // End: 14 days future
      const startTimestamp = Math.floor((now.getTime() - (5 * 24 * 60 * 60 * 1000)) / 1000);
      const endTimestamp = Math.floor((now.getTime() + (14 * 24 * 60 * 60 * 1000)) / 1000);

      const url = `https://ctftime.org/api/v1/events/?limit=50&start=${startTimestamp}&finish=${endTimestamp}`;

      const response = await fetch(url, {
        headers: { "User-Agent": "TelegramBot/1.0" }
      });

      if (!response.ok) {
        return "⚠️ Gagal mengambil data dari CTFtime.";
      }

      const data = await response.json();
      if (!data || data.length === 0) {
        return "Tidak ada event CTF dalam waktu dekat.";
      }

      const running = [];
      const upcoming = [];

      data.forEach(event => {
        const start = new Date(event.start);
        const end = new Date(event.finish);

        if (start <= now && now <= end) {
          running.push(event);
        } else if (start > now) {
          upcoming.push(event);
        }
      });

      let message = "";
      const formatDate = (dateStr) => {
        const d = new Date(dateStr);
        const options = { timeZone: "Asia/Jakarta", day: 'numeric', month: 'short', hour: '2-digit', minute: '2-digit', hour12: false };
        // Manual construction to ensure "DD MMM, HH.mm" format regardless of locale implementation
        const parts = new Intl.DateTimeFormat('id-ID', options).formatToParts(d);
        const day = parts.find(p => p.type === 'day').value;
        const month = parts.find(p => p.type === 'month').value;
        const hour = parts.find(p => p.type === 'hour').value;
        const minute = parts.find(p => p.type === 'minute').value;
        return `${day} ${month}, ${hour}.${minute}`;
      };

      const showRunning = !filter || filter === 'running';
      const showUpcoming = !filter || filter === 'upcoming';

      const escapeHtml = (str) => String(str || "").replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;");

      if (showRunning && running.length > 0) {
        message += "🔥 <b>SEDANG BERJALAN</b>\n";
        running.forEach((event) => {
          message += `• <a href="${event.url}">${escapeHtml(event.title)}</a>\n`;
          message += `   🏁 Selesai: ${formatDate(event.finish)}\n\n`;
        });
      } else if (showRunning && running.length === 0 && filter === 'running') {
        message += "Tidak ada event yang sedang berjalan.\n";
      }

      if (showUpcoming && upcoming.length > 0) {
        message += "⏳ <b>AKAN DATANG</b>\n";
        upcoming.slice(0, 5).forEach((event) => {
          message += `• <a href="${event.url}">${escapeHtml(event.title)}</a>\n`;
          message += `   🟢 Mulai: ${formatDate(event.start)}\n`;
          message += `   🏁 Selesai: ${formatDate(event.finish)}\n\n`;
        });
      } else if (showUpcoming && upcoming.length === 0 && filter === 'upcoming') {
        message += "Tidak ada event mendatang (2 minggu).\n";
      }

      if (message === "") {
        message = "Tidak ada info event CTFtime saat ini.";
      } else {
        message += "\nSumber: <a href=\"https://ctftime.org\">CTFtime.org</a>";
      }

      return message;

    } catch (e) {
      console.error("CTFTime Error:", e);
      return "⚠️ Terjadi kesalahan saat menghubungi CTFtime.";
    }
  },



  async processInitChallenges(env, chatId, event, mySub, startOffset = 0) {
    try {
      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `🚀 **Start Initialization:** ${event.name}\n⏳ Mengambil daftar challenge...`, true);

      // 2. Fetch List
      const headers = { "Content-Type": "application/json", "User-Agent": "TelegramBot/1.0" };
      if (mySub.credentials.mode === 'token') headers["Authorization"] = `Token ${mySub.credentials.value}`;
      else headers["Cookie"] = mySub.credentials.value;

      let challList = [];
      try {
        const res = await fetch(`${event.url}/api/v1/challenges`, { headers });
        if (!res.ok) throw new Error(`HTTP ${res.status}`);
        const json = await res.json();
        if (!json.success) throw new Error("API success=false");
        challList = json.data || [];
      } catch (e) {
        await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Gagal ambil list: ${e.message}`, true);
        return;
      }

      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `📋 **Found: ${challList.length} Challenges.**\n🔽 Fetching details...`, true);

      // 3. Fetch Details (Batching)
      let fullData = [];
      const STORAGE_KEY = `CHALLENGES_${event.id}`;

      // Resume: Load existing if offset > 0
      if (startOffset > 0) {
        try {
          const stored = await env.CTFD_STORE.get(STORAGE_KEY);
          if (stored) fullData = JSON.parse(stored);
        } catch (e) { }
      }

      const BATCH_SIZE = 1; // Sequential for stability
      const START_TIME = Date.now();
      const TIME_LIMIT = 7000; // 7 Seconds (Extremely Safe)

      let complete = true;
      let nextOffset = 0;

      for (let i = startOffset; i < challList.length; i += BATCH_SIZE) {
        // Time Limit Check
        if (Date.now() - START_TIME > TIME_LIMIT) {
          complete = false;
          nextOffset = i;
          break;
        }
        const batch = challList.slice(i, i + BATCH_SIZE);
        await Promise.all(batch.map(async (c) => {
          try {
            const controller = new AbortController();
            const timeoutId = setTimeout(() => controller.abort(), 4000);

            const dRes = await fetch(`${event.url}/api/v1/challenges/${c.id}`, {
              headers,
              signal: controller.signal
            });
            clearTimeout(timeoutId);

            if (dRes.ok) {
              const dJson = await dRes.json();
              if (dJson.success && dJson.data) {
                // UPDATE: User requested to keep file info
                if (dJson.data.files) {
                  // Keep the files array (it's small, just URLs)
                  // Add a summary field
                  dJson.data.file_info = `${dJson.data.files.length} File(s)`;
                } else {
                  dJson.data.file_info = "No Files";
                }

                fullData.push(dJson.data);
              }
            }
          } catch (e) { console.error(`Failed chal ${c.id}: ${e.name === 'AbortError' ? 'Timeout' : e.message}`); }
        }));

        // Progress Update every 5 (Avoid Rate Limit)
        if ((i + BATCH_SIZE) % 5 === 0 || i + BATCH_SIZE >= challList.length) {
          await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⏳ Progress: ${Math.min(i + BATCH_SIZE, challList.length)} / ${challList.length}`, true);
        }
      }


      // 4. Save
      await env.CTFD_STORE.put(STORAGE_KEY, JSON.stringify(fullData));

      // Wait 2 seconds to avoid Telegram Rate Limit (429) after the last progress update
      await new Promise(r => setTimeout(r, 2000));

      if (complete) {
        await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `✅ **Update/Init Complete!**\n\n📚 Saved/Updated: ${fullData.length} challenges.\n`, true);
      } else {
        await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `⚠️ **Time Limit Reached!**\n\nProgress: ${fullData.length} / ${challList.length}\nData disimpan sebagian.\n\n👇 **Klik untuk Lanjut (Resume):**\n/continue_init ${event.id} ${nextOffset}`, true);
      }

    } catch (e) {
      console.error("BG Init Error", e);
      await this.sendMessage(env.TELEGRAM_BOT_TOKEN, chatId, `❌ Error during background init: ${e.message}`, true);
    }
  },

  async updateLeaderboard(env, eventId, userId, telegramName, ctfdName, score, solveCount = 0) {
    try {
      if (!eventId) return;
      let lb = {};
      const key = "TELEGRAM_LEADERBOARD"; // Global Key
      const lbStr = await env.CTFD_STORE.get(key);
      if (lbStr) lb = JSON.parse(lbStr);

      if (!lb[userId]) {
        lb[userId] = {
          telegram_name: telegramName,
          ctfd_name: ctfdName,
          events: {},
          last_update: Date.now()
        };
      }



      // Update specific event score and solve count
      if (!lb[userId].events) lb[userId].events = {};
      if (!lb[userId].solves) lb[userId].solves = {};
      lb[userId].events[eventId] = parseInt(score) || 0;
      lb[userId].solves[eventId] = parseInt(solveCount) || 0;
      lb[userId].telegram_name = telegramName; // update name if changed
      lb[userId].ctfd_name = ctfdName;
      lb[userId].last_update = Date.now();

      await env.CTFD_STORE.put(key, JSON.stringify(lb));
    } catch (e) {
      console.error("Leaderboard update failed", e);
    }
  }
};
