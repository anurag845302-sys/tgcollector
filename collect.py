import asyncio
import json
import sqlite3
import sys
from datetime import datetime
from telethon import TelegramClient, events
from telethon.errors import FloodWaitError

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"
import os
from telethon.sessions import StringSession

SESSION_STRING = os.environ.get("TG_SESSION", "")
if not SESSION_STRING:
    raise SystemExit("TG_SESSION env var missing")

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

TARGETS = [
    "WynnPaySupport",
    "onepaytop1",
    "howtoregister3",
    "howtoregister1",
    "ddpayw",
    "wecoinpaygroup",
    -1003725882638,
    -1003743509152,
    -1003988495633,
]

OUT_FILE = os.environ.get("OUT_FILE", "/tmp/members.jsonl")
DB_FILE = os.environ.get("DB_FILE", "/tmp/seen.db")

db = sqlite3.connect(DB_FILE)
db.execute("CREATE TABLE IF NOT EXISTS seen (user_id INTEGER PRIMARY KEY)")
db.commit()

def is_seen(uid):
    cur = db.execute("SELECT 1 FROM seen WHERE user_id=?", (uid,))
    return cur.fetchone() is not None

def mark_seen(uid):
    db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (uid,))
    db.commit()

client = TelegramClient(SESSION, API_ID, API_HASH)
out = open(OUT_FILE, "a", encoding="utf-8", buffering=1)

def log(msg):
    ts = datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

@client.on(events.NewMessage(chats=TARGETS))
async def handler(event):
    try:
        sender = await event.get_sender()
    except FloodWaitError as e:
        log(f"FloodWait on get_sender: {e.seconds}s")
        await asyncio.sleep(e.seconds)
        return
    except Exception as e:
        log(f"get_sender failed: {e}")
        return

    if sender is None:
        return
    if getattr(sender, "bot", False):
        return
    if getattr(sender, "deleted", False):
        return
    if getattr(sender, "scam", False):
        return
    if getattr(sender, "fake", False):
        return
    uid = sender.id
    if uid is None:
        return
    if is_seen(uid):
        return
    mark_seen(uid)

    chat = await event.get_chat()
    chat_id = getattr(chat, "id", None)
    chat_username = getattr(chat, "username", None)

    rec = {
        "id": uid,
        "access_hash": getattr(sender, "access_hash", None),
        "username": getattr(sender, "username", None),
        "first_name": getattr(sender, "first_name", None),
        "last_name": getattr(sender, "last_name", None),
        "phone": getattr(sender, "phone", None),
        "premium": bool(getattr(sender, "premium", False)),
        "status": type(getattr(sender, "status", None)).__name__ if getattr(sender, "status", None) else None,
        "source_chat_id": chat_id,
        "source_chat_username": chat_username,
        "msg_id": event.message.id,
        "date": event.message.date.isoformat() if event.message.date else None,
    }
    out.write(json.dumps(rec, ensure_ascii=False) + "\n")

    total = db.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    log(f"+ {uid} @{rec['username']} from {chat_username or chat_id} | total={total}")

async def main():
    await client.start()
    me = await client.get_me()
    log(f"Logged in as {me.id} @{me.username}")

    resolved = []
    for t in TARGETS:
        try:
            ent = await client.get_entity(t)
            resolved.append(ent)
            log(f"resolved: {t} -> {getattr(ent,'id',None)}")
        except Exception as e:
            log(f"resolve failed {t}: {e}")

    client.remove_event_handler(handler)
    client.add_event_handler(handler, events.NewMessage(chats=resolved))

    log(f"Listening on {len(resolved)} chats. Output: {OUT_FILE}")
    await client.run_until_disconnected()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        log("stopped by user")
        out.close()
        db.close()
        sys.exit(0)
