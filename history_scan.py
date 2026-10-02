import asyncio
import json
import os
import sqlite3
import sys
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.errors import FloodWaitError
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"

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

# per group kitne purane messages scan karne hain
SCAN_LIMIT = 10000

SESSION_STRING = os.environ.get("TG_SESSION", "")
if not SESSION_STRING:
    raise SystemExit("TG_SESSION env missing")

OUT_FILE = os.environ.get("OUT_FILE", "/data/members.jsonl")
DB_FILE = os.environ.get("DB_FILE", "/data/seen.db")

db = sqlite3.connect(DB_FILE, check_same_thread=False)
db.execute("CREATE TABLE IF NOT EXISTS seen (user_id INTEGER PRIMARY KEY)")
db.commit()

def is_seen(uid):
    return db.execute("SELECT 1 FROM seen WHERE user_id=?", (uid,)).fetchone() is not None

def mark_seen(uid):
    db.execute("INSERT OR IGNORE INTO seen VALUES (?)", (uid,))
    db.commit()

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)
out = open(OUT_FILE, "a", encoding="utf-8", buffering=1)

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

async def scan_group(entity, limit):
    chat_id = getattr(entity, "id", None)
    chat_username = getattr(entity, "username", None)
    new = 0
    seen_in_scan = 0
    try:
        async for msg in client.iter_messages(entity, limit=limit):
            seen_in_scan += 1
            if seen_in_scan % 500 == 0:
                log(f"  ...scanned {seen_in_scan} msgs in {chat_username or chat_id} | new={new}")
            sender = msg.sender
            if sender is None:
                try:
                    sender = await msg.get_sender()
                except Exception:
                    continue
            if sender is None:
                continue
            if getattr(sender, "bot", False):
                continue
            if getattr(sender, "deleted", False):
                continue
            if getattr(sender, "scam", False):
                continue
            if getattr(sender, "fake", False):
                continue
            uid = getattr(sender, "id", None)
            if uid is None:
                continue
            if is_seen(uid):
                continue
            mark_seen(uid)
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
                "msg_id": msg.id,
                "date": msg.date.isoformat() if msg.date else None,
                "source": "history_scan",
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")
            new += 1
            if new % 50 == 0:
                log(f"  +{new} new in {chat_username or chat_id}")
    except FloodWaitError as e:
        log(f"  FloodWait {e.seconds}s on {chat_username or chat_id}, sleeping")
        await asyncio.sleep(e.seconds)
    except Exception as e:
        log(f"  error on {chat_username or chat_id}: {e}")
    return new, seen_in_scan

async def main():
    await client.start()
    me = await client.get_me()
    log(f"Logged in as {me.id} @{me.username}")

    total_new = 0
    for t in TARGETS:
        try:
            ent = await client.get_entity(t)
        except Exception as e:
            log(f"resolve failed {t}: {e}")
            continue
        log(f"scanning {t} (limit={SCAN_LIMIT})...")
        n, s = await scan_group(ent, SCAN_LIMIT)
        total_new += n
        log(f"done {t}: scanned={s} new={n} total_new={total_new}")

    total_all = db.execute("SELECT COUNT(*) FROM seen").fetchone()[0]
    log(f"SCAN COMPLETE. new_this_run={total_new} total_db={total_all}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        out.close()
        db.close()
