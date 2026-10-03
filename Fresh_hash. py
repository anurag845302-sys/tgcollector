import asyncio
import json
import os
from datetime import datetime, timezone
from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import FloodWaitError

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"

SOURCE_GROUPS = [
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

# per group kitne messages scan
MSGS_PER_GROUP = 2000

SESSION = os.environ.get("TG_SESSION_1", "")
if not SESSION:
    raise SystemExit("TG_SESSION_1 missing")

OUT = "/data/members_fresh.jsonl"

client = TelegramClient(StringSession(SESSION), API_ID, API_HASH, timeout=30)

def log(m):
    ts = datetime.now(timezone.utc).strftime("%H:%M:%S")
    print(f"[{ts}] {m}", flush=True)

async def main():
    await client.start()
    me = await client.get_me()
    log(f"logged in {me.id}")

    seen = set()
    f = open(OUT, "a", encoding="utf-8")
    total = 0

    for t in SOURCE_GROUPS:
        try:
            ent = await client.get_entity(t)
        except Exception as e:
            log(f"skip {t}: {type(e).__name__}")
            continue

        count = 0
        new = 0
        try:
            async for msg in client.iter_messages(ent, limit=MSGS_PER_GROUP):
                count += 1
                if count % 500 == 0:
                    log(f"  {t}: scanned {count} new={new}")
                s = msg.sender
                if s is None:
                    try:
                        s = await msg.get_sender()
                    except Exception:
                        continue
                if s is None:
                    continue
                if getattr(s, "bot", False) or getattr(s, "deleted", False):
                    continue
                uid = getattr(s, "id", None)
                ah = getattr(s, "access_hash", None)
                if uid is None or ah is None:
                    continue
                if uid in seen:
                    continue
                seen.add(uid)
                rec = {
                    "id": uid,
                    "access_hash": ah,
                    "username": getattr(s, "username", None),
                    "first_name": getattr(s, "first_name", None),
                    "last_name": getattr(s, "last_name", None),
                    "premium": bool(getattr(s, "premium", False)),
                    "status": type(getattr(s, "status", None)).__name__ if getattr(s, "status", None) else None,
                    "source_chat_username": getattr(ent, "username", None),
                    "source_chat_id": getattr(ent, "id", None),
                    "date": msg.date.isoformat() if msg.date else None,
                    "source": "fresh_hash",
                }
                f.write(json.dumps(rec, ensure_ascii=False) + "\n")
                new += 1
                total += 1
        except FloodWaitError as e:
            log(f"  FloodWait {e.seconds}s on {t}")
            await asyncio.sleep(e.seconds)
        except Exception as e:
            log(f"  error {t}: {type(e).__name__}: {e}")

        log(f"done {t}: scanned={count} new={new} total={total}")

    f.close()
    log(f"ALL DONE total={total}")
    await client.disconnect()

if __name__ == "__main__":
    asyncio.run(main())