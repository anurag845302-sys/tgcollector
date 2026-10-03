import asyncio
import json
import os
import random
import sqlite3
import sys
from datetime import datetime, timezone
from telethon import TelegramClient, functions
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserNotMutualContactError,
    UserChannelsTooMuchError,
    InputUserDeactivatedError,
    UserAlreadyParticipantError,
    ChatAdminRequiredError,
)
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"

TARGET_GROUP = "@earnmax321"
DAILY_LIMIT = 20
MIN_DELAY = 90
MAX_DELAY = 180

SESSION_STRING = os.environ.get("TG_SESSION", "")
if not SESSION_STRING:
    raise SystemExit("TG_SESSION env missing")

DB_FILE = os.environ.get("DB_FILE", "/data/seen.db")
ADD_DB = os.environ.get("ADD_DB", "/data/added.db")

add = sqlite3.connect(ADD_DB)
add.execute("CREATE TABLE IF NOT EXISTS added (user_id INTEGER PRIMARY KEY, ts TEXT)")
add.execute("CREATE TABLE IF NOT EXISTS failed (user_id INTEGER PRIMARY KEY, reason TEXT, ts TEXT)")
add.commit()

def already_added(uid):
    return add.execute("SELECT 1 FROM added WHERE user_id=?", (uid,)).fetchone() is not None

def mark_added(uid):
    ts = datetime.now(timezone.utc).isoformat()
    add.execute("INSERT OR IGNORE INTO added VALUES (?,?)", (uid, ts))
    add.commit()

def mark_failed(uid, reason):
    ts = datetime.now(timezone.utc).isoformat()
    add.execute("INSERT OR IGNORE INTO failed VALUES (?,?,?)", (uid, reason, ts))
    add.commit()

client = TelegramClient(StringSession(SESSION_STRING), API_ID, API_HASH)

def log(msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def load_candidates():
    path = os.environ.get("OUT_FILE", "/data/members.jsonl")
    out = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                rec = json.loads(line)
            except Exception:
                continue
            uid = rec.get("id")
            ah = rec.get("access_hash")
            if uid is None or ah is None:
                continue
            if already_added(uid):
                continue
            out.append((uid, ah))
    return out

async def add_one(uid, access_hash, target):
    peer = InputPeerUser(uid, access_hash)
    await client(functions.channels.InviteToChannelRequest(
        channel=target,
        users=[peer]
    ))

async def main():
    await client.start()
    me = await client.get_me()
    log(f"Logged in as {me.id} @{me.username}")

    try:
        target = await client.get_entity(TARGET_GROUP)
        log(f"target resolved: {TARGET_GROUP} -> {getattr(target,'id',None)} megagroup={getattr(target,'megagroup',None)}")
    except Exception as e:
        log(f"target resolve failed: {e}")
        return

    candidates = load_candidates()
    log(f"candidates loaded: {len(candidates)}")

    random.shuffle(candidates)

    added = 0
    failed = 0
    for uid, ah in candidates:
        if added >= DAILY_LIMIT:
            log(f"daily limit reached ({DAILY_LIMIT})")
            break
        try:
            await add_one(uid, ah, target)
            added += 1
            mark_added(uid)
            log(f"[+] {uid} added ({added}/{DAILY_LIMIT})")
        except FloodWaitError as e:
            log(f"[!] FloodWait {e.seconds}s - sleeping")
            await asyncio.sleep(e.seconds)
            continue
        except PeerFloodError:
            log("[!] PEER_FLOOD - account flagged. stopping.")
            break
        except UserPrivacyRestrictedError:
            failed += 1
            mark_failed(uid, "privacy")
        except UserNotMutualContactError:
            failed += 1
            mark_failed(uid, "not_mutual")
        except UserChannelsTooMuchError:
            failed += 1
            mark_failed(uid, "too_many_channels")
        except InputUserDeactivatedError:
            failed += 1
            mark_failed(uid, "deactivated")
        except UserAlreadyParticipantError:
            failed += 1
            mark_added(uid)
        except ChatAdminRequiredError:
            log("[!] admin required - check your permissions in target group")
            break
        except Exception as e:
            failed += 1
            mark_failed(uid, type(e).__name__)
            log(f"[x] {uid} {type(e).__name__}")

        d = random.uniform(MIN_DELAY, MAX_DELAY)
        await asyncio.sleep(d)

    total_added = add.execute("SELECT COUNT(*) FROM added").fetchone()[0]
    log(f"DONE this_run added={added} failed={failed} total_added_db={total_added}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        add.close()
