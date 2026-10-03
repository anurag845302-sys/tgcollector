import asyncio
import json
import os
import random
import sqlite3
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
    UserIdInvalidError,
    ChannelInvalidError,
)
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"

TARGET_GROUP = "@earnmax321"

ACCOUNTS = [
    {"name": "acc1", "env": "TG_SESSION_1", "daily": 0,  "delay_min": 90, "delay_max": 180},  # paused, PEER_FLOOD
    {"name": "acc2", "env": "TG_SESSION_2", "daily": 10, "delay_min": 90, "delay_max": 180},
    {"name": "acc3", "env": "TG_SESSION_3", "daily": 10, "delay_min": 90, "delay_max": 180},
    {"name": "acc4", "env": "TG_SESSION_4", "daily": 10, "delay_min": 90, "delay_max": 180},
]

DB_FILE = os.environ.get("DB_FILE", "/data/seen.db")
ADD_DB = os.environ.get("ADD_DB", "/data/added.db")
OUT_FILE = os.environ.get("OUT_FILE", "/data/members_fresh.jsonl")

add = sqlite3.connect(ADD_DB, check_same_thread=False)
add.execute("CREATE TABLE IF NOT EXISTS added (user_id INTEGER PRIMARY KEY, ts TEXT, account TEXT)")
add.execute("CREATE TABLE IF NOT EXISTS failed (user_id INTEGER PRIMARY KEY, reason TEXT, ts TEXT, account TEXT)")
add.commit()

def already_added(uid):
    return add.execute("SELECT 1 FROM added WHERE user_id=?", (uid,)).fetchone() is not None

def mark_added(uid, account):
    ts = datetime.now(timezone.utc).isoformat()
    add.execute("INSERT OR IGNORE INTO added VALUES (?,?,?)", (uid, ts, account))
    add.commit()

def mark_failed(uid, reason, account):
    ts = datetime.now(timezone.utc).isoformat()
    add.execute("INSERT OR IGNORE INTO failed VALUES (?,?,?,?)", (uid, reason, ts, account))
    add.commit()

def log(account, msg):
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{ts}] [{account}] {msg}", flush=True)

def load_candidates():
    out = []
    with open(OUT_FILE, "r", encoding="utf-8") as f:
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
    random.shuffle(out)
    return out

async def invite(client, uid, ah, target):
    try:
        peer = InputPeerUser(uid, ah)
        await client(functions.channels.InviteToChannelRequest(channel=target, users=[peer]))
        return ("ok", None)
    except UserIdInvalidError:
        try:
            peer = await client.get_input_entity(uid)
            await client(functions.channels.InviteToChannelRequest(channel=target, users=[peer]))
            return ("ok_fresh", None)
        except Exception as e2:
            return ("fail", type(e2).__name__)
    except UserPrivacyRestrictedError:
        return ("fail", "privacy")
    except UserNotMutualContactError:
        return ("fail", "not_mutual")
    except UserChannelsTooMuchError:
        return ("fail", "too_many_channels")
    except InputUserDeactivatedError:
        return ("fail", "deactivated")
    except UserAlreadyParticipantError:
        return ("ok_fresh", "already")
    except FloodWaitError:
        raise
    except PeerFloodError:
        raise
    except ChatAdminRequiredError:
        raise
    except ChannelInvalidError:
        raise
    except Exception as e:
        return ("fail", type(e).__name__)

async def run_account(cfg, candidates_shared, start_offset):
    name = cfg["name"]
    if cfg["daily"] <= 0:
        log(name, "paused (daily=0), skipping")
        return {"name": name, "added": 0, "failed": 0, "skipped": True}

    session_str = os.environ.get(cfg["env"], "").strip()
    if not session_str:
        log(name, f"SKIP: {cfg['env']} missing")
        return {"name": name, "added": 0, "failed": 0, "skipped": True}

    if start_offset > 0:
        log(name, f"stagger {int(start_offset)}s")
        await asyncio.sleep(start_offset)

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH, timeout=30)
    try:
        await client.start()
        me = await client.get_me()
        log(name, f"logged in {me.id} @{me.username}")
    except Exception as e:
        log(name, f"LOGIN FAILED: {e}")
        return {"name": name, "added": 0, "failed": 0, "skipped": True}

    # RESOLVE TARGET WITH THIS ACCOUNT'S SESSION
    try:
        target = await client.get_entity(TARGET_GROUP)
        log(name, f"target resolved {getattr(target,'id',None)}")
    except Exception as e:
        log(name, f"target resolve FAILED: {type(e).__name__}")
        await client.disconnect()
        return {"name": name, "added": 0, "failed": 0, "skipped": True}

    added = 0
    failed = 0
    cursor = 0
    while added < cfg["daily"] and cursor < len(candidates_shared):
        uid, ah = candidates_shared[cursor]
        cursor += 1
        if already_added(uid):
            continue
        try:
            status, reason = await invite(client, uid, ah, target)
        except FloodWaitError as e:
            log(name, f"[!] FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            continue
        except PeerFloodError:
            log(name, "[!] PEER_FLOOD - stop this account")
            break
        except ChatAdminRequiredError:
            log(name, "[!] ChatAdminRequired - check admin rights")
            break
        except ChannelInvalidError:
            log(name, "[!] ChannelInvalid - session can't access target")
            break

        if status in ("ok", "ok_fresh"):
            added += 1
            mark_added(uid, name)
            log(name, f"[+] {uid} added ({added}/{cfg['daily']}) via {status}")
        else:
            failed += 1
            mark_failed(uid, reason, name)
            if failed % 20 == 0:
                log(name, f"[-] {failed} fails (last: {reason})")

        await asyncio.sleep(random.uniform(cfg["delay_min"], cfg["delay_max"]))

    log(name, f"DONE added={added} failed={failed}")
    try:
        await client.disconnect()
    except Exception:
        pass
    return {"name": name, "added": added, "failed": failed, "skipped": False}

async def main():
    for cfg in ACCOUNTS:
        v = os.environ.get(cfg["env"], "").strip()
        log("boot", f"{cfg['env']}: {'OK len='+str(len(v)) if v else 'MISSING'}")

    candidates = load_candidates()
    log("boot", f"candidates: {len(candidates)}")

    tasks = []
    for i, cfg in enumerate(ACCOUNTS):
        offset = i * random.uniform(300, 900)
        tasks.append(run_account(cfg, candidates, offset))

    results = await asyncio.gather(*tasks, return_exceptions=True)
    for r in results:
        log("boot", f"result: {r}")
    total = add.execute("SELECT COUNT(*) FROM added").fetchone()[0]
    log("boot", f"ALL DONE total_db={total}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        add.close()