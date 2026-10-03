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

# Per account config: env var name, daily limit, delay min, delay max
ACCOUNTS = [
    {"name": "acc1", "env": "TG_SESSION_1", "daily": 20, "delay_min": 90, "delay_max": 180},
    {"name": "acc2", "env": "TG_SESSION_2", "daily": 20, "delay_min": 90, "delay_max": 180},
    {"name": "acc3", "env": "TG_SESSION_3", "daily": 20, "delay_min": 90, "delay_max": 180},
]

DB_FILE = os.environ.get("DB_FILE", "/data/seen.db")
ADD_DB = os.environ.get("ADD_DB", "/data/added.db")

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
    random.shuffle(out)
    return out

async def run_account(cfg, candidates_shared, target):
    name = cfg["name"]
    session_str = os.environ.get(cfg["env"], "")
    if not session_str:
        log(name, f"no session in {cfg['env']}, skipping")
        return 0

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    await client.start()
    me = await client.get_me()
    log(name, f"logged in as {me.id} @{me.username}")

    added = 0
    failed = 0
    cursor = 0
    while added < cfg["daily"] and cursor < len(candidates_shared):
        uid, ah = candidates_shared[cursor]
        cursor += 1
        if already_added(uid):
            continue
        try:
            peer = InputPeerUser(uid, ah)
            await client(functions.channels.InviteToChannelRequest(
                channel=target, users=[peer]
            ))
            added += 1
            mark_added(uid, name)
            log(name, f"[+] {uid} added ({added}/{cfg['daily']})")
        except FloodWaitError as e:
            log(name, f"[!] FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            continue
        except PeerFloodError:
            log(name, "[!] PEER_FLOOD - stopping this account")
            break
        except UserPrivacyRestrictedError:
            failed += 1
            mark_failed(uid, "privacy", name)
        except UserNotMutualContactError:
            failed += 1
            mark_failed(uid, "not_mutual", name)
        except UserChannelsTooMuchError:
            failed += 1
            mark_failed(uid, "too_many_channels", name)
        except InputUserDeactivatedError:
            failed += 1
            mark_failed(uid, "deactivated", name)
        except UserAlreadyParticipantError:
            failed += 1
            mark_added(uid, name)
        except ChatAdminRequiredError:
            log(name, "[!] admin required - check permissions")
            break
        except Exception as e:
            failed += 1
            mark_failed(uid, type(e).__name__, name)
            log(name, f"[x] {uid} {type(e).__name__}")

        d = random.uniform(cfg["delay_min"], cfg["delay_max"])
        await asyncio.sleep(d)

    log(name, f"DONE added={added} failed={failed}")
    await client.disconnect()
    return added

async def main():
    # resolve target with first account
    first = os.environ.get(ACCOUNTS[0]["env"], "")
    if not first:
        print("no first account session")
        return
    boot = TelegramClient(StringSession(first), API_ID, API_HASH)
    await boot.start()
    target = await boot.get_entity(TARGET_GROUP)
    log("boot", f"target resolved: {getattr(target,'id',None)} megagroup={getattr(target,'megagroup',None)}")
    await boot.disconnect()

    candidates = load_candidates()
    log("boot", f"candidates: {len(candidates)}")

    tasks = []
    for cfg in ACCOUNTS:
        tasks.append(run_account(cfg, candidates, target))
        # stagger start 5-15 min
        await asyncio.sleep(random.uniform(300, 900))

    results = await asyncio.gather(*tasks)
    total = add.execute("SELECT COUNT(*) FROM added").fetchone()[0]
    log("boot", f"ALL DONE this_run={sum(results)} total_db={total}")

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
    finally:
        add.close()
