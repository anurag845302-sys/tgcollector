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
    UserIdInvalidError,
)
from telethon.sessions import StringSession
from telethon.tl.types import InputPeerUser

API_ID = 35377971
API_HASH = "8f69b0e7086b2ece81bab51b5649df8a"

TARGET_GROUP = "@earnmax321"

ACCOUNTS = [
    {"name": "acc1", "env": "TG_SESSION_1", "daily": 10, "delay_min": 90, "delay_max": 180},
    {"name": "acc2", "env": "TG_SESSION_2", "daily": 10, "delay_min": 90, "delay_max": 180},
    {"name": "acc3", "env": "TG_SESSION_3", "daily": 10, "delay_min": 90, "delay_max": 180},
    {"name": "acc4", "env": "TG_SESSION_4", "daily": 10, "delay_min": 90, "delay_max": 180},
]

# source groups for fresh hash fallback
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
            if uid is None:
                continue
            if already_added(uid):
                continue
            out.append((uid, ah))
    random.shuffle(out)
    return out

async def invite_with_fallback(client, uid, ah, target):
    """
    Try add with stored hash. If UserIdInvalid, fetch fresh via get_input_entity.
    Returns: ('ok', None) or ('ok_fresh', None) or ('fail', 'reason')
    """
    if ah is not None:
        try:
            peer = InputPeerUser(uid, ah)
            await client(functions.channels.InviteToChannelRequest(channel=target, users=[peer]))
            return ("ok", None)
        except UserIdInvalidError:
            pass  # fall through
        except FloodWaitError:
            raise
        except PeerFloodError:
            raise
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
        except ChatAdminRequiredError:
            raise
        except Exception as e:
            return ("fail", type(e).__name__)

    # fresh fetch
    try:
        peer = await client.get_input_entity(uid)
        await client(functions.channels.InviteToChannelRequest(channel=target, users=[peer]))
        return ("ok_fresh", None)
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
    except Exception as e:
        return ("fail", type(e).__name__)


async def run_account(cfg, candidates_shared, target, start_offset):
    name = cfg["name"]
    session_str = os.environ.get(cfg["env"], "").strip()
    if not session_str:
        log(name, f"SKIP: {cfg['env']} missing")
        return {"name": name, "added": 0, "failed": 0, "skipped": True}

    if start_offset > 0:
        log(name, f"stagger start {int(start_offset)}s")
        await asyncio.sleep(start_offset)

    client = TelegramClient(StringSession(session_str), API_ID, API_HASH)
    try:
        await client.start()
        me = await client.get_me()
        log(name, f"logged in {me.id} @{me.username}")
    except Exception as e:
        log(name, f"LOGIN FAILED: {e}")
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
            status, reason = await invite_with_fallback(client, uid, ah, target)
        except FloodWaitError as e:
            log(name, f"[!] FloodWait {e.seconds}s")
            await asyncio.sleep(e.seconds)
            continue
        except PeerFloodError:
            log(name, "[!] PEER_FLOOD - stop this account")
            break
        except ChatAdminRequiredError:
            log(name, "[!] admin required - make this account admin in @earnmax321")
            break

        if status in ("ok", "ok_fresh"):
            added += 1
            mark_added(uid, name)
            log(name, f"[+] {uid} added ({added}/{cfg['daily']}) via {status}")
        else:
            failed += 1
            mark_failed(uid, reason, name)
            if failed % 10 == 0:
                log(name, f"[-] {failed} fails so far (last: {reason})")

        d = random.uniform(cfg["delay_min"], cfg["delay_max"])
        await asyncio.sleep(d)

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

    first = os.environ.get(ACCOUNTS[0]["env"], "").strip()
    if not first:
        log("boot", "FATAL first session missing")
        return

    boot = TelegramClient(StringSession(first), API_ID, API_HASH)
    await boot.start()
    try:
        target = await boot.get_entity(TARGET_GROUP)
        log("boot", f"target {getattr(target,'id',None)} megagroup={getattr(target,'megagroup',None)}")
    except Exception as e:
        log("boot", f"target fail {e}")
        await boot.disconnect()
        return
    await boot.disconnect()

    candidates = load_candidates()
    log("boot", f"candidates: {len(candidates)}")

    tasks = []
    for i, cfg in enumerate(ACCOUNTS):
        offset = i * random.uniform(300, 900)
        tasks.append(run_account(cfg, candidates, target, offset))

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