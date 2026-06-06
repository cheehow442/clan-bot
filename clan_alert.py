import json
import os
import time
import requests
import ctypes
import sys
from datetime import datetime
from zoneinfo import ZoneInfo

CLASH_API_TOKEN = os.getenv("CLASH_API_TOKEN")
CLAN_TAG = "%23RPYC8P2Y"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CHECK_EVERY_SECONDS = 10
HOURLY_SUMMARY_SECONDS = 14400
SG_RANK_CACHE_SECONDS = 600
LEADERSHIP_REPORT_SECONDS = 604800  # every 7 days
POL_MAINTENANCE_GRACE_SECONDS = 1800  # 30 mins

BOT_TIMEZONE = ZoneInfo("Asia/Singapore")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

UC_ALERT_STATE_FILE = os.path.join(DATA_DIR, "uc_alert_state.json")
STATE_FILE = os.path.join(DATA_DIR, "clan_members_state.json")
MESSAGE_STATE_FILE = os.path.join(DATA_DIR, "telegram_message_state.json")
HOURLY_SNAPSHOT_FILE = os.path.join(DATA_DIR, "hourly_trophy_snapshot.json")
WAR_STATE_FILE = os.path.join(DATA_DIR, "war_state.json")
LOCATION_CACHE_FILE = os.path.join(DATA_DIR, "location_cache.json")
SG_RANK_CACHE_FILE = os.path.join(DATA_DIR, "sg_rank_cache.json")
MEMBER_HISTORY_FILE = os.path.join(DATA_DIR, "member_history.json")
LEADERSHIP_REPORT_STATE_FILE = os.path.join(DATA_DIR, "leadership_report_state.json")

CLASH_HEADERS = {"Authorization": f"Bearer {CLASH_API_TOKEN}"}

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001

_instance_lock_file = None


def log(message):
    now_text = datetime.now(BOT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{now_text}] {message}", flush=True)


def atomic_json_save(filepath, data):
    temp_path = f"{filepath}.tmp"
    with open(temp_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    os.replace(temp_path, filepath)


def load_json_file(filepath):
    if not os.path.exists(filepath):
        return None

    try:
        with open(filepath, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to load JSON file {filepath}: {e}")
        return None


def ensure_single_instance():
    global _instance_lock_file

    if os.name == "nt":
        mutex_name = "KopiOClanBotSingleInstance"

        kernel32 = ctypes.windll.kernel32
        kernel32.CreateMutexW(None, False, mutex_name)

        ERROR_ALREADY_EXISTS = 183
        if kernel32.GetLastError() == ERROR_ALREADY_EXISTS:
            log("Another instance is already running. Exiting...")
            sys.exit(0)

        return True

    lock_path = "/tmp/KopiOClanBotSingleInstance.lock"

    try:
        import fcntl
        _instance_lock_file = open(lock_path, "w")
        fcntl.flock(_instance_lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
        _instance_lock_file.write(str(os.getpid()))
        _instance_lock_file.flush()
        return _instance_lock_file
    except BlockingIOError:
        log("Another instance is already running. Exiting...")
        sys.exit(0)
    except Exception as e:
        log(f"Single-instance lock failed: {e}")
        return None


def keep_awake():
    if os.name != "nt":
        return

    try:
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED
        )
    except Exception as e:
        log(f"Keep-awake failed: {e}")


def format_sgt_timestamp(ts):
    return datetime.fromtimestamp(ts, BOT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")


def clash_get(url, retries=3, delay_seconds=3):
    for attempt in range(1, retries + 1):
        try:
            r = requests.get(url, headers=CLASH_HEADERS, timeout=20)
        except requests.RequestException as e:
            log(f"Clash API request error (attempt {attempt}/{retries}): {e}")
            if attempt < retries:
                time.sleep(delay_seconds)
                continue
            return None

        if r.status_code != 200:
            log(f"Error fetching Clash API: {r.status_code} {r.text}")
            return None

        try:
            return r.json()
        except ValueError:
            log("Failed to parse Clash API response as JSON.")
            return None

    return None


def get_player_details(player_tag):
    encoded_tag = player_tag.replace("#", "%23")
    url = f"https://api.clashroyale.com/v1/players/{encoded_tag}"
    return clash_get(url)


def path_of_legends_name(league_number):
    mapping = {
        1: "Master I",
        2: "Master II",
        3: "Master III",
        4: "Champion",
        5: "Grand Champion",
        6: "Royal Champion",
        7: "Ultimate Champion",
    }
    return mapping.get(league_number, "Unranked")


def extract_path_of_legends(player_data):
    if not player_data:
        return {
            "league_number": None,
            "league_name": "Unranked",
            "trophies": None,
            "rank": None,
            "step": None
        }

    current_pol = player_data.get("currentPathOfLegendSeasonResult") or {}

    league_number = current_pol.get("leagueNumber")
    pol_trophies = current_pol.get("trophies")
    pol_rank = current_pol.get("rank")
    pol_step = current_pol.get("step")

    if league_number is None:
        league_number = current_pol.get("league")

    return {
        "league_number": league_number,
        "league_name": path_of_legends_name(league_number),
        "trophies": pol_trophies,
        "rank": pol_rank,
        "step": pol_step
    }


def format_path_of_legends(pol_data):
    if not pol_data:
        return "Unranked"
    return pol_data.get("league_name", "Unranked")


def format_pol_detail_line(pol_data):
    if not pol_data:
        return "Unranked"

    league = pol_data.get("league_name", "Unranked")
    trophies = pol_data.get("trophies")
    step = pol_data.get("step")
    rank = pol_data.get("rank")

    extras = []

    if trophies is not None:
        extras.append(f"Trophies: {trophies}")
    if step is not None:
        extras.append(f"Step: {step}")
    if rank is not None:
        extras.append(f"Rank: #{rank}")

    if extras:
        return f"{league} ({' | '.join(extras)})"

    return league


def get_clan_members():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}"
    data = clash_get(url)

    if not data:
        return None

    members = data.get("memberList", [])
    result = {}

    for m in members:
        tag = m["tag"]
        player_data = get_player_details(tag)
        pol_data = extract_path_of_legends(player_data)

        result[tag] = {
            "name": m["name"],
            "trophies": m.get("trophies", 0),
            "role": m.get("role", "member"),
            "path_of_legends": pol_data
        }

        time.sleep(0.15)

    return result


def get_clan_data():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}"
    return clash_get(url)


def get_clan_score():
    data = get_clan_data()

    if not data:
        return 0

    clan_score = data.get("clanScore", 0)
    log(f"Fetched API clan score: {clan_score}")
    return clan_score


def get_clan_war_trophies():
    data = get_clan_data()

    if not data:
        return 0

    war_trophies = data.get("clanWarTrophies", 0)
    log(f"Fetched API clan war trophies: {war_trophies}")
    return war_trophies


def get_clan_war_league_name(trophies):
    """Return clan war league name based on Clan War trophies."""
    try:
        trophies = int(trophies)
    except (TypeError, ValueError):
        trophies = 0

    if trophies < 600:
        return "🟫 Bronze League"
    elif trophies < 1500:
        return "⬜ Silver League"
    elif trophies < 3000:
        return "🟨 Gold League"
    else:
        return "🟪 Legendary League"


def load_location_cache():
    return load_json_file(LOCATION_CACHE_FILE)


def save_location_cache(data):
    try:
        atomic_json_save(LOCATION_CACHE_FILE, data)
    except Exception as e:
        log(f"Failed to save location cache file: {e}")


def load_sg_rank_cache():
    return load_json_file(SG_RANK_CACHE_FILE)


def save_sg_rank_cache(rank_data, cache_time=None):
    if cache_time is None:
        cache_time = time.time()

    data = {
        "timestamp": cache_time,
        "rank_data": rank_data
    }

    try:
        atomic_json_save(SG_RANK_CACHE_FILE, data)
    except Exception as e:
        log(f"Failed to save SG rank cache file: {e}")


def load_member_history():
    data = load_json_file(MEMBER_HISTORY_FILE)
    return data if isinstance(data, dict) else {}


def save_member_history(data):
    try:
        atomic_json_save(MEMBER_HISTORY_FILE, data)
    except Exception as e:
        log(f"Failed to save member history file: {e}")


def load_leadership_report_state():
    data = load_json_file(LEADERSHIP_REPORT_STATE_FILE)
    return data if isinstance(data, dict) else {}


def save_leadership_report_state(data):
    try:
        atomic_json_save(LEADERSHIP_REPORT_STATE_FILE, data)
    except Exception as e:
        log(f"Failed to save leadership report state file: {e}")


def load_uc_alert_state():
    data = load_json_file(UC_ALERT_STATE_FILE)
    return data if isinstance(data, dict) else {}


def save_uc_alert_state(data):
    try:
        atomic_json_save(UC_ALERT_STATE_FILE, data)
    except Exception as e:
        log(f"Failed to save UC alert state file: {e}")


def get_location_id_by_name(location_name):
    cache = load_location_cache() or {}
    cached_id = cache.get(location_name)

    if cached_id is not None:
        return cached_id

    url = "https://api.clashroyale.com/v1/locations"
    data = clash_get(url)

    if not data:
        return None

    for item in data.get("items", []):
        if str(item.get("name", "")).strip().lower() == location_name.strip().lower():
            location_id = item.get("id")
            if location_id is not None:
                cache[location_name] = location_id
                save_location_cache(cache)
                return location_id

    return None


def get_clan_rank_by_location(location_id, clan_tag):
    clean_tag = clan_tag.replace("%23", "").replace("#", "").upper()
    after = None

    while True:
        url = f"https://api.clashroyale.com/v1/locations/{location_id}/rankings/clans"
        if after:
            url += f"?after={after}"

        data = clash_get(url)
        if not data:
            return None

        items = data.get("items", [])
        for clan in items:
            api_tag = str(clan.get("tag", "")).replace("#", "").upper()
            if api_tag == clean_tag:
                return {
                    "rank": clan.get("rank"),
                    "name": clan.get("name"),
                    "score": clan.get("clanScore"),
                    "member_count": clan.get("memberCount"),
                    "location_id": location_id
                }

        paging = data.get("paging", {}) or {}
        cursors = paging.get("cursors", {}) or {}
        after = cursors.get("after")

        if not after:
            break

        time.sleep(0.2)

    return None


def get_sg_clan_rank():
    cache_data = load_sg_rank_cache()

    if cache_data:
        cache_timestamp = cache_data.get("timestamp", 0)
        rank_data = cache_data.get("rank_data")
        if (time.time() - cache_timestamp) < SG_RANK_CACHE_SECONDS:
            return rank_data

    singapore_location_id = get_location_id_by_name("Singapore")
    if singapore_location_id is None:
        log("Failed to resolve Singapore location ID.")
        return cache_data.get("rank_data") if cache_data else None

    rank_data = get_clan_rank_by_location(singapore_location_id, CLAN_TAG)

    if rank_data and rank_data.get("rank") is not None:
        save_sg_rank_cache(rank_data)
        log(f"Official API SG rank found: #{rank_data['rank']}")
        return rank_data

    log("Official API SG rank not found.")
    return cache_data.get("rank_data") if cache_data else None


def load_previous_members():
    return load_json_file(STATE_FILE)


def save_members(members):
    try:
        atomic_json_save(STATE_FILE, members)
    except Exception as e:
        log(f"Failed to save state file: {e}")


def load_message_state():
    data = load_json_file(MESSAGE_STATE_FILE)
    if data is not None:
        log(f"Loaded message state: {data}")
    else:
        log(f"Message state file not found or unreadable: {MESSAGE_STATE_FILE}")
    return data


def save_message_state(message_id):
    try:
        atomic_json_save(MESSAGE_STATE_FILE, {"message_id": message_id})
        log(f"Saved message state file: {MESSAGE_STATE_FILE} | message_id={message_id}")
    except Exception as e:
        log(f"Failed to save message state file: {e}")


def get_saved_message_id():
    message_state = load_message_state()
    if message_state and message_state.get("message_id"):
        return message_state["message_id"]
    return None


def load_hourly_snapshot():
    data = load_json_file(HOURLY_SNAPSHOT_FILE)
    if data:
        ts = data.get("timestamp")
        if ts:
            log(f"Loaded hourly snapshot: {format_sgt_timestamp(ts)} | file={HOURLY_SNAPSHOT_FILE}")
        else:
            log(f"No hourly snapshot found at: {HOURLY_SNAPSHOT_FILE}")
    return data


def save_hourly_snapshot(members, snapshot_time=None):
    if snapshot_time is None:
        snapshot_time = time.time()

    data = {
        "timestamp": snapshot_time,
        "members": members
    }

    try:
        atomic_json_save(HOURLY_SNAPSHOT_FILE, data)
        log(f"Saved hourly snapshot: {format_sgt_timestamp(snapshot_time)} | file={HOURLY_SNAPSHOT_FILE}")
    except Exception as e:
        log(f"Failed to save hourly snapshot file: {e}")


def load_war_state():
    return load_json_file(WAR_STATE_FILE)


def load_or_init_war_state():
    data = load_war_state() or {}

    if not isinstance(data, dict):
        data = {}

    if "alerted_days" not in data or not isinstance(data["alerted_days"], dict):
        data["alerted_days"] = {
            "riverRace": [],
            "colosseum": []
        }

    if "last_seen_timestamp" not in data:
        data["last_seen_timestamp"] = 0

    if "last_war_key" not in data:
        data["last_war_key"] = None

    return data


def save_war_state(data):
    try:
        atomic_json_save(WAR_STATE_FILE, data)
    except Exception as e:
        log(f"Failed to save war state file: {e}")


def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

    try:
        r = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "text": text
            },
            timeout=20
        )
    except requests.RequestException as e:
        log(f"Telegram request error: {e}")
        return None

    if r.status_code != 200:
        log(f"Telegram error: {r.status_code} {r.text}")
        return None

    try:
        data = r.json()
        log("Telegram message sent.")
        return data
    except ValueError:
        log("Telegram sendMessage response is not valid JSON.")
        return None


def edit_telegram_message(message_id, text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/editMessageText"

    try:
        r = requests.post(
            url,
            data={
                "chat_id": TELEGRAM_CHAT_ID,
                "message_id": message_id,
                "text": text
            },
            timeout=20
        )
    except requests.RequestException as e:
        log(f"Telegram edit request error: {e}")
        return False

    if r.status_code == 200:
        log(f"Telegram full list message edited. message_id={message_id}")
        return True

    try:
        error_data = r.json()
        description = error_data.get("description", "")
    except ValueError:
        description = r.text

    if r.status_code == 400 and "message is not modified" in description.lower():
        log("Telegram message already up to date. No edit needed.")
        return True

    log(f"Telegram edit error for message_id={message_id}: {r.status_code} {description}")
    return False


def build_full_clan_list_text(members):
    sorted_members = sorted(
        members.items(),
        key=lambda x: x[1]["trophies"],
        reverse=True
    )

    clan_score = get_clan_score()
    clan_war_trophies = get_clan_war_trophies()
    clan_war_league = get_clan_war_league_name(clan_war_trophies)
    sg_rank_data = get_sg_clan_rank()

    lines = []
    for i, (tag, info) in enumerate(sorted_members, start=1):
        pol_text = format_path_of_legends(info.get("path_of_legends"))

        lines.append(
            f"{i}. {info['name']}\n"
            f"   🏆 {info['trophies']} | 🛡️ {info['role']}\n"
            f"   🎯 {pol_text}\n"
            f"   🔖 {tag}"
        )

    header = (
        f"📋 Kopi O current clan members ({len(sorted_members)}/50)\n"
        f"🏆 Clan Score: {clan_score}\n"
        f"⚔️ Clan War Trophies: {clan_war_trophies} ({clan_war_league})"
    )

    if sg_rank_data and sg_rank_data.get("rank") is not None:
        header += f"\n🇸🇬 SG Rank: #{sg_rank_data['rank']}"
    else:
        header += "\n🇸🇬 SG Rank: Not found"

    return header + "\n\n" + "\n\n".join(lines)


def send_full_clan_list(members):
    text = build_full_clan_list_text(members)
    return send_telegram_message(text)


def update_full_clan_list_message(message_id, members):
    if not message_id:
        log("No saved message_id found. Skipping edit to avoid duplicate full list.")
        return False

    text = build_full_clan_list_text(members)
    ok = edit_telegram_message(message_id, text)

    if ok:
        return True

    log("Edit failed. Not sending a new full list to avoid spam.")
    return False


def ensure_full_clan_list_message(current_members):
    saved_message_id = get_saved_message_id()

    if saved_message_id:
        log(f"Trying to re-edit old Telegram message: {saved_message_id}")
        ok = update_full_clan_list_message(saved_message_id, current_members)
        if ok:
            return saved_message_id

    log("Old message not usable. Sending one new full clan list message...")
    result = send_full_clan_list(current_members)

    if result and result.get("ok") and result.get("result"):
        new_message_id = result["result"]["message_id"]
        save_message_state(new_message_id)
        return new_message_id

    return saved_message_id


def update_member_history(previous_members, current_members):
    history = load_member_history()
    now_ts = time.time()

    for tag, info in current_members.items():
        current_name = info.get("name", "Unknown")
        current_trophies = info.get("trophies", 0)
        current_role = info.get("role", "member")

        record = history.get(tag, {})

        if not isinstance(record, dict):
            record = {}

        if "first_seen" not in record:
            record["first_seen"] = now_ts

        record["last_seen"] = now_ts
        record["name"] = current_name
        record["role"] = current_role
        record["current_trophies"] = current_trophies

        if "last_trophy_change" not in record:
            record["last_trophy_change"] = now_ts

        if "total_trophy_gain" not in record:
            record["total_trophy_gain"] = 0

        if "days_in_clan_estimate" not in record:
            record["days_in_clan_estimate"] = 0

        previous_info = previous_members.get(tag)
        if previous_info:
            previous_trophies = previous_info.get("trophies", 0)
            diff = current_trophies - previous_trophies

            if diff != 0:
                record["last_trophy_change"] = now_ts

            if diff > 0:
                record["total_trophy_gain"] += diff

        first_seen = record.get("first_seen", now_ts)
        days_in_clan = int((now_ts - first_seen) // 86400)
        record["days_in_clan_estimate"] = max(0, days_in_clan)

        history[tag] = record

    save_member_history(history)
    return history


def build_promotion_watchlist(current_members, history):
    PROMOTE_MIN_TROPHIES = 7500
    PROMOTE_MIN_DAYS_IN_CLAN = 7
    PROMOTE_MIN_TROPHY_GAIN = 100

    candidates = []

    for tag, info in current_members.items():
        role = info.get("role", "member")

        if role != "member":
            continue

        record = history.get(tag, {})
        days_in_clan = record.get("days_in_clan_estimate", 0)
        total_trophy_gain = record.get("total_trophy_gain", 0)
        current_trophies = info.get("trophies", 0)

        if (
            current_trophies >= PROMOTE_MIN_TROPHIES
            and days_in_clan >= PROMOTE_MIN_DAYS_IN_CLAN
            and total_trophy_gain >= PROMOTE_MIN_TROPHY_GAIN
        ):
            candidates.append({
                "tag": tag,
                "name": info.get("name", "Unknown"),
                "role": role,
                "score": current_trophies,
                "reasons": [
                    f"{current_trophies} trophies",
                    f"{days_in_clan} days in clan",
                    f"+{total_trophy_gain} total trophy gain",
                    "high trophies, active, safe to promote"
                ]
            })

    candidates.sort(key=lambda x: -x["score"])
    return candidates[:10]


def build_kick_risk_watchlist(current_members, history):
    KICK_INACTIVE_DAYS = 7
    KICK_LOW_TROPHIES = 5000

    candidates = []
    now_ts = time.time()

    for tag, info in current_members.items():
        role = info.get("role", "member")

        if role in ["leader", "coLeader"]:
            continue

        record = history.get(tag, {})
        current_trophies = info.get("trophies", 0)
        last_trophy_change = record.get("last_trophy_change", now_ts)
        inactive_days = int((now_ts - last_trophy_change) // 86400)

        if inactive_days >= KICK_INACTIVE_DAYS:
            reasons = [f"no trophy change for {inactive_days} days"]

            if current_trophies < KICK_LOW_TROPHIES:
                reasons.append(f"low trophies ({current_trophies})")
                score = inactive_days + 1000
            else:
                score = inactive_days

            candidates.append({
                "tag": tag,
                "name": info.get("name", "Unknown"),
                "role": role,
                "score": score,
                "reasons": reasons
            })

    candidates.sort(key=lambda x: (-x["score"], x["name"].lower()))
    return candidates[:10]


def build_leadership_review_text(current_members, history):
    promotion_list = build_promotion_watchlist(current_members, history)

    lines = []
    lines.append("👑 Kopi O Weekly Promotion Review")

    if promotion_list:
        lines.append("\n⭐ Recommended Promotions")

        for i, item in enumerate(promotion_list, start=1):
            reason_text = ", ".join(item["reasons"][:3])

            lines.append(
                f"{i}. {item['name']}\n"
                f"   🏆 {item['score']} trophies\n"
                f"   🛡️ Current Role: {item['role']}\n"
                f"   📌 {reason_text}\n"
                f"   🔖 {item['tag']}"
            )
    else:
        lines.append("\n⭐ No promotion candidates this week.")

    return "\n".join(lines)


def maybe_send_leadership_review(current_members, history):
    now = datetime.now(BOT_TIMEZONE)

    # Sunday only
    if now.weekday() != 6:
        return

    state = load_leadership_report_state()
    last_sent = state.get("last_sent_timestamp", 0)
    now_ts = time.time()

    # prevent duplicate send within 7 days
    if (now_ts - last_sent) < LEADERSHIP_REPORT_SECONDS:
        return

    text = build_leadership_review_text(current_members, history)
    result = send_telegram_message(text)

    if result and result.get("ok"):
        save_leadership_report_state({
            "last_sent_timestamp": now_ts
        })
        log("Weekly promotion review sent.")
    else:
        log("Weekly promotion review failed.")


def send_role_change_alert(name, tag, old_role, new_role, trophies, member_count):
    msg = (
        f"🛡️ Role changed in Kopi O\n"
        f"👤 {name}\n"
        f"🔄 {old_role} ➜ {new_role}\n"
        f"🏆 {trophies} trophies\n"
        f"🔖 {tag}\n"
        f"📊 Members: {member_count}/50"
    )
    send_telegram_message(msg)


def send_path_of_legends_change_alert(name, tag, old_pol, new_pol, member_count):
    new_text = format_pol_detail_line(new_pol)

    msg = (
        f"🎯 Path of Legends updated in Kopi O\n"
        f"👤 {name}\n"
        f"➡️ {new_text}\n"
        f"🔖 {tag}\n"
        f"📊 Members: {member_count}/50"
    )
    send_telegram_message(msg)


def send_clan_capacity_alert(previous_count, current_count):
    if previous_count < 50 and current_count == 50:
        send_telegram_message("🚨 Kopi O is now full (50/50)")
    elif previous_count == 50 and current_count < 50:
        send_telegram_message(f"✅ Kopi O has an open slot ({current_count}/50)")


def send_join_alerts(joined_tags, current_members):
    for tag in joined_tags:
        info = current_members[tag]
        name = info["name"]
        text = info["name"]
        trophies = info["trophies"]
        role = info["role"]
        pol_text = format_path_of_legends(info.get("path_of_legends"))

        msg = (
            f"🎉 Member joined Kopi O\n"
            f"👤 {name}\n"
            f"🏆 {trophies} trophies\n"
            f"🛡️ Role: {role}\n"
            f"🎯 {pol_text}\n"
            f"🔖 {tag}\n"
            f"📊 Members: {len(current_members)}/50"
        )
        send_telegram_message(msg)
        log(f"Join sent: {name} ({tag})")


def send_leave_alerts(left_tags, previous_members, current_count):
    for tag in left_tags:
        info = previous_members[tag]
        name = info["name"]
        trophies = info.get("trophies", 0)
        role = info.get("role", "member")
        pol_text = format_path_of_legends(info.get("path_of_legends"))

        msg = (
            f"🚪 Member left Kopi O\n"
            f"👤 {name}\n"
            f"🏆 {trophies} trophies\n"
            f"🛡️ Role: {role}\n"
            f"🎯 {pol_text}\n"
            f"🔖 {tag}\n"
            f"📊 Members: {current_count}/50"
        )
        send_telegram_message(msg)
        log(f"Leave sent: {name} ({tag})")


def get_current_river_race():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}/currentriverrace"
    return clash_get(url)


def get_war_phase():
    data = get_current_river_race()

    if not data:
        return None

    period_index = data.get("periodIndex")
    period_type = data.get("periodType")

    if period_index is None:
        log(f"River race data missing periodIndex. Raw data: {data}")
        return None

    cycle_day = period_index % 7
    is_colosseum = period_type == "colosseum"

    is_training_day = cycle_day in [0, 1, 2]
    is_war_day = cycle_day in [3, 4, 5, 6]

    day_number = None
    if is_war_day:
        day_number = cycle_day - 2

    return {
        "periodIndex": period_index,
        "cycle_day": cycle_day,
        "periodType": period_type,
        "is_colosseum": is_colosseum,
        "is_training_day": is_training_day,
        "is_war_day": is_war_day,
        "day_number": day_number
    }


def send_clan_war_day_alert(day_number, is_colosseum=False, recovery=False):
    if is_colosseum:
        title = f"🏟️ Kopi O Colosseum Day {day_number}"
    else:
        title = f"⚔️ Kopi O Clan War Day {day_number}"

    if recovery:
        msg = (
            f"{title}\n"
            f"🔄 Recovered after downtime\n"
            f"🔥 Good luck in battles!"
        )
    else:
        msg = (
            f"{title}\n"
            f"🔥 Good luck in battles!"
        )

    result = send_telegram_message(msg)
    return bool(result and result.get("ok"))


def check_war_day_started():
    state = load_or_init_war_state()
    current_war = get_war_phase()

    if not current_war:
        log("Could not fetch current river race.")
        return

    current_period_index = current_war.get("periodIndex")
    current_cycle_day = current_war.get("cycle_day")
    current_period_type = current_war.get("periodType")
    current_is_war_day = current_war.get("is_war_day", False)
    current_is_colosseum = current_war.get("is_colosseum", False)
    current_day_number = current_war.get("day_number")

    war_key = "colosseum" if current_is_colosseum else "riverRace"
    previous_war_key = state.get("last_war_key")

    if previous_war_key != war_key:
        state["alerted_days"][war_key] = []
        state["last_war_key"] = war_key
        log(f"War type changed to {war_key}. Reset alerted days for this mode.")

    if not current_is_war_day:
        previous_day = state.get("day_number")
        previous_is_war_day = state.get("is_war_day", False)

        if previous_is_war_day and previous_day in [1, 2, 3, 4]:
            state["alerted_days"]["riverRace"] = []
            state["alerted_days"]["colosseum"] = []
            log("War cycle ended. Reset alerted war days.")

        state["periodIndex"] = current_period_index
        state["cycle_day"] = current_cycle_day
        state["periodType"] = current_period_type
        state["is_war_day"] = current_is_war_day
        state["day_number"] = current_day_number
        state["last_seen_timestamp"] = time.time()
        save_war_state(state)

        log(
            f"No war alert sent (training phase). "
            f"periodType={current_period_type}, "
            f"periodIndex={current_period_index}, "
            f"cycle_day={current_cycle_day}"
        )
        return

    if current_day_number is None:
        log("Invalid war data received. Skipping alert.")
        return

    alerted_days = state["alerted_days"].get(war_key, [])
    alerted_days = sorted(set(day for day in alerted_days if isinstance(day, int)))

    max_alerted_day = max(alerted_days) if alerted_days else 0

    if current_day_number > max_alerted_day:
        missing_days = list(range(max_alerted_day + 1, current_day_number + 1))

        for day in missing_days:
            recovery_mode = day < current_day_number or max_alerted_day > 0

            ok = send_clan_war_day_alert(
                day_number=day,
                is_colosseum=current_is_colosseum,
                recovery=recovery_mode
            )

            if ok:
                alerted_days.append(day)
                state["alerted_days"][war_key] = sorted(set(alerted_days))
                save_war_state(state)
                log(
                    f"War alert sent. "
                    f"periodType={current_period_type}, "
                    f"periodIndex={current_period_index}, "
                    f"cycle_day={current_cycle_day}, "
                    f"day={day}"
                )
            else:
                log(
                    f"War alert FAILED. "
                    f"periodType={current_period_type}, "
                    f"periodIndex={current_period_index}, "
                    f"cycle_day={current_cycle_day}, "
                    f"day={day}"
                )
                break
    else:
        log(
            f"No new war alert needed. "
            f"periodType={current_period_type}, "
            f"periodIndex={current_period_index}, "
            f"cycle_day={current_cycle_day}, "
            f"day={current_day_number}"
        )

    state["periodIndex"] = current_period_index
    state["cycle_day"] = current_cycle_day
    state["periodType"] = current_period_type
    state["is_war_day"] = current_is_war_day
    state["day_number"] = current_day_number
    state["last_seen_timestamp"] = time.time()
    save_war_state(state)


def check_role_changes(previous_members, current_members):
    """Only alert:
    - member -> elder
    - elder -> coLeader
    """

    common_tags = set(previous_members.keys()) & set(current_members.keys())

    IMPORTANT_ROLE_CHANGES = {
        ("member", "elder"),
        ("elder", "coLeader"),
    }

    for tag in common_tags:
        old_role = previous_members[tag].get("role", "member")
        new_role = current_members[tag].get("role", "member")

        if old_role == new_role:
            continue

        # Ignore all other role changes
        if (old_role, new_role) not in IMPORTANT_ROLE_CHANGES:
            continue

        name = current_members[tag]["name"]
        trophies = current_members[tag].get("trophies", 0)

        send_role_change_alert(
            name=name,
            tag=tag,
            old_role=old_role,
            new_role=new_role,
            trophies=trophies,
            member_count=len(current_members)
        )

        log(
            f"Promotion alert sent: "
            f"{name} ({tag}) "
            f"{old_role} -> {new_role}"
        )


def has_role_changes(previous_members, current_members):
    """Only detect:
    - member -> elder
    - elder -> coLeader
    """

    IMPORTANT_ROLE_CHANGES = {
        ("member", "elder"),
        ("elder", "coLeader"),
    }

    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_role = previous_members[tag].get("role", "member")
        new_role = current_members[tag].get("role", "member")

        if old_role == new_role:
            continue

        if (old_role, new_role) in IMPORTANT_ROLE_CHANGES:
            return True

    return False


def check_path_of_legends_changes(previous_members, current_members):
    """Smart Path of Legends tracking:
    - ignores maintenance spam
    - ignores Unranked spam
    - ignores low leagues
    - only alerts important promotions
    """

    IMPORTANT_LEAGUES = {
        1: "Master I",
        2: "Master II",
        3: "Master III",
        4: "Champion",
        5: "Grand Champion",
        6: "Royal Champion",
        7: "Ultimate Champion"
    }

    now_ts = time.time()

    uc_state = load_uc_alert_state()

    if not isinstance(uc_state, dict):
        uc_state = {}

    alerted_tags = set(uc_state.get("alerted_tags", []))
    maintenance_until = uc_state.get("maintenance_until", 0)

    common_tags = set(previous_members.keys()) & set(current_members.keys())

    unranked_count = 0
    total_checked = 0

    # Detect maintenance/API issues
    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {}) or {}
        new_pol = current_members[tag].get("path_of_legends", {}) or {}

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        if old_league is not None:
            total_checked += 1

            if new_league is None:
                unranked_count += 1

    # If many players suddenly become unranked, assume maintenance/API issue
    if total_checked >= 10:
        ratio = unranked_count / total_checked

        if ratio >= 0.4:
            maintenance_until = now_ts + POL_MAINTENANCE_GRACE_SECONDS

            uc_state["maintenance_until"] = maintenance_until
            save_uc_alert_state(uc_state)

            log(
                f"Detected Clash Royale maintenance/API issue. "
                f"Suppressing PoL alerts for "
                f"{POL_MAINTENANCE_GRACE_SECONDS} seconds."
            )

            return

    # Suppress alerts during maintenance cooldown
    if now_ts < maintenance_until:
        remaining = int(maintenance_until - now_ts)

        log(
            f"PoL alerts suppressed due to maintenance cooldown. "
            f"{remaining}s remaining."
        )
        return

    # Normal alert logic
    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {}) or {}
        new_pol = current_members[tag].get("path_of_legends", {}) or {}

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        # No change
        if old_league == new_league:
            continue

        # Ignore unranked transitions
        if new_league is None:
            continue

        # Ignore low leagues
        if new_league not in IMPORTANT_LEAGUES:
            continue

        # Only alert upward promotions
        if old_league is not None and new_league <= old_league:
            continue

        name = current_members[tag]["name"]

        send_path_of_legends_change_alert(
            name=name,
            tag=tag,
            old_pol=old_pol,
            new_pol=new_pol,
            member_count=len(current_members)
        )

        if new_league == 7 and tag not in alerted_tags:
            alerted_tags.add(tag)

        log(
            f"Important PoL promotion: "
            f"{name} ({tag}) "
            f"{old_league} -> {new_league}"
        )

    uc_state["alerted_tags"] = sorted(alerted_tags)
    save_uc_alert_state(uc_state)


def has_path_of_legends_changes(previous_members, current_members):
    """Only detect REAL upward PoL promotions:
    Master I -> Ultimate Champion
    """

    IMPORTANT_LEAGUES = {1, 2, 3, 4, 5, 6, 7}

    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {}) or {}
        new_pol = current_members[tag].get("path_of_legends", {}) or {}

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        # no league change
        if old_league == new_league:
            continue

        # ignore maintenance reset / unranked
        if new_league is None:
            continue

        # ignore anything below Master I
        if new_league not in IMPORTANT_LEAGUES:
            continue

        # only upward promotions
        if old_league is not None and new_league <= old_league:
            continue

        return True

    return False


def has_member_trophy_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_trophies = previous_members[tag].get("trophies", 0)
        new_trophies = current_members[tag].get("trophies", 0)

        if old_trophies != new_trophies:
            return True

    return False


def build_hourly_trophy_summary_text(snapshot_members, current_members, snapshot_timestamp):
    common_tags = set(snapshot_members.keys()) & set(current_members.keys())

    changed_players = []
    pol_changed_players = []
    total_up = 0
    total_down = 0

    for tag in common_tags:
        old_info = snapshot_members[tag]
        new_info = current_members[tag]

        old_trophies = old_info.get("trophies", 0)
        new_trophies = new_info.get("trophies", 0)
        diff = new_trophies - old_trophies

        name = new_info.get("name", old_info.get("name", "Unknown"))

        if diff != 0:
            changed_players.append({
                "tag": tag,
                "name": name,
                "old_trophies": old_trophies,
                "new_trophies": new_trophies,
                "diff": diff
            })

            if diff > 0:
                total_up += diff
            else:
                total_down += abs(diff)

        old_pol = old_info.get("path_of_legends", {}) or {}
        new_pol = new_info.get("path_of_legends", {}) or {}

        old_text = format_pol_detail_line(old_pol)
        new_text = format_pol_detail_line(new_pol)

        if old_text != new_text:
            pol_changed_players.append({
                "tag": tag,
                "name": name,
                "old_pol_text": old_text,
                "new_pol_text": new_text
            })

    joined_tags = set(current_members.keys()) - set(snapshot_members.keys())
    left_tags = set(snapshot_members.keys()) - set(current_members.keys())

    changed_players.sort(key=lambda x: (-x["diff"], x["name"].lower()))
    pol_changed_players.sort(key=lambda x: x["name"].lower())

    start_time_text = format_sgt_timestamp(snapshot_timestamp)
    end_time_text = datetime.now(BOT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

    changed_tag_set = set()
    changed_tag_set.update(player["tag"] for player in changed_players)
    changed_tag_set.update(player["tag"] for player in pol_changed_players)
    total_changed_count = len(changed_tag_set)

    header = (
        f"📊 Kopi O 4 Hour Trophy Summary\n"
        f"🕒 {start_time_text} → {end_time_text}\n"
        f"📈 Total Gained: +{total_up}\n"
        f"📉 Total Lost: -{total_down}\n"
        f"👥 Changed Players: {total_changed_count}"
    )

    lines = []

    if changed_players:
        lines.append("\n🏆 Trophy Changes")
        for i, player in enumerate(changed_players, start=1):
            diff_text = f"+{player['diff']}" if player["diff"] > 0 else str(player["diff"])
            lines.append(
                f"{i}. {player['name']} {diff_text} "
                f"({player['old_trophies']} → {player['new_trophies']})"
            )
    else:
        lines.append("\n🏆 No trophy changes in the last 4 hours.")

    if pol_changed_players:
        lines.append("\n🎯 Path of Legends Changes")
        for i, player in enumerate(pol_changed_players, start=1):
            lines.append(
                f"{i}. {player['name']}\n"
                f"   {player['old_pol_text']} → {player['new_pol_text']}"
            )

    if joined_tags:
        lines.append("\n🎉 Joined During This Period")
        joined_names = sorted(current_members[tag]["name"] for tag in joined_tags)
        for i, name in enumerate(joined_names, start=1):
            lines.append(f"{i}. {name}")

    if left_tags:
        lines.append("\n🚪 Left During This Period")
        left_names = sorted(snapshot_members[tag]["name"] for tag in left_tags)
        for i, name in enumerate(left_names, start=1):
            lines.append(f"{i}. {name}")

    return header + "\n" + "\n".join(lines)


def maybe_send_hourly_trophy_summary(current_members):
    snapshot_data = load_hourly_snapshot()

    if snapshot_data is None:
        log("No existing hourly snapshot. Creating first snapshot now.")
        save_hourly_snapshot(current_members)
        return

    snapshot_timestamp = snapshot_data.get("timestamp", 0)
    snapshot_members = snapshot_data.get("members", {})

    if not isinstance(snapshot_members, dict):
        snapshot_members = {}

    if not snapshot_timestamp:
        log("Snapshot timestamp missing or invalid. Resetting hourly snapshot.")
        save_hourly_snapshot(current_members)
        return

    elapsed = time.time() - snapshot_timestamp
    remaining = HOURLY_SUMMARY_SECONDS - elapsed

    log(
        f"Hourly summary check | "
        f"snapshot={format_sgt_timestamp(snapshot_timestamp)} | "
        f"elapsed={int(elapsed)}s | remaining={max(0, int(remaining))}s"
    )

    if elapsed < HOURLY_SUMMARY_SECONDS:
        return

    text = build_hourly_trophy_summary_text(
        snapshot_members=snapshot_members,
        current_members=current_members,
        snapshot_timestamp=snapshot_timestamp
    )

    send_telegram_message(text)
    log("4 hour trophy summary sent.")

    save_hourly_snapshot(current_members)


def main():
    if not CLASH_API_TOKEN:
        log("Missing CLASH_API_TOKEN")
        return
    if not TELEGRAM_BOT_TOKEN:
        log("Missing TELEGRAM_BOT_TOKEN")
        return
    if not TELEGRAM_CHAT_ID:
        log("Missing TELEGRAM_CHAT_ID")
        return

    keep_awake()
    ensure_single_instance()

    log("Bot started...")
    log(f"Process ID: {os.getpid()}")
    log(f"DATA_DIR: {DATA_DIR}")
    log(f"STATE_FILE: {STATE_FILE}")
    log(f"MESSAGE_STATE_FILE: {MESSAGE_STATE_FILE}")
    log(f"HOURLY_SNAPSHOT_FILE: {HOURLY_SNAPSHOT_FILE}")

    previous_members = load_previous_members()
    current_members = get_clan_members()
    check_war_day_started()

    if current_members is None:
        log("Failed to fetch clan members on startup. Bot will not continue.")
        return

    full_list_message_id = ensure_full_clan_list_message(current_members)
    save_members(current_members)
    history = update_member_history(previous_members or {}, current_members)

    if load_hourly_snapshot() is None:
        save_hourly_snapshot(current_members)

    previous_members = current_members
    log("Startup sync completed.")

    while True:
        try:
            keep_awake()
            current_members = get_clan_members()
            check_war_day_started()

            if current_members is None:
                log("Skipping this cycle because clan data could not be fetched.")
                time.sleep(CHECK_EVERY_SECONDS)
                continue

            joined = set(current_members.keys()) - set(previous_members.keys())
            left = set(previous_members.keys()) - set(current_members.keys())
            role_changed = has_role_changes(previous_members, current_members)
            pol_changed = has_path_of_legends_changes(previous_members, current_members)
            trophy_changed = has_member_trophy_changes(previous_members, current_members)

            log(f"Checked clan. Current members: {len(current_members)}")

            if joined:
                send_join_alerts(joined, current_members)

            if left:
                send_leave_alerts(left, previous_members, len(current_members))

            check_role_changes(previous_members, current_members)
            check_path_of_legends_changes(previous_members, current_members)
            send_clan_capacity_alert(len(previous_members), len(current_members))

            if joined or left or role_changed or pol_changed or trophy_changed:
                update_full_clan_list_message(full_list_message_id, current_members)

            maybe_send_hourly_trophy_summary(current_members)

            history = update_member_history(previous_members, current_members)
            maybe_send_leadership_review(current_members, history)

            previous_members = current_members
            save_members(current_members)

        except Exception as e:
            log(f"Unexpected error: {e}")

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
