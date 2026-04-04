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
HOURLY_SUMMARY_SECONDS = 7200
SG_RANK_CACHE_SECONDS = 600

BOT_TIMEZONE = ZoneInfo("Asia/Singapore")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

STATE_FILE = os.path.join(DATA_DIR, "clan_members_state.json")
MESSAGE_STATE_FILE = os.path.join(DATA_DIR, "telegram_message_state.json")
HOURLY_SNAPSHOT_FILE = os.path.join(DATA_DIR, "hourly_trophy_snapshot.json")
WAR_STATE_FILE = os.path.join(DATA_DIR, "war_state.json")
LOCATION_CACHE_FILE = os.path.join(DATA_DIR, "location_cache.json")
SG_RANK_CACHE_FILE = os.path.join(DATA_DIR, "sg_rank_cache.json")

CLASH_HEADERS = {
    "Authorization": f"Bearer {CLASH_API_TOKEN}"
}

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001

_instance_lock_file = None


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


def get_clan_score():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}"
    data = clash_get(url)

    if not data:
        return 0

    clan_score = data.get("clanScore", 0)
    log(f"Fetched API clan score: {clan_score}")
    return clan_score


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
        f"🏆 Clan Score: {clan_score}"
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
    old_text = format_pol_detail_line(old_pol)
    new_text = format_pol_detail_line(new_pol)

    msg = (
        f"🎯 Path of Legends updated in Kopi O\n"
        f"👤 {name}\n"
        f"🔄 {old_text}\n"
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

    section_index = data.get("sectionIndex")
    period_type = data.get("periodType")
    period_index = data.get("periodIndex")

    # Detect war type from Clash API
    is_colosseum = period_type == "colosseum"
    is_normal_war = period_type != "colosseum"

    # Same day mapping for both normal war and colosseum:
    # 0,1,2 = training
    # 3,4,5,6 = day 1,2,3,4
    is_training_day = section_index in [0, 1, 2]
    is_war_day = section_index in [3, 4, 5, 6]

    return {
        "sectionIndex": section_index,
        "periodType": period_type,
        "periodIndex": period_index,
        "is_colosseum": is_colosseum,
        "is_normal_war": is_normal_war,
        "is_training_day": is_training_day,
        "is_war_day": is_war_day
    }


def send_clan_war_day_alert(day_number, is_colosseum=False):
    if is_colosseum:
        msg = (
            f"🏟️ Kopi O Colosseum Day {day_number} Started\n"
            f"🔥 Good luck in battles!"
        )
    else:
        msg = (
            f"⚔️ Kopi O Clan War Day {day_number} Started\n"
            f"🔥 Good luck in battles!"
        )

    send_telegram_message(msg)


def check_war_day_started():
    previous_war = load_war_state() or {}
    current_war = get_war_phase()

    if current_war is None:
        log("Could not fetch current river race.")
        return

    previous_section = previous_war.get("sectionIndex")
    current_section = current_war.get("sectionIndex")

    previous_period_type = previous_war.get("periodType")
    current_period_type = current_war.get("periodType")

    previous_is_war_day = previous_war.get("is_war_day", False)
    current_is_war_day = current_war.get("is_war_day", False)

    current_is_colosseum = current_war.get("is_colosseum", False)

    # 🧪 Not a war day → just save state and exit
    if not current_is_war_day:
        save_war_state(current_war)
        log(
            f"No war alert sent. Training / non-war phase. "
            f"periodType={current_period_type}, sectionIndex={current_section}"
        )
        return

    # 📅 Calculate day number
    if current_section is None:
        log("Invalid sectionIndex received from API.")
        save_war_state(current_war)
        return

    if current_is_colosseum:
        current_day_number = current_section - 1
    else:
        current_day_number = current_section - 2

    # 🚨 Determine if alert should be sent
    should_alert = (
        not previous_is_war_day
        or previous_section != current_section
        or previous_period_type != current_period_type
    )

    # 📢 Send alert if needed
    if should_alert:
        send_clan_war_day_alert(
            day_number=current_day_number,
            is_colosseum=current_is_colosseum
        )
        log(
            f"War alert sent. "
            f"periodType={current_period_type}, "
            f"sectionIndex={current_section}, "
            f"day={current_day_number}"
        )

    # 💾 Save latest state
    save_war_state(current_war)


def check_role_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_role = previous_members[tag].get("role", "member")
        new_role = current_members[tag].get("role", "member")

        if old_role != new_role:
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
            log(f"Role change sent: {name} ({tag}) {old_role} -> {new_role}")


def has_role_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_role = previous_members[tag].get("role", "member")
        new_role = current_members[tag].get("role", "member")

        if old_role != new_role:
            return True

    return False


def check_path_of_legends_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {}) or {}
        new_pol = current_members[tag].get("path_of_legends", {}) or {}

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        old_trophies = old_pol.get("trophies")
        new_trophies = new_pol.get("trophies")

        old_step = old_pol.get("step")
        new_step = new_pol.get("step")

        old_rank = old_pol.get("rank")
        new_rank = new_pol.get("rank")

        if (
            old_league != new_league
            or old_trophies != new_trophies
            or old_step != new_step
            or old_rank != new_rank
        ):
            name = current_members[tag]["name"]
            send_path_of_legends_change_alert(
                name=name,
                tag=tag,
                old_pol=old_pol,
                new_pol=new_pol,
                member_count=len(current_members)
            )
            log(f"Path of Legends change sent: {name} ({tag})")


def has_path_of_legends_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {}) or {}
        new_pol = current_members[tag].get("path_of_legends", {}) or {}

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        old_trophies = old_pol.get("trophies")
        new_trophies = new_pol.get("trophies")

        old_step = old_pol.get("step")
        new_step = new_pol.get("step")

        old_rank = old_pol.get("rank")
        new_rank = new_pol.get("rank")

        if (
            old_league != new_league
            or old_trophies != new_trophies
            or old_step != new_step
            or old_rank != new_rank
        ):
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

        old_league_name = old_pol.get("league_name", "Unranked")
        new_league_name = new_pol.get("league_name", "Unranked")

        if old_league_name != new_league_name:
            pol_changed_players.append({
                "tag": tag,
                "name": name,
                "old_league_name": old_league_name,
                "new_league_name": new_league_name
            })

    joined_tags = set(current_members.keys()) - set(snapshot_members.keys())
    left_tags = set(snapshot_members.keys()) - set(current_members.keys())

    changed_players.sort(key=lambda x: (-x["diff"], x["name"].lower()))
    pol_changed_players.sort(key=lambda x: x["name"].lower())

    start_time_text = format_sgt_timestamp(snapshot_timestamp)
    end_time_text = datetime.now(BOT_TIMEZONE).strftime("%Y-%m-%d %H:%M:%S")

    total_changed_count = len(changed_players) + len(pol_changed_players)

    header = (
        f"📊 Kopi O 2 Hour Trophy Summary\n"
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
        lines.append("\n🏆 No trophy changes in the last 2 hours.")

    if pol_changed_players:
        lines.append("\n🎯 Path of Legends Changes")
        for i, player in enumerate(pol_changed_players, start=1):
            lines.append(
                f"{i}. {player['name']} {player['old_league_name']} → {player['new_league_name']}"
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
    log("2 hour trophy summary sent.")

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

            previous_members = current_members
            save_members(current_members)

        except Exception as e:
            log(f"Unexpected error: {e}")

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
