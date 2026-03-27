import json
import os
import time
import requests
import ctypes

CLASH_API_TOKEN = os.getenv("CLASH_API_TOKEN")
CLAN_TAG = "%23RPYC8P2Y"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CHECK_EVERY_SECONDS = 30
STATE_FILE = "clan_members_state.json"
MESSAGE_STATE_FILE = "telegram_message_state.json"
HOURLY_SNAPSHOT_FILE = "hourly_trophy_snapshot.json"
HOURLY_SUMMARY_SECONDS = 7200

CLASH_HEADERS = {
    "Authorization": f"Bearer {CLASH_API_TOKEN}"
}

ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")


def keep_awake():
    try:
        ctypes.windll.kernel32.SetThreadExecutionState(
            ES_CONTINUOUS | ES_SYSTEM_REQUIRED
        )
    except Exception as e:
        log(f"Keep-awake failed: {e}")


def clash_get(url):
    try:
        r = requests.get(url, headers=CLASH_HEADERS, timeout=20)
    except requests.RequestException as e:
        log(f"Clash API request error: {e}")
        return None

    if r.status_code != 200:
        log(f"Error fetching Clash API: {r.status_code} {r.text}")
        return None

    try:
        return r.json()
    except ValueError:
        log("Failed to parse Clash API response as JSON.")
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

    league_name = pol_data.get("league_name", "Unranked")
    return league_name


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


def load_previous_members():
    if not os.path.exists(STATE_FILE):
        return None

    try:
        with open(STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to load state file: {e}")
        return None


def save_members(members):
    try:
        with open(STATE_FILE, "w", encoding="utf-8") as f:
            json.dump(members, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"Failed to save state file: {e}")


def load_message_state():
    if not os.path.exists(MESSAGE_STATE_FILE):
        return None

    try:
        with open(MESSAGE_STATE_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to load message state file: {e}")
        return None


def save_message_state(message_id):
    try:
        with open(MESSAGE_STATE_FILE, "w", encoding="utf-8") as f:
            json.dump({"message_id": message_id}, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"Failed to save message state file: {e}")


def load_hourly_snapshot():
    if not os.path.exists(HOURLY_SNAPSHOT_FILE):
        return None

    try:
        with open(HOURLY_SNAPSHOT_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception as e:
        log(f"Failed to load hourly snapshot file: {e}")
        return None


def save_hourly_snapshot(members, snapshot_time=None):
    if snapshot_time is None:
        snapshot_time = time.time()

    data = {
        "timestamp": snapshot_time,
        "members": members
    }

    try:
        with open(HOURLY_SNAPSHOT_FILE, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=2)
    except Exception as e:
        log(f"Failed to save hourly snapshot file: {e}")


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
        log("Telegram full list message edited.")
        return True

    try:
        error_data = r.json()
        description = error_data.get("description", "")
    except ValueError:
        description = r.text

    if r.status_code == 400 and "message is not modified" in description.lower():
        log("Telegram message already up to date. No edit needed.")
        return True

    log(f"Telegram edit error: {r.status_code} {r.text}")
    return False


def build_full_clan_list_text(members):
    sorted_members = sorted(
        members.items(),
        key=lambda x: x[1]["trophies"],
        reverse=True
    )

    clan_score = get_clan_score()

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

    return header + "\n\n" + "\n\n".join(lines)


def send_full_clan_list(members):
    text = build_full_clan_list_text(members)
    return send_telegram_message(text)


def update_full_clan_list_message(members):
    text = build_full_clan_list_text(members)
    message_state = load_message_state()

    if message_state and message_state.get("message_id"):
        ok = edit_telegram_message(message_state["message_id"], text)
        if ok:
            return

        log("Edit failed. Sending a new full clan list message instead...")

    result = send_telegram_message(text)
    if result and result.get("ok") and result.get("result"):
        new_message_id = result["result"]["message_id"]
        save_message_state(new_message_id)
        log(f"Saved new full list message_id: {new_message_id}")


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
    old_text = format_path_of_legends(old_pol)
    new_text = format_path_of_legends(new_pol)

    msg = (
        f"🎯 Path of Legends changed in Kopi O\n"
        f"👤 {name}\n"
        f"🔄 {old_text} ➜ {new_text}\n"
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


def check_path_of_legends_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {})
        new_pol = current_members[tag].get("path_of_legends", {})

        old_league = old_pol.get("league_number")
        new_league = new_pol.get("league_number")

        old_trophies = old_pol.get("trophies")
        new_trophies = new_pol.get("trophies")

        old_step = old_pol.get("step")
        new_step = new_pol.get("step")

        if old_league != new_league or old_trophies != new_trophies or old_step != new_step:
            name = current_members[tag]["name"]
            send_path_of_legends_change_alert(
                name=name,
                tag=tag,
                old_pol=old_pol,
                new_pol=new_pol,
                member_count=len(current_members)
            )
            log(f"Path of Legends change sent: {name} ({tag})")


def has_trophy_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_trophies = previous_members[tag].get("trophies", 0)
        new_trophies = current_members[tag].get("trophies", 0)

        if old_trophies != new_trophies:
            return True

    return False


def has_path_of_legends_changes(previous_members, current_members):
    common_tags = set(previous_members.keys()) & set(current_members.keys())

    for tag in common_tags:
        old_pol = previous_members[tag].get("path_of_legends", {})
        new_pol = current_members[tag].get("path_of_legends", {})

        if old_pol.get("league_number") != new_pol.get("league_number"):
            return True

        if old_pol.get("trophies") != new_pol.get("trophies"):
            return True

        if old_pol.get("step") != new_pol.get("step"):
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

    start_time_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(snapshot_timestamp))
    end_time_text = time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())

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
        lines.append("\n🎉 Joined During This Hour")
        joined_names = sorted(current_members[tag]["name"] for tag in joined_tags)
        for i, name in enumerate(joined_names, start=1):
            lines.append(f"{i}. {name}")

    if left_tags:
        lines.append("\n🚪 Left During This Hour")
        left_names = sorted(snapshot_members[tag]["name"] for tag in left_tags)
        for i, name in enumerate(left_names, start=1):
            lines.append(f"{i}. {name}")

    return header + "\n" + "\n".join(lines)


def maybe_send_hourly_trophy_summary(current_members):
    snapshot_data = load_hourly_snapshot()

    if snapshot_data is None:
        save_hourly_snapshot(current_members)
        log("Hourly snapshot file created.")
        return

    snapshot_timestamp = snapshot_data.get("timestamp", 0)
    snapshot_members = snapshot_data.get("members", {})

    if not isinstance(snapshot_members, dict):
        snapshot_members = {}

    elapsed = time.time() - snapshot_timestamp

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
    log("Bot started...")

    previous_members = load_previous_members()
    current_members = get_clan_members()

    if current_members is None:
        log("Failed to fetch clan members on startup. Bot will not continue.")
        return

    if previous_members is None:
        log("First run detected. Sending full clan list to Telegram...")
        result = send_full_clan_list(current_members)

        if result and result.get("ok") and result.get("result"):
            message_id = result["result"]["message_id"]
            save_message_state(message_id)
            log(f"Saved full list message_id: {message_id}")

        save_members(current_members)
        save_hourly_snapshot(current_members)
        log("Current members saved.")
        previous_members = current_members
    else:
        update_full_clan_list_message(current_members)
        save_members(current_members)

        if load_hourly_snapshot() is None:
            save_hourly_snapshot(current_members)

        previous_members = current_members
        log("Full clan list refreshed on startup.")

    while True:
        try:
            keep_awake()
            current_members = get_clan_members()

            if current_members is None:
                log("Skipping this cycle because clan data could not be fetched.")
                time.sleep(CHECK_EVERY_SECONDS)
                continue

            joined = set(current_members.keys()) - set(previous_members.keys())
            left = set(previous_members.keys()) - set(current_members.keys())
            trophy_changed = has_trophy_changes(previous_members, current_members)
            pol_changed = has_path_of_legends_changes(previous_members, current_members)

            log(f"Checked clan. Current members: {len(current_members)}")

            if joined:
                send_join_alerts(joined, current_members)

            if left:
                send_leave_alerts(left, previous_members, len(current_members))

            check_role_changes(previous_members, current_members)
            check_path_of_legends_changes(previous_members, current_members)
            send_clan_capacity_alert(len(previous_members), len(current_members))

            if joined or left or trophy_changed or pol_changed:
                update_full_clan_list_message(current_members)

            maybe_send_hourly_trophy_summary(current_members)

            previous_members = current_members
            save_members(current_members)

        except Exception as e:
            log(f"Unexpected error: {e}")

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
