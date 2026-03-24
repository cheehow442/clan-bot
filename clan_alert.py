import json
import os
import time
import requests

CLASH_API_TOKEN = os.getenv("CLASH_API_TOKEN")
CLAN_TAG = "%23RPYC8P2Y"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CHECK_EVERY_SECONDS = 10
STATE_FILE = "clan_members_state.json"

CLASH_HEADERS = {
    "Authorization": f"Bearer {CLASH_API_TOKEN}"
}


def log(message):
    print(f"[{time.strftime('%Y-%m-%d %H:%M:%S')}] {message}")


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


def get_clan_members():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}"
    data = clash_get(url)

    if not data:
        return None

    members = data.get("memberList", [])

    return {
        m["tag"]: {
            "name": m["name"],
            "trophies": m.get("trophies", 0),
            "role": m.get("role", "member")
        }
        for m in members
    }


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
        return

    if r.status_code != 200:
        log(f"Telegram error: {r.status_code} {r.text}")
    else:
        log("Telegram message sent.")


def send_long_message(lines, header="", max_len=3500):
    message = header.strip()
    if message:
        message += "\n\n"

    for line in lines:
        if len(message) + len(line) + 1 > max_len:
            send_telegram_message(message.strip())
            message = ""
        message += line + "\n"

    if message.strip():
        send_telegram_message(message.strip())


def send_full_clan_list(members):
    sorted_members = sorted(
        members.items(),
        key=lambda x: x[1]["trophies"],
        reverse=True
    )

    lines = []
    for i, (tag, info) in enumerate(sorted_members, start=1):
        lines.append(
            f"{i}. {info['name']} - {info['trophies']}🏆 - {info['role']} ({tag})"
        )

    header = f"📋 Kopi O current clan members ({len(sorted_members)}/50)"
    send_long_message(lines, header=header)


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

        msg = (
            f"🎉 Member joined Kopi O\n"
            f"👤 {name}\n"
            f"🏆 {trophies} trophies\n"
            f"🛡️ Role: {role}\n"
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

        msg = (
            f"🚪 Member left Kopi O\n"
            f"👤 {name}\n"
            f"🏆 {trophies} trophies\n"
            f"🛡️ Role: {role}\n"
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

    log("Bot started...")

    previous_members = load_previous_members()
    current_members = get_clan_members()

    if current_members is None:
        log("Failed to fetch clan members on startup. Bot will not continue.")
        return

    # Only send full list on the very first run
    if previous_members is None:
        log("First run detected. Sending full clan list to Telegram...")
        send_full_clan_list(current_members)
        save_members(current_members)
        log("Current members saved.")
        previous_members = current_members

    while True:
        try:
            current_members = get_clan_members()

            if current_members is None:
                log("Skipping this cycle because clan data could not be fetched.")
                time.sleep(CHECK_EVERY_SECONDS)
                continue

            joined = set(current_members.keys()) - set(previous_members.keys())
            left = set(previous_members.keys()) - set(current_members.keys())

            log(f"Checked clan. Current members: {len(current_members)}")

            if joined:
                send_join_alerts(joined, current_members)

            if left:
                send_leave_alerts(left, previous_members, len(current_members))

            check_role_changes(previous_members, current_members)
            send_clan_capacity_alert(len(previous_members), len(current_members))

            previous_members = current_members
            save_members(current_members)

        except Exception as e:
            log(f"Unexpected error: {e}")

        time.sleep(CHECK_EVERY_SECONDS)


if __name__ == "__main__":
    main()
