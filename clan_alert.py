import json
import os
import time
import requests

CLASH_API_TOKEN = os.getenv("CLASH_API_TOKEN")
CLAN_TAG = "%23RPYC8P2Y"
TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")

CHECK_EVERY_SECONDS = 10
STATE_FILE = "members.json"
SEND_FULL_LIST_ON_EVERY_START = True

CLASH_HEADERS = {
    "Authorization": f"Bearer {CLASH_API_TOKEN}"
}

def get_clan_members():
    url = f"https://api.clashroyale.com/v1/clans/{CLAN_TAG}"
    r = requests.get(url, headers=CLASH_HEADERS, timeout=20)

    if r.status_code != 200:
        print("Error fetching clan:", r.text)
        return {}

    data = r.json()
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

    with open(STATE_FILE, "r", encoding="utf-8") as f:
        return json.load(f)

def save_members(members):
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(members, f, ensure_ascii=False, indent=2)

def send_telegram_message(text):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    r = requests.post(
        url,
        data={
            "chat_id": TELEGRAM_CHAT_ID,
            "text": text
        },
        timeout=20
    )

    if r.status_code != 200:
        print("Telegram error:", r.text)
    else:
        print("Telegram message sent.")

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

def main():
    if not CLASH_API_TOKEN:
        print("Missing CLASH_API_TOKEN")
        return
    if not TELEGRAM_BOT_TOKEN:
        print("Missing TELEGRAM_BOT_TOKEN")
        return
    if not TELEGRAM_CHAT_ID:
        print("Missing TELEGRAM_CHAT_ID")
        return

    print("Bot started...")
    previous_members = load_previous_members()
    current_members = get_clan_members()

    if SEND_FULL_LIST_ON_EVERY_START:
        print("Sending full clan list to Telegram...")
        send_full_clan_list(current_members)

    if previous_members is None:
        save_members(current_members)
        print("First run detected. Current members saved.")
        previous_members = current_members

    while True:
        try:
            current_members = get_clan_members()

            joined = set(current_members.keys()) - set(previous_members.keys())
            left = set(previous_members.keys()) - set(current_members.keys())

            print(f"Checked clan. Current members: {len(current_members)}")

            for tag in joined:
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
                print(f"Join sent: {name} ({tag})")

            for tag in left:
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
                    f"📊 Members: {len(current_members)}/50"
                )
                send_telegram_message(msg)
                print(f"Leave sent: {name} ({tag})")

            previous_members = current_members
            save_members(current_members)

        except Exception as e:
            print("Error:", e)

        time.sleep(CHECK_EVERY_SECONDS)

if __name__ == "__main__":
    main()