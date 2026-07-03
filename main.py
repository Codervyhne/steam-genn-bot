import asyncio
import json
import os
import random
import shutil
import time
from datetime import datetime
from urllib import error as urlerror
from urllib import request as urlrequest
import discord
from discord import app_commands
from discord.ext import commands, tasks

# â”€â”€ Config â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
FREE_CHANNEL_ID          = 1521710736093614189
PREMIUM_CHANNEL_ID       = 1521710737658089545
LOG_CHANNEL_ID           = 1521710737142317232   # generation logs & stock alerts
STOCK_MONITOR_CHANNEL_ID = 1521710736634548384   # live stock counter + restock notifs
PREMIUM_ROLE_ID          = 1521710733992267788   # required to use premium gen
BOOSTER_PREMIUM_ROLE_ID  = 1521938810076790815   # temporary premium for boosters
ALERT_ROLE_ID            = 1521710733992267789   # pinged on low stock / logs
GUARD_CHANNEL_ID         = 1521710735091044512   # no-talk channel â€“ speakers get kicked
INVITE_LINK              = "https://discord.gg/pdfFPjG5gr"
TOKEN = os.getenv("DISCORD_TOKEN") or os.getenv("TOKEN")

STOCK_ALERT_THRESHOLD = 5
RESTOCK_MIN_ACCOUNTS  = 100   # min accounts copied from pool per restock
RESTOCK_MAX_ACCOUNTS  = 500   # max accounts copied from pool per restock
RESTOCK_MIN_DELAY     = 60    # seconds (1 min)
RESTOCK_MAX_DELAY     = 300   # seconds (5 min)
BOOSTER_PREMIUM_SECONDS = 3 * 24 * 60 * 60
BASE_COOLDOWN_SECONDS = {"free": 600, "prem": 300}

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.getenv("DATA_DIR", BASE_DIR)
os.makedirs(DATA_DIR, exist_ok=True)


def data_file(name: str) -> str:
    path = os.path.join(DATA_DIR, name)
    seed_path = os.path.join(BASE_DIR, name)
    if DATA_DIR != BASE_DIR and not os.path.exists(path) and os.path.exists(seed_path):
        shutil.copyfile(seed_path, path)
    return path


COOLDOWN_FILE      = data_file("cooldowns.json")
STATS_FILE         = data_file("stats.json")
MONITOR_FILE       = data_file("monitor.json")
BLACKLIST_FILE     = data_file("blacklist.json")
NOTIFY_FILE        = data_file("notifications.json")
BOOST_SUBS_FILE    = data_file("boost_subscriptions.json")
GEN_HISTORY_FILE   = data_file("gen_history.json")
FREE_STOCK_FILE    = data_file("freestock.txt")
PREMIUM_STOCK_FILE = data_file("premstock.txt")
STOCK_POOL_FILE    = data_file("stockpool.txt")   # large pool used for auto-restock (never modified)

# â”€â”€ Locks â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
STOCK_LOCK      = asyncio.Lock()
PREM_STOCK_LOCK = asyncio.Lock()

# â”€â”€ Persistent state â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
free_cooldowns: dict[str, float] = {}   # str(user_id) -> expiry timestamp
prem_cooldowns: dict[str, float] = {}
stats_data: dict[str, int]       = {"free_generated": 0, "prem_generated": 0}
stock_monitor_msg_id: int | None = None
blacklist_data: dict[str, list]  = {"free": [], "prem": []}  # lists of int user IDs
blacklist_reasons: dict[str, dict] = {"free": {}, "prem": {}}
gen_history: list[dict] = []
GEN_HISTORY_LIMIT = 500
notification_data = {
    "webhook_url": "",
    "events": {"stock_zero": False, "auto_restock": False}
}
zero_stock_notified = {"free": False, "prem": False}
boost_subscriptions: dict[str, dict] = {}
cooldown_config = {
    "defaults": {
        "free": {"seconds": BASE_COOLDOWN_SECONDS["free"], "expires_at": None},
        "prem": {"seconds": BASE_COOLDOWN_SECONDS["prem"], "expires_at": None},
    },
    "users": {"free": {}, "prem": {}},
    "blocks": {"free": {}, "prem": {}},
}

# â”€â”€ Restock pending flags (prevent duplicate tasks) â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€
restock_pending = {"free": False, "prem": False}


# â”€â”€ Persistence helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€


def merge_cooldown_config(loaded: dict) -> dict:
    config = {
        "defaults": {
            "free": {"seconds": BASE_COOLDOWN_SECONDS["free"], "expires_at": None},
            "prem": {"seconds": BASE_COOLDOWN_SECONDS["prem"], "expires_at": None},
        },
        "users": {"free": {}, "prem": {}},
        "blocks": {"free": {}, "prem": {}},
    }
    for section in ("defaults", "users", "blocks"):
        source = loaded.get(section, {})
        if isinstance(source, dict):
            for tier in ("free", "prem"):
                value = source.get(tier)
                if isinstance(value, dict):
                    config[section][tier].update(value)
    return config


def cooldown_tier(value: str) -> str | None:
    text = str(value or "").lower().strip()
    if text == "free":
        return "free"
    if text in ("prem", "premium"):
        return "prem"
    return None


def cooldown_tier_label(tier: str) -> str:
    return "Free" if tier == "free" else "Premium"


def duration_text(seconds: int) -> str:
    seconds = max(0, int(seconds))
    days, rem = divmod(seconds, 86400)
    hours, rem = divmod(rem, 3600)
    minutes, secs = divmod(rem, 60)
    parts = []
    if days:
        parts.append(f"{days}d")
    if hours:
        parts.append(f"{hours}h")
    if minutes:
        parts.append(f"{minutes}m")
    if secs or not parts:
        parts.append(f"{secs}s")
    return " ".join(parts)


def cleanup_cooldown_config() -> bool:
    changed = False
    now = time.time()
    for tier in ("free", "prem"):
        default = cooldown_config["defaults"].get(tier, {})
        expires_at = default.get("expires_at")
        if expires_at and now >= float(expires_at):
            cooldown_config["defaults"][tier] = {
                "seconds": BASE_COOLDOWN_SECONDS[tier],
                "expires_at": None,
            }
            changed = True

        for section in ("users", "blocks"):
            records = cooldown_config[section].get(tier, {})
            for uid, record in list(records.items()):
                expires_at = record.get("expires_at") if isinstance(record, dict) else None
                if expires_at and now >= float(expires_at):
                    records.pop(uid, None)
                    changed = True
    return changed


def get_generation_cooldown_seconds(tier: str, user_id: int) -> int:
    if cleanup_cooldown_config():
        save_cooldowns()
    uid = str(user_id)
    user_record = cooldown_config["users"].get(tier, {}).get(uid)
    if isinstance(user_record, dict):
        return max(0, int(user_record.get("seconds", BASE_COOLDOWN_SECONDS[tier])))
    default_record = cooldown_config["defaults"].get(tier, {})
    return max(0, int(default_record.get("seconds", BASE_COOLDOWN_SECONDS[tier])))


def get_cooldown_block(tier: str, user_id: int) -> dict | None:
    if cleanup_cooldown_config():
        save_cooldowns()
    return cooldown_config["blocks"].get(tier, {}).get(str(user_id))


def cooldown_expiry(minutes: int | None) -> float | None:
    if not minutes or minutes <= 0:
        return None
    return time.time() + (minutes * 60)


def expiry_text(expires_at: float | int | None) -> str:
    if not expires_at:
        return "permanent"
    return f"until <t:{int(float(expires_at))}:F> (<t:{int(float(expires_at))}:R>)"


def blacklist_reason(tier: str, user_id: int) -> str | None:
    record = blacklist_reasons.get(tier, {}).get(str(user_id))
    if isinstance(record, dict):
        return record.get("reason")
    if isinstance(record, str):
        return record
    return None


def blacklist_message(tier: str, user_id: int) -> str:
    reason = blacklist_reason(tier, user_id)
    label = "free" if tier == "free" else "premium"
    if reason:
        return f"You are blacklisted from the {label} generator. Reason: {reason}"
    return f"You are blacklisted from the {label} generator."


def add_generation_history(tier: str, user: discord.User, username: str, password: str):
    gen_history.append({
        "tier": tier,
        "user_id": user.id,
        "username": user.name,
        "display_name": getattr(user, "display_name", user.name),
        "account_username": username,
        "account_password": password,
        "generated_at": time.time(),
    })
    if len(gen_history) > GEN_HISTORY_LIMIT:
        del gen_history[:-GEN_HISTORY_LIMIT]
    save_gen_history()
def load_state():
    """Load all persisted state from disk on startup."""
    global free_cooldowns, prem_cooldowns, stats_data, blacklist_data, blacklist_reasons, gen_history, notification_data, boost_subscriptions, cooldown_config

    if os.path.exists(COOLDOWN_FILE):
        try:
            with open(COOLDOWN_FILE, "r", encoding="utf-8") as f:
                data = json.load(f)
            free_cooldowns = data.get("free", {})
            prem_cooldowns = data.get("prem", {})
            loaded_config = data.get("config")
            if isinstance(loaded_config, dict):
                cooldown_config = merge_cooldown_config(loaded_config)
        except Exception:
            pass

    if os.path.exists(STATS_FILE):
        try:
            with open(STATS_FILE, "r", encoding="utf-8") as f:
                stats_data = json.load(f)
        except Exception:
            pass

    if os.path.exists(BLACKLIST_FILE):
        try:
            with open(BLACKLIST_FILE, "r", encoding="utf-8") as f:
                loaded_blacklist = json.load(f)
                if isinstance(loaded_blacklist, dict):
                    blacklist_data["free"] = loaded_blacklist.get("free", [])
                    blacklist_data["prem"] = loaded_blacklist.get("prem", [])
                    loaded_reasons = loaded_blacklist.get("reasons", {})
                    if isinstance(loaded_reasons, dict):
                        blacklist_reasons["free"] = loaded_reasons.get("free", {})
                        blacklist_reasons["prem"] = loaded_reasons.get("prem", {})
        except Exception:
            pass

    if os.path.exists(NOTIFY_FILE):
        try:
            with open(NOTIFY_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if "events" in loaded:
                notification_data["webhook_url"] = loaded.get("webhook_url", "")
                notification_data["events"]["stock_zero"] = bool(
                    loaded.get("events", {}).get("stock_zero", False)
                )
                notification_data["events"]["auto_restock"] = bool(
                    loaded.get("events", {}).get("auto_restock", False)
                )
            else:
                notification_data["events"]["stock_zero"] = bool(loaded.get("stock_zero"))
                notification_data["events"]["auto_restock"] = bool(loaded.get("auto_restock"))
        except Exception:
            pass

    if os.path.exists(BOOST_SUBS_FILE):
        try:
            with open(BOOST_SUBS_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, dict):
                boost_subscriptions = loaded
        except Exception:
            pass

    if os.path.exists(GEN_HISTORY_FILE):
        try:
            with open(GEN_HISTORY_FILE, "r", encoding="utf-8") as f:
                loaded = json.load(f)
            if isinstance(loaded, list):
                gen_history = loaded[-GEN_HISTORY_LIMIT:]
        except Exception:
            pass


def save_cooldowns():
    with open(COOLDOWN_FILE, "w", encoding="utf-8") as f:
        json.dump({"free": free_cooldowns, "prem": prem_cooldowns, "config": cooldown_config}, f, indent=2)


def save_stats():
    with open(STATS_FILE, "w", encoding="utf-8") as f:
        json.dump(stats_data, f)


def save_blacklist():
    with open(BLACKLIST_FILE, "w", encoding="utf-8") as f:
        json.dump({"free": blacklist_data.get("free", []), "prem": blacklist_data.get("prem", []), "reasons": blacklist_reasons}, f, indent=2)


def save_notifications():
    with open(NOTIFY_FILE, "w", encoding="utf-8") as f:
        json.dump(notification_data, f)


def save_boost_subscriptions():
    with open(BOOST_SUBS_FILE, "w", encoding="utf-8") as f:
        json.dump(boost_subscriptions, f, indent=2)


def save_gen_history():
    with open(GEN_HISTORY_FILE, "w", encoding="utf-8") as f:
        json.dump(gen_history[-GEN_HISTORY_LIMIT:], f, indent=2)


def save_monitor_id(msg_id: int):
    with open(MONITOR_FILE, "w", encoding="utf-8") as f:
        json.dump({"message_id": msg_id}, f)


def load_monitor_id() -> int | None:
    if not os.path.exists(MONITOR_FILE):
        return None
    try:
        with open(MONITOR_FILE, "r", encoding="utf-8") as f:
            return json.load(f).get("message_id")
    except Exception:
        return None


# â”€â”€ Stock helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def get_stock_count():
    filename = FREE_STOCK_FILE
    if not os.path.exists(filename) or os.stat(filename).st_size == 0:
        return 0
    try:
        with open(filename, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        return len([l for l in lines if l.count(":") == 1])
    except Exception:
        return 0


def get_premium_stock_count():
    filename = PREMIUM_STOCK_FILE
    if not os.path.exists(filename) or os.stat(filename).st_size == 0:
        return 0
    try:
        with open(filename, "r", encoding="utf-8") as f:
            lines = f.read().splitlines()
        return len([l for l in lines if l.count(":") == 1])
    except Exception:
        return 0


# â”€â”€ Stock monitor helpers â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def build_stock_embed() -> discord.Embed:
    """Build the live stock-count embed."""
    free = get_stock_count()
    prem = get_premium_stock_count()
    embed = discord.Embed(
        title="Stock",
        color=discord.Color.green(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="Free Stock",    value=f"**{free}** account(s)", inline=True)
    embed.add_field(name="Premium Stock", value=f"**{prem}** account(s)", inline=True)
    embed.set_footer(text="Last updated")
    return embed


async def update_stock_monitor(bot: commands.Bot):
    """Edit the pinned stock-monitor message with fresh counts."""
    global stock_monitor_msg_id
    channel = bot.get_channel(STOCK_MONITOR_CHANNEL_ID)
    if not channel:
        return
    embed = build_stock_embed()
    if stock_monitor_msg_id:
        try:
            msg = await channel.fetch_message(stock_monitor_msg_id)
            await msg.edit(embed=embed)
            return
        except (discord.NotFound, discord.HTTPException):
            stock_monitor_msg_id = None  # gone â€“ post fresh
    msg = await channel.send(embed=embed)
    stock_monitor_msg_id = msg.id
    save_monitor_id(msg.id)


def notification_enabled(event_name: str) -> bool:
    return bool(notification_data.get("events", {}).get(event_name, False))


async def send_event_notifications(bot: commands.Bot, event_name: str,
                                   title: str, description: str):
    """Send enabled events to a non-Discord webhook notification target."""
    if not notification_enabled(event_name):
        return

    webhook_url = notification_data.get("webhook_url", "")
    if not webhook_url:
        return

    payload = {
        "event": event_name,
        "title": title,
        "description": description,
        "free_stock": get_stock_count(),
        "premium_stock": get_premium_stock_count(),
        "timestamp": datetime.utcnow().isoformat() + "Z",
    }

    def post_webhook():
        data = json.dumps(payload).encode("utf-8")
        req = urlrequest.Request(
            webhook_url,
            data=data,
            headers={"Content-Type": "application/json"},
            method="POST"
        )
        with urlrequest.urlopen(req, timeout=10) as response:
            response.read()

    try:
        await asyncio.to_thread(post_webhook)
    except (urlerror.URLError, TimeoutError, OSError) as exc:
        print(f"Notification webhook failed for {event_name}: {exc}")


async def notify_stock_zero(bot: commands.Bot, tier: str):
    if zero_stock_notified[tier]:
        return
    zero_stock_notified[tier] = True
    label = "Free" if tier == "free" else "Premium"
    await send_event_notifications(
        bot,
        "stock_zero",
        f"{label} Stock Reached Zero",
        f"{label} stock is empty. Auto-restock has been queued."
    )


# â”€â”€ Auto-restock â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def auto_restock(bot: commands.Bot, tier: str):
    """
    Wait a random 1â€“5 min delay, then copy 100â€“500 random accounts from
    stockpool.txt into the depleted stock file (pool is never modified).
    Posts a notification in the stock monitor channel when done.
    """
    delay = random.randint(RESTOCK_MIN_DELAY, RESTOCK_MAX_DELAY)
    await asyncio.sleep(delay)

    filename = FREE_STOCK_FILE if tier == "free" else PREMIUM_STOCK_FILE
    lock     = STOCK_LOCK      if tier == "free" else PREM_STOCK_LOCK

    # Read pool (read-only, never modified)
    if not os.path.exists(STOCK_POOL_FILE) or os.stat(STOCK_POOL_FILE).st_size == 0:
        restock_pending[tier] = False
        return

    with open(STOCK_POOL_FILE, "r", encoding="utf-8") as f:
        pool = [l.strip() for l in f.read().splitlines() if l.strip().count(":") == 1]

    if not pool:
        restock_pending[tier] = False
        return

    count    = min(random.randint(RESTOCK_MIN_ACCOUNTS, RESTOCK_MAX_ACCOUNTS), len(pool))
    selected = random.sample(pool, count)

    async with lock:
        existing = ""
        if os.path.exists(filename) and os.stat(filename).st_size > 0:
            with open(filename, "r", encoding="utf-8") as f:
                existing = f.read()
        with open(filename, "a", encoding="utf-8") as f:
            if existing and not existing.endswith("\n"):
                f.write("\n")
            f.write("\n".join(selected) + "\n")

    restock_pending[tier] = False
    zero_stock_notified[tier] = False

    # Notification in stock monitor channel
    channel = bot.get_channel(STOCK_MONITOR_CHANNEL_ID)
    if channel:
        label     = "Free" if tier == "free" else "Premium"
        new_count = get_stock_count() if tier == "free" else get_premium_stock_count()
        embed = discord.Embed(
            title=f"ðŸ”„ {label} Restocked ðŸ”„",
            description=(
                f"**{count}** accounts were added to **{label}** stock "
            ),
            color=discord.Color.blue(),
            timestamp=datetime.utcnow()
        )
        embed.add_field(name="New Stock", value=f"**{new_count}** account(s)", inline=False)
        await channel.send(embed=embed)

    label = "Free" if tier == "free" else "Premium"
    await send_event_notifications(
        bot,
        "auto_restock",
        f"ðŸ”„ {label} Restocked",
        f"**{count}** accounts were added to **{label}** stock "
    )

    await update_stock_monitor(bot)


def trigger_restock_if_needed(bot: commands.Bot, tier: str):
    """Kick off an auto-restock task if stock is 0 and none is already pending."""
    count = get_stock_count() if tier == "free" else get_premium_stock_count()
    if count == 0 and not restock_pending[tier]:
        restock_pending[tier] = True
        asyncio.create_task(notify_stock_zero(bot, tier))
        asyncio.create_task(auto_restock(bot, tier))


# â”€â”€ Logging & alerts â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

async def send_log(bot: commands.Bot, tier: str, user: discord.User,
                   username: str, password: str):
    """Post a generation log embed to the log channel."""
    channel = bot.get_channel(LOG_CHANNEL_ID)
    if not channel:
        return
    color = discord.Color.blue() if tier == "free" else discord.Color.gold()
    embed = discord.Embed(
        title=f"{'Free' if tier == 'free' else 'Premium'} Account Generated",
        color=color,
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="User",        value=f"{user.mention} (`{user.id}`)", inline=True)
    embed.add_field(name="Tier",        value=tier.capitalize(),               inline=True)
    embed.add_field(name="Credentials", value=f"`{username}:{password}`",      inline=False)
    embed.set_footer(
        text=f"Free stock: {get_stock_count()} | Premium stock: {get_premium_stock_count()}"
    )
    await channel.send(embed=embed)


async def check_and_alert(bot: commands.Bot, tier: str):
    """Ping the alert role if stock is at or below threshold."""
    count = get_stock_count() if tier == "free" else get_premium_stock_count()
    if count <= STOCK_ALERT_THRESHOLD:
        channel = bot.get_channel(LOG_CHANNEL_ID)
        if not channel:
            return
        role    = channel.guild.get_role(ALERT_ROLE_ID)
        mention = role.mention if role else ""
        label   = "Free" if tier == "free" else "Premium"
        embed   = discord.Embed(
            title       = f"ðŸš¨ Low {label} Stock ðŸš¨",
            description = f"{label} stock has dropped to **{count}** account(s).",
            color       = discord.Color.red(),
            timestamp   = datetime.utcnow()
        )
        await channel.send(content=mention, embed=embed)


# â”€â”€ Copy view â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class CopyView(discord.ui.View):
    def __init__(self, raw_credentials: str):
        super().__init__(timeout=None)
        self.raw_credentials = raw_credentials

    @discord.ui.button(
        label="Isolate username and password",
        style=discord.ButtonStyle.gray,
        custom_id="copy_credentials_button"
    )
    async def copy_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        await interaction.response.send_message(
            f"`{self.raw_credentials}`",
            ephemeral=True
        )


# â”€â”€ Free generator â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class GeneratorView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def update_panel_stock(self, message: discord.Message):
        if message and message.embeds:
            embed = message.embeds[0]
            count = get_stock_count()
            embed.description = (
                f"Click the button below to generate a free account.\n\n"
                f"**Cooldown:** 10 minutes\n**Current Stock:** {count}"
            )
            await message.edit(embed=embed, view=self)

    @discord.ui.button(
        label="Generate",
        style=discord.ButtonStyle.green,
        custom_id="persistent_generate_button"
    )
    async def generate_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id      = interaction.user.id
        uid_str      = str(user_id)
        current_time = time.time()
        cooldown_duration = get_generation_cooldown_seconds("free", user_id)

        # â”€â”€ Blacklist check â”€â”€
        if user_id in blacklist_data.get("free", []):
            await interaction.response.send_message(
                blacklist_message("free", user_id), ephemeral=True
            )
            return

        # â”€â”€ Cooldown check â”€â”€
        if uid_str in free_cooldowns:
            remaining = free_cooldowns[uid_str] - current_time
            if remaining > 0:
                minutes = int(remaining // 60)
                seconds = int(remaining % 60)
                await interaction.response.send_message(
                    f"You must wait **{minutes}m {seconds}s** before generating another free account.",
                    ephemeral=True
                )
                return

        await interaction.response.defer(ephemeral=True)

        filename = FREE_STOCK_FILE
        account_selected = None

        async with STOCK_LOCK:
            if not os.path.exists(filename) or os.stat(filename).st_size == 0:
                await interaction.followup.send("Free stock is currently empty.", ephemeral=True)
                await self.update_panel_stock(interaction.message)
                trigger_restock_if_needed(interaction.client, "free")
                return

            with open(filename, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()

            valid_accounts = [l for l in lines if l.count(":") == 1]
            if not valid_accounts:
                await interaction.followup.send("Free stock is currently empty.", ephemeral=True)
                await self.update_panel_stock(interaction.message)
                trigger_restock_if_needed(interaction.client, "free")
                return

            account_selected = random.choice(valid_accounts)
            lines.remove(account_selected)

            with open(filename, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + ("\n" if lines else ""))

        username, password = account_selected.split(":")

        dm_embed = discord.Embed(
            title="Your Free Account",
            description="Here are your generated credentials:",
            color=discord.Color.green()
        )
        dm_embed.add_field(
            name="Credentials",
            value=f"Username: `{username}`\nPassword: `{password}`",
            inline=False
        )
        dm_embed.set_footer(text="Please keep in mind not all accounts work.")

        try:
            copy_view = CopyView(raw_credentials=account_selected)
            await interaction.user.send(embed=dm_embed, view=copy_view)

            free_cooldowns[uid_str] = time.time() + cooldown_duration
            save_cooldowns()
            stats_data["free_generated"] += 1
            save_stats()
            add_generation_history("free", interaction.user, username, password)

            await interaction.followup.send("Your account has been sent to your DMs!", ephemeral=True)
            await self.update_panel_stock(interaction.message)
            await send_log(interaction.client, "free", interaction.user, username, password)
            await check_and_alert(interaction.client, "free")
            await update_stock_monitor(interaction.client)
            trigger_restock_if_needed(interaction.client, "free")

        except discord.Forbidden:
            async with STOCK_LOCK:
                existing_content = ""
                if os.path.exists(filename) and os.stat(filename).st_size > 0:
                    with open(filename, "r", encoding="utf-8") as f:
                        existing_content = f.read()
                with open(filename, "w", encoding="utf-8") as f:
                    if existing_content and not existing_content.endswith("\n"):
                        f.write(existing_content + "\n" + account_selected + "\n")
                    else:
                        f.write(existing_content + account_selected + "\n")

            await interaction.followup.send(
                "I couldn't send you a DM. Please enable Direct Messages and try again.",
                ephemeral=True
            )
            await self.update_panel_stock(interaction.message)


# â”€â”€ Premium generator â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

class PremiumGeneratorView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def update_panel_stock(self, message: discord.Message):
        if message and message.embeds:
            embed = message.embeds[0]
            count = get_premium_stock_count()
            embed.description = (
                f"Click the button below to generate a premium account.\n\n"
                f"**Cooldown:** 5 minutes\n**Current Stock:** {count}"
            )
            await message.edit(embed=embed, view=self)

    @discord.ui.button(
        label="Generate",
        style=discord.ButtonStyle.blurple,
        custom_id="persistent_premium_generate_button"
    )
    async def generate_button(self, interaction: discord.Interaction, button: discord.ui.Button):
        user_id      = interaction.user.id
        uid_str      = str(user_id)
        current_time = time.time()
        cooldown_duration = get_generation_cooldown_seconds("prem", user_id)

        # â”€â”€ Blacklist check â”€â”€
        if user_id in blacklist_data.get("prem", []):
            await interaction.response.send_message(
                blacklist_message("prem", user_id), ephemeral=True
            )
            return

        # â”€â”€ Role gate â”€â”€
        if isinstance(interaction.user, discord.Member):
            if not any(r.id in (PREMIUM_ROLE_ID, BOOSTER_PREMIUM_ROLE_ID) for r in interaction.user.roles):
                await interaction.response.send_message(
                    "You need the **Premium** role to use this generator.",
                    ephemeral=True
                )
                return

        # â”€â”€ Cooldown check â”€â”€
        if uid_str in prem_cooldowns:
            remaining = prem_cooldowns[uid_str] - current_time
            if remaining > 0:
                minutes = int(remaining // 60)
                seconds = int(remaining % 60)
                await interaction.response.send_message(
                    f"You must wait **{minutes}m {seconds}s** before generating another premium account.",
                    ephemeral=True
                )
                return

        await interaction.response.defer(ephemeral=True)

        filename = PREMIUM_STOCK_FILE
        account_selected = None

        async with PREM_STOCK_LOCK:
            if not os.path.exists(filename) or os.stat(filename).st_size == 0:
                await interaction.followup.send("Premium stock is currently empty.", ephemeral=True)
                await self.update_panel_stock(interaction.message)
                trigger_restock_if_needed(interaction.client, "prem")
                return

            with open(filename, "r", encoding="utf-8") as f:
                lines = f.read().splitlines()

            valid_accounts = [l for l in lines if l.count(":") == 1]
            if not valid_accounts:
                await interaction.followup.send("Premium stock is currently empty.", ephemeral=True)
                await self.update_panel_stock(interaction.message)
                trigger_restock_if_needed(interaction.client, "prem")
                return

            account_selected = random.choice(valid_accounts)
            lines.remove(account_selected)

            with open(filename, "w", encoding="utf-8") as f:
                f.write("\n".join(lines) + ("\n" if lines else ""))

        username, password = account_selected.split(":")

        dm_embed = discord.Embed(
            title="Your Premium Account",
            description="Here are your generated credentials:",
            color=discord.Color.gold()
        )
        dm_embed.add_field(
            name="Credentials",
            value=f"Username: `{username}`\nPassword: `{password}`",
            inline=False
        )
        dm_embed.set_footer(text="Please keep in mind not all accounts work.")

        try:
            copy_view = CopyView(raw_credentials=account_selected)
            await interaction.user.send(embed=dm_embed, view=copy_view)

            prem_cooldowns[uid_str] = time.time() + cooldown_duration
            save_cooldowns()
            stats_data["prem_generated"] += 1
            save_stats()
            add_generation_history("premium", interaction.user, username, password)

            await interaction.followup.send("Your premium account has been sent to your DMs!", ephemeral=True)
            await self.update_panel_stock(interaction.message)
            await send_log(interaction.client, "premium", interaction.user, username, password)
            await check_and_alert(interaction.client, "premium")
            await update_stock_monitor(interaction.client)
            trigger_restock_if_needed(interaction.client, "prem")

        except discord.Forbidden:
            async with PREM_STOCK_LOCK:
                existing_content = ""
                if os.path.exists(filename) and os.stat(filename).st_size > 0:
                    with open(filename, "r", encoding="utf-8") as f:
                        existing_content = f.read()
                with open(filename, "w", encoding="utf-8") as f:
                    if existing_content and not existing_content.endswith("\n"):
                        f.write(existing_content + "\n" + account_selected + "\n")
                    else:
                        f.write(existing_content + account_selected + "\n")

            await interaction.followup.send(
                "I couldn't send you a DM. Please enable Direct Messages and try again.",
                ephemeral=True
            )
            await self.update_panel_stock(interaction.message)


# â”€â”€ Bot â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def build_stats_dashboard_embed() -> discord.Embed:
    total = stats_data["free_generated"] + stats_data["prem_generated"]
    stock_zero = "ON" if notification_enabled("stock_zero") else "OFF"
    auto_restock_status = "ON" if notification_enabled("auto_restock") else "OFF"
    webhook_status = "SET" if notification_data.get("webhook_url") else "NOT SET"

    embed = discord.Embed(
        title="Stats",
        description="Current generator stats",
        color=discord.Color.blurple(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="Free Stock", value=f"**{get_stock_count()}**", inline=True)
    embed.add_field(name="Premium Stock", value=f"**{get_premium_stock_count()}**", inline=True)
    embed.add_field(name="Total Generated", value=f"**{total}**", inline=True)
    embed.add_field(name="Free Generated", value=f"**{stats_data['free_generated']}**", inline=True)
    embed.add_field(name="Premium Generated", value=f"**{stats_data['prem_generated']}**", inline=True)
    embed.add_field(
        name="Notifications",
        value=(
            f"Webhook: **{webhook_status}**\n"
            f"Stock zero: **{stock_zero}**\n"
            f"Auto-restock: **{auto_restock_status}**"
        ),
        inline=False
    )
    return embed


class StatsNotificationView(discord.ui.View):
    def __init__(self):
        super().__init__(timeout=None)

    async def toggle_event(self, interaction: discord.Interaction, event_name: str):
        events = notification_data.setdefault("events", {})
        if events.get(event_name, False):
            events[event_name] = False
            state = "off"
        else:
            events[event_name] = True
            state = "on"
        save_notifications()

        await interaction.response.edit_message(
            embed=build_stats_dashboard_embed(),
            view=self
        )
        await interaction.followup.send(
            f"Turned **{event_name.replace('_', ' ')}** webhook notifications {state}.",
            ephemeral=True
        )

    @discord.ui.button(
        label="Toggle stock zero",
        style=discord.ButtonStyle.gray,
        custom_id="stats_toggle_stock_zero"
    )
    async def toggle_stock_zero(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_event(interaction, "stock_zero")

    @discord.ui.button(
        label="Toggle auto-restock",
        style=discord.ButtonStyle.gray,
        custom_id="stats_toggle_auto_restock"
    )
    async def toggle_auto_restock(self, interaction: discord.Interaction, button: discord.ui.Button):
        await self.toggle_event(interaction, "auto_restock")



def is_current_booster(member: discord.Member) -> bool:
    return member.premium_since is not None


def has_role(member: discord.Member, role_id: int) -> bool:
    return any(role.id == role_id for role in member.roles)


async def ensure_booster_premium(member: discord.Member):
    role = member.guild.get_role(BOOSTER_PREMIUM_ROLE_ID)
    if not role:
        return

    uid = str(member.id)
    now = time.time()
    record = boost_subscriptions.get(uid)

    if is_current_booster(member):
        if not record:
            record = {
                "user_id": member.id,
                "started_at": now,
                "expires_at": now + BOOSTER_PREMIUM_SECONDS,
                "kept_while_boosting": False,
            }
            boost_subscriptions[uid] = record
        elif now >= float(record.get("expires_at", 0)):
            record["kept_while_boosting"] = True

        if not has_role(member, BOOSTER_PREMIUM_ROLE_ID):
            try:
                await member.add_roles(role, reason="Temporary premium for server boosting")
            except discord.HTTPException:
                pass
        save_boost_subscriptions()
        return

    if not record:
        return

    expires_at = float(record.get("expires_at", 0))
    if now < expires_at:
        if not has_role(member, BOOSTER_PREMIUM_ROLE_ID):
            try:
                await member.add_roles(role, reason="Temporary premium boost period still active")
            except discord.HTTPException:
                pass
        return

    if has_role(member, BOOSTER_PREMIUM_ROLE_ID):
        try:
            await member.remove_roles(role, reason="Temporary booster premium expired")
        except discord.HTTPException:
            pass
    boost_subscriptions.pop(uid, None)
    save_boost_subscriptions()


async def check_booster_premium_guild(guild: discord.Guild):
    tracked_ids = set(boost_subscriptions.keys())
    member_ids = {str(member.id) for member in guild.members}

    for member in guild.members:
        if is_current_booster(member) or str(member.id) in tracked_ids:
            await ensure_booster_premium(member)

    missing_ids = tracked_ids - member_ids
    if missing_ids:
        for uid in missing_ids:
            boost_subscriptions.pop(uid, None)
        save_boost_subscriptions()


@tasks.loop(minutes=5)
async def booster_premium_maintenance():
    for guild in bot.guilds:
        await check_booster_premium_guild(guild)


@booster_premium_maintenance.before_loop
async def before_booster_premium_maintenance():
    await bot.wait_until_ready()

class GeneratorBot(commands.Bot):
    def __init__(self):
        intents = discord.Intents.default()
        intents.message_content = True
        intents.members = True
        super().__init__(command_prefix=commands.when_mentioned_or("!"), intents=intents)
        self.startup_channels_cleaned = False

    async def setup_hook(self):
        self.add_view(GeneratorView())
        self.add_view(PremiumGeneratorView())
        self.add_view(StatsNotificationView())
        self.tree.on_error = self.on_tree_error
        if not booster_premium_maintenance.is_running():
            booster_premium_maintenance.start()

    async def on_message(self, message: discord.Message):
        # â”€â”€ Guard channel: delete, DM invite, kick â”€â”€
        if (
            message.channel.id == GUARD_CHANNEL_ID
            and not message.author.bot
            and message.guild
        ):
            member = message.guild.get_member(message.author.id)
            try:
                await message.delete()
            except discord.HTTPException:
                pass
            try:
                await message.author.send(
                    f"You were kicked for speaking in the bait channel.\n"
                    f"Rejoin here: {INVITE_LINK}"
                )
            except discord.Forbidden:
                pass
            if member:
                try:
                    await member.kick(reason="Sent a message in a restricted channel.")
                except discord.Forbidden:
                    pass
            return

        await self.process_commands(message)

    async def on_member_update(self, before: discord.Member, after: discord.Member):
        if before.premium_since != after.premium_since:
            await ensure_booster_premium(after)

    async def clear_startup_channels(self):
        channel_ids = {
            FREE_CHANNEL_ID,
            PREMIUM_CHANNEL_ID,
            LOG_CHANNEL_ID,
            STOCK_MONITOR_CHANNEL_ID,
        }
        channel_ids.discard(GUARD_CHANNEL_ID)

        for channel_id in channel_ids:
            channel = self.get_channel(channel_id)
            if not isinstance(channel, discord.TextChannel):
                continue
            try:
                await channel.purge(limit=None)
                print(f"Cleared startup channel: {channel.name} ({channel.id})")
            except Exception as exc:
                print(f"Failed to clear channel {channel_id}: {exc}")

    async def sync_slash_commands(self):
        commands_to_sync = list(self.tree.get_commands())

        self.tree.clear_commands(guild=None)
        await self.tree.sync()
        print("Cleared global slash commands to prevent duplicates.")

        for guild in self.guilds:
            try:
                self.tree.clear_commands(guild=guild)
                for command in commands_to_sync:
                    self.tree.add_command(command, guild=guild)
                await self.tree.sync(guild=guild)
                print(f"Synced slash commands to {guild.name} ({guild.id})")
            except Exception as exc:
                print(f"Failed to sync slash commands to {guild.id}: {exc}")

    async def on_tree_error(self, interaction: discord.Interaction, error: app_commands.AppCommandError):
        print(f"Slash command error: {error!r}")
        message = "That command hit an error. Try again in a moment."
        try:
            if interaction.response.is_done():
                await interaction.followup.send(message, ephemeral=True)
            else:
                await interaction.response.send_message(message, ephemeral=True)
        except discord.HTTPException:
            pass

    async def on_ready(self):
        load_state()
        await self.sync_slash_commands()
        for guild in self.guilds:
            await check_booster_premium_guild(guild)
        print(f"Logged in as {self.user.name} ({self.user.id})")
        print("Bot is ready!")

        if not self.startup_channels_cleaned:
            await self.clear_startup_channels()
            self.startup_channels_cleaned = True

        # â”€â”€ Free channel â”€â”€
        channel = self.get_channel(FREE_CHANNEL_ID)
        if channel and isinstance(channel, discord.TextChannel):
            try:
                await channel.purge(limit=100)
            except Exception:
                pass
            count = get_stock_count()
            embed = discord.Embed(
                title="Free Gen!",
                description=(
                    f"Click the button below to generate a free account.\n\n"
                    f"**Cooldown:** 10 minutes\n**Current Stock:** {count}"
                ),
                color=discord.Color.blue()
            )
            embed.set_footer(text="Free Generator")
            await channel.send(embed=embed, view=GeneratorView())

        # â”€â”€ Premium channel â”€â”€
        prem_channel = self.get_channel(PREMIUM_CHANNEL_ID)
        if prem_channel and isinstance(prem_channel, discord.TextChannel):
            try:
                await prem_channel.purge(limit=100)
            except Exception:
                pass
            count = get_premium_stock_count()
            embed = discord.Embed(
                title="Premium Gen!",
                description=(
                    f"Click the button below to generate a premium account.\n\n"
                    f"**Cooldown:** 5 minutes\n**Current Stock:** {count}"
                ),
                color=discord.Color.gold()
            )
            embed.set_footer(text="Premium Generator")
            await prem_channel.send(embed=embed, view=PremiumGeneratorView())

        # â”€â”€ Stock monitor â”€â”€
        global stock_monitor_msg_id
        stock_monitor_msg_id = load_monitor_id()
        await update_stock_monitor(self)


bot = GeneratorBot()


# â”€â”€ Admin permission check â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

def is_admin():
    async def predicate(interaction: discord.Interaction) -> bool:
        if (
            isinstance(interaction.user, discord.Member)
            and interaction.user.guild_permissions.administrator
        ):
            return True
        await interaction.response.send_message(
            "âŒ You need **Administrator** permission to use this command.",
            ephemeral=True
        )
        return False
    return app_commands.check(predicate)


# â”€â”€ Slash commands â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@bot.tree.command(name="addstock", description="Add accounts to free or premium stock")
@app_commands.describe(
    tier="Which stock to add to: free or premium",
    accounts="Accounts in user:pass format, one per line"
)
@is_admin()
async def cmd_addstock(interaction: discord.Interaction, tier: str, accounts: str):
    tier = tier.lower()
    if tier not in ("free", "premium"):
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return

    filename = FREE_STOCK_FILE if tier == "free" else PREMIUM_STOCK_FILE
    lock     = STOCK_LOCK      if tier == "free" else PREM_STOCK_LOCK

    new_lines = [l.strip() for l in accounts.splitlines() if l.strip().count(":") == 1]
    if not new_lines:
        await interaction.response.send_message(
            "No valid accounts found. Make sure they are in `user:pass` format.", ephemeral=True
        )
        return

    async with lock:
        existing = ""
        if os.path.exists(filename) and os.stat(filename).st_size > 0:
            with open(filename, "r", encoding="utf-8") as f:
                existing = f.read()
        with open(filename, "a", encoding="utf-8") as f:
            if existing and not existing.endswith("\n"):
                f.write("\n")
            f.write("\n".join(new_lines) + "\n")

    total = get_stock_count() if tier == "free" else get_premium_stock_count()
    await interaction.response.send_message(
        f"âœ… Added **{len(new_lines)}** account(s) to **{tier}** stock. Total: **{total}**.",
        ephemeral=True
    )
    await update_stock_monitor(interaction.client)


@bot.tree.command(name="addtxt", description="Upload a .txt file to bulk-add accounts to stock")
@app_commands.describe(
    tier="Which stock to add to: free or premium",
    file="A .txt file with user:pass accounts (one per line)"
)
@is_admin()
async def cmd_addtxt(interaction: discord.Interaction, tier: str, file: discord.Attachment):
    tier = tier.lower()
    if tier not in ("free", "premium"):
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return

    if not file.filename.endswith(".txt"):
        await interaction.response.send_message("Please attach a `.txt` file.", ephemeral=True)
        return

    await interaction.response.defer(ephemeral=True)

    try:
        raw = await file.read()
        text = raw.decode("utf-8", errors="ignore")
    except Exception:
        await interaction.followup.send("Failed to read the file.", ephemeral=True)
        return

    new_lines = [l.strip() for l in text.splitlines() if l.strip().count(":") == 1]
    if not new_lines:
        await interaction.followup.send(
            "No valid accounts found in the file. Make sure lines are in `user:pass` format.",
            ephemeral=True
        )
        return

    filename = FREE_STOCK_FILE if tier == "free" else PREMIUM_STOCK_FILE
    lock     = STOCK_LOCK      if tier == "free" else PREM_STOCK_LOCK

    async with lock:
        existing = ""
        if os.path.exists(filename) and os.stat(filename).st_size > 0:
            with open(filename, "r", encoding="utf-8") as f:
                existing = f.read()
        with open(filename, "a", encoding="utf-8") as f:
            if existing and not existing.endswith("\n"):
                f.write("\n")
            f.write("\n".join(new_lines) + "\n")

    total = get_stock_count() if tier == "free" else get_premium_stock_count()
    await interaction.followup.send(
        f"âœ… Loaded **{len(new_lines)}** account(s) from `{file.filename}` into **{tier}** stock. "
        f"Total: **{total}**.",
        ephemeral=True
    )
    await update_stock_monitor(interaction.client)


@bot.tree.command(name="clearstock", description="Wipe all free or premium stock")
@app_commands.describe(tier="Which stock to clear: free or premium")
@is_admin()
async def cmd_clearstock(interaction: discord.Interaction, tier: str):
    tier = tier.lower()
    if tier not in ("free", "premium"):
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return

    filename = FREE_STOCK_FILE if tier == "free" else PREMIUM_STOCK_FILE
    lock     = STOCK_LOCK      if tier == "free" else PREM_STOCK_LOCK

    async with lock:
        with open(filename, "w", encoding="utf-8") as f:
            f.write("")

    await interaction.response.send_message(
        f"ðŸ—‘ï¸ **{tier.capitalize()}** stock has been cleared.", ephemeral=True
    )
    await update_stock_monitor(interaction.client)


@bot.tree.command(name="clearcooldown", description="Reset a user's cooldown for free or premium")
@app_commands.describe(tier="free or premium", user="The user whose cooldown to clear")
@is_admin()
async def cmd_clearcooldown(interaction: discord.Interaction, tier: str, user: discord.Member):
    tier_key = cooldown_tier(tier)
    if not tier_key:
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return

    uid_str = str(user.id)
    if tier_key == "free":
        free_cooldowns.pop(uid_str, None)
    else:
        prem_cooldowns.pop(uid_str, None)
    cooldown_config["users"][tier_key].pop(uid_str, None)
    cooldown_config["blocks"][tier_key].pop(uid_str, None)
    save_cooldowns()

    await interaction.response.send_message(
        f"Cleared **{cooldown_tier_label(tier_key)}** cooldowns and overrides for {user.mention}.",
        ephemeral=True
    )



@bot.tree.command(name="setcooldown", description="Set generator or user cooldown length")
@app_commands.describe(
    tier="free or premium",
    seconds="Cooldown length after each generation, in seconds",
    user="Optional user override. Leave empty to change the whole generator.",
    expires_in_minutes="Optional. 0 or blank means permanent until changed."
)
@is_admin()
async def cmd_setcooldown(
    interaction: discord.Interaction,
    tier: str,
    seconds: int,
    user: discord.Member | None = None,
    expires_in_minutes: int = 0,
):
    tier_key = cooldown_tier(tier)
    if not tier_key:
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return
    if seconds < 0 or seconds > 604800:
        await interaction.response.send_message("Seconds must be between `0` and `604800`.", ephemeral=True)
        return

    expires_at = cooldown_expiry(expires_in_minutes)
    record = {"seconds": seconds, "expires_at": expires_at, "updated_by": interaction.user.id}
    if user:
        cooldown_config["users"][tier_key][str(user.id)] = record
        target = user.mention
    else:
        cooldown_config["defaults"][tier_key] = record
        target = f"the **{cooldown_tier_label(tier_key)}** generator"
    save_cooldowns()

    await interaction.response.send_message(
        f"Set cooldown for {target} to **{duration_text(seconds)}** ({expiry_text(expires_at)}).",
        ephemeral=True
    )


@bot.tree.command(name="cooldownblock", description="Put a user on a generator cooldown now")
@app_commands.describe(
    tier="free or premium",
    user="User to cooldown",
    minutes="How long to block them. 0 means permanent until cleared.",
    reason="Optional reason"
)
@is_admin()
async def cmd_cooldownblock(
    interaction: discord.Interaction,
    tier: str,
    user: discord.Member,
    minutes: int = 0,
    reason: str = "Manual cooldown block",
):
    tier_key = cooldown_tier(tier)
    if not tier_key:
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return
    if minutes < 0 or minutes > 525600:
        await interaction.response.send_message("Minutes must be between `0` and `525600`.", ephemeral=True)
        return

    expires_at = cooldown_expiry(minutes)
    cooldown_config["blocks"][tier_key][str(user.id)] = {
        "expires_at": expires_at,
        "reason": reason,
        "updated_by": interaction.user.id,
    }
    if expires_at:
        if tier_key == "free":
            free_cooldowns[str(user.id)] = expires_at
        else:
            prem_cooldowns[str(user.id)] = expires_at
    save_cooldowns()

    await interaction.response.send_message(
        f"Put {user.mention} on **{cooldown_tier_label(tier_key)}** cooldown {expiry_text(expires_at)}.",
        ephemeral=True
    )


@bot.tree.command(name="cooldownstatus", description="Show cooldown settings for a generator or user")
@app_commands.describe(tier="free or premium", user="Optional user to inspect")
@is_admin()
async def cmd_cooldownstatus(interaction: discord.Interaction, tier: str, user: discord.Member | None = None):
    tier_key = cooldown_tier(tier)
    if not tier_key:
        await interaction.response.send_message("Tier must be `free` or `premium`.", ephemeral=True)
        return
    if cleanup_cooldown_config():
        save_cooldowns()

    default_record = cooldown_config["defaults"].get(tier_key, {})
    embed = discord.Embed(
        title=f"{cooldown_tier_label(tier_key)} Cooldowns",
        color=discord.Color.blurple(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(
        name="Generator Default",
        value=(
            f"**{duration_text(int(default_record.get('seconds', BASE_COOLDOWN_SECONDS[tier_key])))}**\n"
            f"Scope: {expiry_text(default_record.get('expires_at'))}"
        ),
        inline=False
    )

    if user:
        uid = str(user.id)
        user_record = cooldown_config["users"].get(tier_key, {}).get(uid)
        block_record = cooldown_config["blocks"].get(tier_key, {}).get(uid)
        active_map = free_cooldowns if tier_key == "free" else prem_cooldowns
        active_expires = active_map.get(uid)
        embed.add_field(name="User", value=f"{user.mention}\n`{user.id}`", inline=False)
        embed.add_field(
            name="User Override",
            value=(
                f"**{duration_text(int(user_record.get('seconds')))}**\n{expiry_text(user_record.get('expires_at'))}"
                if user_record else "None"
            ),
            inline=False
        )
        embed.add_field(
            name="Cooldown Block",
            value=expiry_text(block_record.get("expires_at")) if block_record else "None",
            inline=True
        )
        embed.add_field(
            name="Current Active Cooldown",
            value=expiry_text(active_expires) if active_expires and float(active_expires) > time.time() else "None",
            inline=True
        )

    await interaction.response.send_message(embed=embed, ephemeral=True)
@bot.tree.command(name="stockcount", description="Check current stock levels for both tiers")
@is_admin()
async def cmd_stockcount(interaction: discord.Interaction):
    free = get_stock_count()
    prem = get_premium_stock_count()
    embed = discord.Embed(title="ðŸ“¦ Stock Levels", color=discord.Color.blurple())
    embed.add_field(name="Free Stock",    value=f"**{free}** account(s)", inline=True)
    embed.add_field(name="Premium Stock", value=f"**{prem}** account(s)", inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="id", description="Show your Discord user ID")
async def cmd_id(interaction: discord.Interaction):
    await interaction.response.send_message(
        f"Your Discord ID is `{interaction.user.id}`.",
        ephemeral=True
    )




def format_time(ts: float | int | None) -> str:
    if not ts:
        return "Unknown"
    return f"<t:{int(float(ts))}:F> (<t:{int(float(ts))}:R>)"


def build_boostpremium_embed(member: discord.Member) -> discord.Embed:
    uid = str(member.id)
    record = boost_subscriptions.get(uid)
    role = member.guild.get_role(BOOSTER_PREMIUM_ROLE_ID)
    has_premium_role = has_role(member, BOOSTER_PREMIUM_ROLE_ID)
    boosting = is_current_booster(member)

    embed = discord.Embed(
        title="Temporary Premium",
        color=discord.Color.gold() if has_premium_role else discord.Color.dark_gray(),
        timestamp=datetime.utcnow()
    )
    embed.add_field(name="User", value=f"{member.mention}\n`{member.id}`", inline=False)
    embed.add_field(name="Currently Boosting", value="Yes" if boosting else "No", inline=True)
    embed.add_field(name="Premium Role", value="Assigned" if has_premium_role else "Not assigned", inline=True)
    embed.add_field(name="Role", value=role.mention if role else f"Missing `{BOOSTER_PREMIUM_ROLE_ID}`", inline=True)

    if record:
        embed.add_field(name="Started", value=format_time(record.get("started_at")), inline=False)
        embed.add_field(name="Expires", value=format_time(record.get("expires_at")), inline=False)
        embed.add_field(name="Source", value=str(record.get("source", "server_boost")), inline=True)
        embed.add_field(name="Kept While Boosting", value="Yes" if record.get("kept_while_boosting") else "No", inline=True)
        if record.get("role_pending") or record.get("remove_pending"):
            embed.add_field(name="Retry", value="Queued. The bot retries every 5 minutes.", inline=False)
        if record.get("last_role_error"):
            embed.add_field(name="Last Role Error", value=str(record.get("last_role_error"))[:1024], inline=False)
    else:
        embed.description = "No temporary premium record found for this user."

    return embed


@bot.tree.command(name="boostpremium", description="Check temporary premium from boosting")
@app_commands.describe(user="Optional user to check. Admins can check other people.")
async def cmd_boostpremium(interaction: discord.Interaction, user: discord.Member | None = None):
    target = user or interaction.user
    if user and isinstance(interaction.user, discord.Member) and not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Only admins can check another user.", ephemeral=True)
        return

    if not isinstance(target, discord.Member):
        await interaction.response.send_message("Run this inside the server.", ephemeral=True)
        return

    await ensure_booster_premium(target)
    await interaction.response.send_message(embed=build_boostpremium_embed(target), ephemeral=True)


@bot.tree.command(name="grantpremium", description="Grant temporary premium manually")
@app_commands.describe(user="User to grant premium to", days="How many days", reason="Reason stored in the JSON file")
@is_admin()
async def cmd_grantpremium(interaction: discord.Interaction, user: discord.Member, days: int, reason: str = "Manual grant"):
    if days <= 0 or days > 365:
        await interaction.response.send_message("Days must be between 1 and 365.", ephemeral=True)
        return

    now = time.time()
    boost_subscriptions[str(user.id)] = {
        "user_id": user.id,
        "started_at": now,
        "expires_at": now + (days * 24 * 60 * 60),
        "kept_while_boosting": False,
        "source": "manual_grant",
        "reason": reason,
        "granted_by": interaction.user.id,
    }
    save_boost_subscriptions()
    await ensure_booster_premium(user)
    await interaction.response.send_message(embed=build_boostpremium_embed(user), ephemeral=True)


@bot.tree.command(name="premiumexpires", description="Check when temporary premium expires")
@app_commands.describe(user="Optional user to check. Admins can check other people.")
async def cmd_premiumexpires(interaction: discord.Interaction, user: discord.Member | None = None):
    target = user or interaction.user
    if user and isinstance(interaction.user, discord.Member) and not interaction.user.guild_permissions.administrator:
        await interaction.response.send_message("Only admins can check another user.", ephemeral=True)
        return
    if not isinstance(target, discord.Member):
        await interaction.response.send_message("Run this inside the server.", ephemeral=True)
        return

    await ensure_booster_premium(target)
    record = boost_subscriptions.get(str(target.id))
    embed = discord.Embed(title="Premium Expiration", color=discord.Color.gold(), timestamp=datetime.utcnow())
    embed.add_field(name="User", value=f"{target.mention}\n`{target.id}`", inline=False)

    if record:
        embed.add_field(name="Started", value=format_time(record.get("started_at")), inline=False)
        embed.add_field(name="Expires", value=format_time(record.get("expires_at")), inline=False)
        embed.add_field(name="Source", value=str(record.get("source", "server_boost")), inline=True)
        embed.add_field(name="Currently Boosting", value="Yes" if is_current_booster(target) else "No", inline=True)
        if record.get("kept_while_boosting") and is_current_booster(target):
            embed.add_field(name="Status", value="Past the 3 day grant, but kept while still boosting.", inline=False)
    elif has_role(target, PREMIUM_ROLE_ID):
        embed.add_field(name="Status", value="Permanent premium role. No expiration stored.", inline=False)
    else:
        embed.add_field(name="Status", value="No temporary premium record found.", inline=False)

    await interaction.response.send_message(embed=embed, ephemeral=True)


@bot.tree.command(name="genhistory", description="Show recent generation history")
@app_commands.describe(user="Optional user to filter", tier="Optional tier: free or premium", limit="How many entries, max 10")
@is_admin()
async def cmd_genhistory(
    interaction: discord.Interaction,
    user: discord.Member | None = None,
    tier: str = "all",
    limit: int = 5,
):
    limit = max(1, min(limit, 10))
    tier_key = cooldown_tier(tier) if tier.lower() != "all" else None
    if tier.lower() != "all" and not tier_key:
        await interaction.response.send_message("Tier must be `free`, `premium`, or `all`.", ephemeral=True)
        return

    rows = []
    for record in reversed(gen_history):
        if user and int(record.get("user_id", 0)) != user.id:
            continue
        if tier_key:
            record_tier = "prem" if record.get("tier") in ("prem", "premium") else "free"
            if record_tier != tier_key:
                continue
        rows.append(record)
        if len(rows) >= limit:
            break

    embed = discord.Embed(title="Generation History", color=discord.Color.blurple(), timestamp=datetime.utcnow())
    if not rows:
        embed.description = "No matching generation history found."
    else:
        for record in rows:
            generated_at = int(float(record.get("generated_at", time.time())))
            tier_name = "Premium" if record.get("tier") in ("prem", "premium") else "Free"
            value = (
                f"User: <@{record.get('user_id')}> (`{record.get('user_id')}`)\n"
                f"Account: `{record.get('account_username')}:{record.get('account_password')}`\n"
                f"Time: <t:{generated_at}:F> (<t:{generated_at}:R>)"
            )
            embed.add_field(name=tier_name, value=value[:1024], inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)
@bot.tree.command(name="stats", description="View total generation statistics")
@is_admin()
async def cmd_stats(interaction: discord.Interaction):
    total = stats_data["free_generated"] + stats_data["prem_generated"]
    embed = discord.Embed(title="ðŸ“Š Generation Stats", color=discord.Color.blurple())
    embed.add_field(name="Free Generated",    value=f"**{stats_data['free_generated']}**", inline=True)
    embed.add_field(name="Premium Generated", value=f"**{stats_data['prem_generated']}**", inline=True)
    embed.add_field(name="Total Generated",   value=f"**{total}**",                        inline=True)
    await interaction.response.send_message(embed=embed, ephemeral=True)


# â”€â”€ Blacklist commands â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€â”€

@bot.tree.command(name="statspanel", description="Open the stats dashboard and notification toggles")
@is_admin()
async def cmd_statspanel(interaction: discord.Interaction):
    await interaction.response.send_message(
        embed=build_stats_dashboard_embed(),
        view=StatsNotificationView(),
        ephemeral=True
    )

blacklist_group = app_commands.Group(name="blacklist", description="Manage the generator blacklist")
bot.tree.add_command(blacklist_group)


@blacklist_group.command(name="add", description="Blacklist a user from free, premium, or both generators")
@app_commands.describe(user="The user to blacklist", tier="free, premium, or both", reason="Reason shown to admins and the blacklisted user")
@is_admin()
async def bl_add(interaction: discord.Interaction, user: discord.Member, tier: str, reason: str = "No reason provided"):
    tier = tier.lower()
    if tier not in ("free", "premium", "both"):
        await interaction.response.send_message("Tier must be `free`, `premium`, or `both`.", ephemeral=True)
        return

    tiers = ["free", "prem"] if tier == "both" else [tier if tier == "free" else "prem"]
    added_to = []
    updated_to = []
    now = time.time()
    for t in tiers:
        if user.id not in blacklist_data[t]:
            blacklist_data[t].append(user.id)
            added_to.append("free" if t == "free" else "premium")
        else:
            updated_to.append("free" if t == "free" else "premium")
        blacklist_reasons[t][str(user.id)] = {
            "reason": reason,
            "updated_by": interaction.user.id,
            "updated_at": now,
        }
    save_blacklist()

    changed = added_to or updated_to
    if changed:
        parts = []
        if added_to:
            parts.append(f"blacklisted from **{', '.join(added_to)}**")
        if updated_to and not added_to:
            parts.append(f"updated for **{', '.join(updated_to)}**")
        await interaction.response.send_message(
            f"{user.mention} {', '.join(parts)}. Reason: `{reason}`", ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"{user.mention} was already blacklisted from those tier(s). Reason updated.", ephemeral=True
        )


@blacklist_group.command(name="remove", description="Remove a user from the blacklist")
@app_commands.describe(user="The user to unblacklist", tier="free, premium, or both")
@is_admin()
async def bl_remove(interaction: discord.Interaction, user: discord.Member, tier: str):
    tier = tier.lower()
    if tier not in ("free", "premium", "both"):
        await interaction.response.send_message("Tier must be `free`, `premium`, or `both`.", ephemeral=True)
        return

    tiers = ["free", "prem"] if tier == "both" else [tier if tier == "free" else "prem"]
    removed_from = []
    for t in tiers:
        if user.id in blacklist_data[t]:
            blacklist_data[t].remove(user.id)
            removed_from.append("free" if t == "free" else "premium")
    save_blacklist()

    if removed_from:
        await interaction.response.send_message(
            f"âœ… {user.mention} has been removed from the blacklist for: **{', '.join(removed_from)}**.",
            ephemeral=True
        )
    else:
        await interaction.response.send_message(
            f"{user.mention} was not blacklisted from those tier(s).", ephemeral=True
        )


@blacklist_group.command(name="list", description="Show all blacklisted users")
@is_admin()
async def bl_list(interaction: discord.Interaction):
    embed = discord.Embed(title="ðŸš« Blacklisted Users", color=discord.Color.red())

    def fmt(tier_name, ids):
        rows = []
        for uid in ids:
            reason = blacklist_reason(tier_name, int(uid)) or "No reason provided"
            rows.append(f"<@{uid}> (`{uid}`) - {reason}")
        return "\n".join(rows) or "*None*"

    embed.add_field(name="Free",    value=fmt("free", blacklist_data.get("free", []))[:1024], inline=False)
    embed.add_field(name="Premium", value=fmt("prem", blacklist_data.get("prem", []))[:1024], inline=False)
    await interaction.response.send_message(embed=embed, ephemeral=True)


if __name__ == "__main__":
    bot.run(TOKEN)











