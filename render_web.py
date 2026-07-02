import asyncio
import math
import os
import time

from aiohttp import web

from main import TOKEN, bot

BOT_STARTED_AT = time.time()
BOT_STATE = {
    "bot_status": "starting",
    "bot_error": None,
    "next_retry_at": None,
}


async def health(request: web.Request) -> web.Response:
    latency = bot.latency
    latency_ms = None
    if isinstance(latency, (int, float)) and math.isfinite(latency):
        latency_ms = round(latency * 1000)

    task = request.app.get("bot_task")
    task_done = bool(task and task.done())
    task_error = BOT_STATE.get("bot_error")

    return web.json_response({
        "ok": bot.is_ready() and not task_error,
        "bot_ready": bot.is_ready(),
        "bot_closed": bot.is_closed(),
        "bot_task_done": task_done,
        "bot_status": BOT_STATE.get("bot_status"),
        "bot_error": task_error,
        "next_retry_at": BOT_STATE.get("next_retry_at"),
        "bot_user": str(bot.user) if bot.user else None,
        "guilds": len(bot.guilds),
        "latency_ms": latency_ms,
        "uptime_seconds": round(time.time() - BOT_STARTED_AT),
    })


async def home(request: web.Request) -> web.Response:
    return web.Response(text="Steam Genn bot is running. Use /health for cron checks.\n")


def summarize_bot_error(exc: Exception) -> str:
    raw = repr(exc)
    if "1015" in raw or "Too Many Requests" in raw or "rate limited" in raw.lower():
        return "Discord/Cloudflare is temporarily rate-limiting this Render IP. The bot will retry automatically."
    return raw[:1000]


async def run_discord_bot_forever(app: web.Application) -> None:
    delay = 60
    while True:
        try:
            BOT_STATE["bot_status"] = "connecting"
            BOT_STATE["bot_error"] = None
            BOT_STATE["next_retry_at"] = None
            print("Starting Discord bot client...", flush=True)
            await bot.start(TOKEN, reconnect=True)
            BOT_STATE["bot_status"] = "stopped"
            BOT_STATE["bot_error"] = "Discord bot stopped without an exception."
        except asyncio.CancelledError:
            BOT_STATE["bot_status"] = "stopping"
            raise
        except Exception as exc:
            raw_error = repr(exc)
            is_rate_limited = (
                "1015" in raw_error
                or "Too Many Requests" in raw_error
                or "rate limited" in raw_error.lower()
            )
            retry_delay = max(delay, 1800) if is_rate_limited else delay
            BOT_STATE["bot_status"] = "retry_wait"
            BOT_STATE["bot_error"] = summarize_bot_error(exc)
            BOT_STATE["next_retry_at"] = round(time.time() + retry_delay)
            print(f"Discord bot login/connect failed: {BOT_STATE['bot_error']}. Retrying in {retry_delay}s.", flush=True)
            await asyncio.sleep(retry_delay)
            delay = min(retry_delay * 2, 3600)


async def start_discord_bot(app: web.Application) -> None:
    if not TOKEN:
        raise RuntimeError("Set DISCORD_TOKEN in Render environment variables.")

    app["bot_error"] = None
    app["bot_status"] = "starting"
    app["next_retry_at"] = None
    app["bot_task"] = asyncio.create_task(run_discord_bot_forever(app))

async def stop_discord_bot(app: web.Application) -> None:
    task = app.get("bot_task")
    if task:
        task.cancel()
    await bot.close()


def create_app() -> web.Application:
    app = web.Application()
    app.router.add_get("/", home)
    app.router.add_get("/health", health)
    app.on_startup.append(start_discord_bot)
    app.on_cleanup.append(stop_discord_bot)
    return app


if __name__ == "__main__":
    port = int(os.getenv("PORT", "10000"))
    web.run_app(create_app(), host="0.0.0.0", port=port)






