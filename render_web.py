import asyncio
import math
import os

from aiohttp import web

from main import TOKEN, bot


async def health(request: web.Request) -> web.Response:
    latency = bot.latency
    latency_ms = None
    if isinstance(latency, (int, float)) and math.isfinite(latency):
        latency_ms = round(latency * 1000)

    return web.json_response({
        "ok": not bot.is_closed(),
        "bot_user": str(bot.user) if bot.user else None,
        "guilds": len(bot.guilds),
        "latency_ms": latency_ms,
    })


async def home(request: web.Request) -> web.Response:
    return web.Response(text="Steam Genn bot is running. Use /health for cron checks.\n")


async def start_discord_bot(app: web.Application) -> None:
    if not TOKEN:
        raise RuntimeError("Set DISCORD_TOKEN in Render environment variables.")
    app["bot_task"] = asyncio.create_task(bot.start(TOKEN))


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

