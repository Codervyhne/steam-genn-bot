import asyncio
import math
import os
import time

from aiohttp import web

from main import TOKEN, bot

BOT_STARTED_AT = time.time()


async def health(request: web.Request) -> web.Response:
    latency = bot.latency
    latency_ms = None
    if isinstance(latency, (int, float)) and math.isfinite(latency):
        latency_ms = round(latency * 1000)

    task = request.app.get("bot_task")
    task_done = bool(task and task.done())
    task_error = request.app.get("bot_error")

    return web.json_response({
        "ok": bot.is_ready() and not task_error,
        "bot_ready": bot.is_ready(),
        "bot_closed": bot.is_closed(),
        "bot_task_done": task_done,
        "bot_error": task_error,
        "bot_user": str(bot.user) if bot.user else None,
        "guilds": len(bot.guilds),
        "latency_ms": latency_ms,
        "uptime_seconds": round(time.time() - BOT_STARTED_AT),
    })


async def home(request: web.Request) -> web.Response:
    return web.Response(text="Steam Genn bot is running. Use /health for cron checks.\n")


async def start_discord_bot(app: web.Application) -> None:
    if not TOKEN:
        raise RuntimeError("Set DISCORD_TOKEN in Render environment variables.")

    app["bot_error"] = None
    task = asyncio.create_task(bot.start(TOKEN))
    app["bot_task"] = task

    def report_bot_crash(done_task: asyncio.Task) -> None:
        try:
            done_task.result()
        except asyncio.CancelledError:
            return
        except Exception as exc:
            app["bot_error"] = repr(exc)
            print(f"Discord bot task stopped: {exc!r}", flush=True)

    task.add_done_callback(report_bot_crash)


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


