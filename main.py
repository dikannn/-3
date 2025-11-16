import asyncio
import logging
import os

from aiogram import Bot, Dispatcher, Router, F
from aiogram.enums import ParseMode
from aiogram.client.default import DefaultBotProperties
from aiogram.filters import CommandStart, Command
from aiogram.types import Message, CallbackQuery

from flyerapi import Flyer
import asyncpg
from aiohttp import web

# Load environment variables
try:
    from dotenv import load_dotenv
    load_dotenv()
except:
    pass

BOT_TOKEN = os.getenv("BOT_TOKEN")
FLYER_KEY_PREMIUM = os.getenv("FLYER_KEY_PREMIUM")
FLYER_KEY_REGULAR = os.getenv("FLYER_KEY_REGULAR")
DATABASE_URL = os.getenv("DATABASE_URL")
PORT = int(os.getenv("PORT", "8000"))

if not BOT_TOKEN:
    raise RuntimeError("❌ Missing BOT_TOKEN")
if not FLYER_KEY_PREMIUM:
    raise RuntimeError("❌ Missing FLYER_KEY_PREMIUM")
if not FLYER_KEY_REGULAR:
    raise RuntimeError("❌ Missing FLYER_KEY_REGULAR")
if not DATABASE_URL:
    raise RuntimeError("❌ Missing DATABASE_URL")

logging.basicConfig(level=logging.INFO)
bot = Bot(token=BOT_TOKEN, parse_mode=ParseMode.HTML)
dp = Dispatcher()
router = Router()
dp.include_router(router)

flyer_premium = Flyer(FLYER_KEY_PREMIUM)
flyer_regular = Flyer(FLYER_KEY_REGULAR)

db_pool = None

async def init_db():
    global db_pool
    db_pool = await asyncpg.create_pool(DATABASE_URL)

    async with db_pool.acquire() as conn:
        await conn.execute("""
        CREATE TABLE IF NOT EXISTS users (
            id BIGSERIAL PRIMARY KEY,
            tg_id BIGINT UNIQUE NOT NULL,
            is_premium BOOLEAN NOT NULL DEFAULT FALSE,
            source TEXT,
            flyer_passed BOOLEAN NOT NULL DEFAULT FALSE,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
            last_seen_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

        await conn.execute("""
        CREATE TABLE IF NOT EXISTS traffic_log (
            id BIGSERIAL PRIMARY KEY,
            tg_id BIGINT NOT NULL,
            event TEXT NOT NULL,
            source TEXT,
            is_premium BOOLEAN,
            created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
        );
        """)

    logging.info("📁 Database initialized")

async def upsert_user(user, source=None, flyer_passed=False):
    async with db_pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO users (tg_id, is_premium, source, flyer_passed)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (tg_id) DO UPDATE
        SET is_premium = EXCLUDED.is_premium,
            last_seen_at = NOW(),
            flyer_passed = users.flyer_passed OR EXCLUDED.flyer_passed,
            source = COALESCE(users.source, EXCLUDED.source);
        """, user.id, bool(user.is_premium), source, flyer_passed)

async def log_event(user, event, source=None):
    async with db_pool.acquire() as conn:
        await conn.execute("""
        INSERT INTO traffic_log (tg_id, event, source, is_premium)
        VALUES ($1, $2, $3, $4)
        """, user.id, event, source, bool(user.is_premium))

async def check_flyer(user):
    flyer_client = flyer_premium if user.is_premium else flyer_regular
    return await flyer_client.check(user_id=user.id, language_code=user.language_code or "ru")

async def ensure_access(message: Message, source=None):
    user = message.from_user
    await log_event(user, "check_access", source)
    ok = await check_flyer(user)
    await upsert_user(user, source=source, flyer_passed=ok)
    return ok

async def ensure_access_cb(call: CallbackQuery):
    user = call.from_user
    ok = await check_flyer(user)
    await upsert_user(user, flyer_passed=ok)
    return ok

@router.message(CommandStart())
async def start_cmd(message: Message):
    source = message.text.split(" ")[1] if len(message.text.split()) > 1 else None
    await log_event(message.from_user, "start", source)

    if not await ensure_access(message, source):
        return

    await message.answer(
        f"👋 Добро пожаловать!\n\n"
        f"Premium: {'💎 Да' if message.from_user.is_premium else '❌ Нет'}\n"
        f"Проверки пройдены.\n"
        f"/menu - открыть меню"
    )

@router.message(Command("menu"))
async def menu_cmd(message: Message):
    await log_event(message.from_user, "menu")
    if not await ensure_access(message):
        return
    await message.answer("📍 Пример меню — функционал будет тут.")

async def flyer_webhook(request):
    payload = await request.json()
    event_type = payload.get("type")
    data = payload.get("data", {})

    if event_type == "sub_completed":
        user_id = data.get("user_id")
        async with db_pool.acquire() as conn:
            await conn.execute("UPDATE users SET flyer_passed = TRUE WHERE tg_id = $1", user_id)

    logging.info(f"Flyer Webhook received: {payload}")
    return web.json_response({"status": True})

async def start_web_server():
    app = web.Application()
    app.router.add_post("/flyer-webhook", flyer_webhook)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", PORT)
    await site.start()
    logging.info(f"🌐 Webhook server running on port {PORT}")
    await asyncio.Event().wait()

async def main():
    await init_db()
    await asyncio.gather(dp.start_polling(bot), start_web_server())

if __name__ == "__main__":
    asyncio.run(main())
