import asyncio
import json
import logging
import os
import threading
import uuid
import calendar
import database

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.fsm.storage.redis import RedisStorage
from redis.asyncio import Redis
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback
from aiogram_calendar.schemas import SimpleCalAct
import datetime
import vk_api
from vk_api.bot_longpoll import VkBotLongPoll, VkBotEventType
from vk_api.keyboard import VkKeyboard, VkKeyboardColor

# Настройка логирования
logging.basicConfig(level=logging.INFO)

API_TOKEN = (
    os.environ.get("TELEGRAM_BOT_TOKEN")
    or os.environ.get("BOT_TOKEN")
    or os.environ.get("TELEGRAM_TOKEN")
)
VK_BOT_TOKEN_ENV = os.environ.get("VK_BOT_TOKEN")
VK_TOKEN_ENV = os.environ.get("VK_TOKEN")
VK_TOKEN = VK_BOT_TOKEN_ENV or VK_TOKEN_ENV
VK_GROUP_ID = os.environ.get("VK_GROUP_ID")
ADMIN_ID = 2127578673
VK_ADMIN_ID = int(os.environ.get("VK_ADMIN_ID") or ADMIN_ID)
REDIS_URL = os.environ.get("REDIS_URL")
PLATFORM_TELEGRAM = "telegram"
PLATFORM_VK = "vk"
VK_MAX_BUTTONS_ON_LINE = 5
VK_MAX_LINES = 10
VK_REMINDER_USERS_PAGE_SIZE = 8

if not API_TOKEN:
    raise ValueError("❌ Не задан токен бота. Укажи TELEGRAM_BOT_TOKEN (или BOT_TOKEN / TELEGRAM_TOKEN)")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
redis = None
vk_session = None
vk_api_client = None
vk_longpoll = None
vk_states = {}


def mask_secret(secret: str) -> str:
    if not secret:
        return "missing"
    if len(secret) <= 8:
        return "*" * len(secret)
    return f"{secret[:4]}...{secret[-4:]}"


if REDIS_URL:
    try:
        redis = Redis.from_url(REDIS_URL, socket_connect_timeout=1, socket_timeout=1)
        storage = RedisStorage(redis=redis)
        logging.info("Используется RedisStorage")
    except Exception as e:
        redis = None
        storage = MemoryStorage()
        logging.warning(f"Не удалось настроить RedisStorage, переключаемся на MemoryStorage: {e}")
else:
    storage = MemoryStorage()
    logging.warning("REDIS_URL не задан. Используется MemoryStorage (FSM-состояние не сохраняется между перезапусками)")

dp = Dispatcher(storage=storage)

# Инициализация БД
database.init_db()

def execute_query(query, params=(), fetch=False, fetchone=False):
    conn = database.get_connection()
    cursor = conn.cursor()
    try:
        cursor.execute(query, params)
        if fetch:
            return cursor.fetchall()
        if fetchone:
            return cursor.fetchone()
        conn.commit()
    except Exception as e:
        logging.error(f"Database error: {e}")
        conn.rollback()
        raise e
    finally:
        cursor.close()
        conn.close()


def make_internal_user_id(platform: str, platform_user_id: int) -> int:
    numeric_user_id = int(platform_user_id)
    if platform == PLATFORM_VK:
        return -abs(numeric_user_id)
    return abs(numeric_user_id)


def detect_platform_by_user_id(user_id: int) -> str:
    return PLATFORM_VK if int(user_id) < 0 else PLATFORM_TELEGRAM


def get_platform_user_id(user_id: int) -> int:
    return abs(int(user_id))


def telegram_internal_user_id(user: types.User) -> int:
    return make_internal_user_id(PLATFORM_TELEGRAM, user.id)


def get_user_record(user_id: int):
    return execute_query(
        """
        SELECT user_id, platform, platform_user_id, first_name, last_name, mafia_nick,
               telegram_username, vk_username
        FROM users
        WHERE user_id = %s
        """,
        (user_id,),
        fetchone=True
    )


def get_display_username(user_row) -> str:
    if not user_row:
        return "username не указан"

    telegram_username = user_row[6] if len(user_row) > 6 else None
    vk_username = user_row[7] if len(user_row) > 7 else None
    platform = user_row[1] if len(user_row) > 1 else detect_platform_by_user_id(user_row[0])
    platform_user_id = user_row[2] if len(user_row) > 2 else get_platform_user_id(user_row[0])

    if telegram_username:
        return f"@{telegram_username}"
    if vk_username:
        return f"vk.com/{vk_username}"
    if platform == PLATFORM_VK:
        return f"vk id{platform_user_id}"
    return "username не указан"


def build_profile_link(platform: str, platform_user_id: int, telegram_username: str = None, vk_username: str = None) -> str:
    if telegram_username:
        return f"https://t.me/{telegram_username}"
    if platform == PLATFORM_TELEGRAM:
        return f"tg://user?id={platform_user_id}"
    if vk_username:
        return f"https://vk.com/{vk_username}"
    return f"https://vk.com/id{platform_user_id}"


def upsert_user(
    platform: str,
    platform_user_id: int,
    first_name: str,
    last_name: str,
    mafia_nick: str,
    age: int,
    telegram_username: str = None,
    vk_username: str = None,
):
    internal_user_id = make_internal_user_id(platform, platform_user_id)
    execute_query(
        """
        INSERT INTO users (
            user_id, platform, platform_user_id, first_name, last_name,
            mafia_nick, age, telegram_username, vk_username
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
            platform = EXCLUDED.platform,
            platform_user_id = EXCLUDED.platform_user_id,
            first_name = EXCLUDED.first_name,
            last_name = EXCLUDED.last_name,
            mafia_nick = EXCLUDED.mafia_nick,
            age = EXCLUDED.age,
            telegram_username = EXCLUDED.telegram_username,
            vk_username = EXCLUDED.vk_username
        """,
        (
            internal_user_id,
            platform,
            int(platform_user_id),
            first_name,
            last_name,
            mafia_nick,
            age,
            telegram_username,
            vk_username,
        ),
    )
    return internal_user_id


async def send_text_to_user(user_id: int, text: str, parse_mode: str = None, reply_markup=None):
    platform = detect_platform_by_user_id(user_id)
    platform_user_id = get_platform_user_id(user_id)

    if platform == PLATFORM_TELEGRAM:
        await bot.send_message(
            platform_user_id,
            text,
            parse_mode=parse_mode,
            reply_markup=reply_markup
        )
        return

    if not vk_api_client:
        logging.warning("VK API client не инициализирован, сообщение не отправлено пользователю %s", user_id)
        return

    vk_api_client.messages.send(
        user_id=platform_user_id,
        random_id=uuid.uuid4().int & 0x7FFFFFFF,
        message=text,
        keyboard=reply_markup
    )


async def notify_admin(text: str):
    await bot.send_message(ADMIN_ID, text)


def build_game_title(game_name: str, game_date: str) -> str:
    return f"{game_date} {game_name}"


def fetch_active_games(include_deleted: bool = False):
    if include_deleted:
        return sort_games_by_date(
            execute_query("SELECT game_id, game_name, game_date, is_deleted FROM games", fetch=True)
        )
    return sort_games_by_date(
        execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
    )


def fetch_upcoming_games():
    return sort_games_by_date(filter_upcoming_games(fetch_active_games()))


def format_user_participants(game_id: int, title: str) -> str:
    participants = execute_query(
        """
        SELECT u.user_id, u.mafia_nick
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s AND r.status = %s
        """,
        (game_id, 'registered'),
        fetch=True
    )
    thinking_users = set()
    late_users = set()
    try:
        thinking_users = set(asyncio.run(get_thinking(game_id)))
    except RuntimeError:
        pass
    try:
        late_users = set(asyncio.run(get_late_players(game_id)))
    except RuntimeError:
        pass

    if not participants and not thinking_users:
        return f"На игру {title} пока никто не записался."

    response = f"Список участников на игру {title}:\n"
    participant_ids = {uid for uid, _ in participants}
    regular_participants = [p for p in participants if p[0] not in late_users]
    late_participants = [p for p in participants if p[0] in late_users]

    idx = 1
    for uid, nick in regular_participants + late_participants:
        mark = " (думает)" if uid in thinking_users else ""
        late_mark = " (опоздает)" if uid in late_users else ""
        response += f"{idx}. {nick}{mark}{late_mark}\n"
        idx += 1

    for uid in thinking_users:
        if uid not in participant_ids:
            ud = execute_query("SELECT mafia_nick FROM users WHERE user_id=%s", (uid,), fetchone=True)
            if ud:
                response += f"- {ud[0]} (думает)\n"
    return response.strip()


async def format_user_participants_async(game_id: int, title: str) -> str:
    participants = execute_query(
        """
        SELECT u.user_id, u.mafia_nick
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s AND r.status = %s
        """,
        (game_id, 'registered'),
        fetch=True
    )
    thinking_users = set(await get_thinking(game_id))
    late_users = set(await get_late_players(game_id))

    if not participants and not thinking_users:
        return f"На игру {title} пока никто не записался."

    response = f"Список участников на игру {title}:\n"
    participant_ids = {uid for uid, _ in participants}
    regular_participants = [p for p in participants if p[0] not in late_users]
    late_participants = [p for p in participants if p[0] in late_users]

    idx = 1
    for uid, nick in regular_participants + late_participants:
        mark = " (думает)" if uid in thinking_users else ""
        late_mark = " (опоздает)" if uid in late_users else ""
        response += f"{idx}. {nick}{mark}{late_mark}\n"
        idx += 1

    for uid in thinking_users:
        if uid not in participant_ids:
            ud = execute_query("SELECT mafia_nick FROM users WHERE user_id=%s", (uid,), fetchone=True)
            if ud:
                response += f"- {ud[0]} (думает)\n"
    return response.strip()


async def format_admin_participants_async(game_id: int, title: str) -> str:
    participants = execute_query(
        """
        SELECT u.user_id, u.first_name, u.last_name, u.mafia_nick, u.telegram_username, u.vk_username, u.platform, u.platform_user_id
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s AND r.status = %s
        """,
        (game_id, 'registered'),
        fetch=True
    )
    thinking_users = set(await get_thinking(game_id))
    late_users = set(await get_late_players(game_id))

    if not participants and not thinking_users:
        return f"На игру {title} пока никто не записался."

    response = f"Список участников на игру {title}:\n"
    ordered_participants = [p for p in participants if p[0] not in late_users] + [p for p in participants if p[0] in late_users]

    for idx, (user_id, first_name, last_name, nick, tg_username, vk_username, platform, platform_user_id) in enumerate(ordered_participants, start=1):
        profile_link = build_profile_link(platform, platform_user_id, tg_username, vk_username)
        mark = " (думает)" if user_id in thinking_users else ""
        late_mark = " (опоздает)" if user_id in late_users else ""
        response += f"{idx}. {first_name} {last_name} ({nick}, {profile_link}){mark}{late_mark}\n"

    participant_ids = {user_id for user_id, *_ in participants}
    for uid in thinking_users:
        if uid not in participant_ids:
            ud = execute_query(
                "SELECT first_name, last_name, mafia_nick, telegram_username, vk_username, platform, platform_user_id FROM users WHERE user_id=%s",
                (uid,),
                fetchone=True
            )
            if ud:
                profile_link = build_profile_link(ud[5], ud[6], ud[3], ud[4])
                response += f"- {ud[0]} {ud[1]} ({ud[2]}, {profile_link}) (думает)\n"
    return response.strip()

# Состояния FSM
class Form(StatesGroup):
    start = State()
    get_name = State()
    get_lastname = State()
    get_nick = State()
    get_age = State()
    edit_profile_nick = State()
    edit_profile_name = State()
    edit_profile_lastname = State()
    edit_profile_age = State()
    menu = State()
    user_view_participants = State()
    game_registration = State()
    game_cancellation = State()
    admin_menu = State()
    add_game_date = State()
    add_game_type = State()
    delete_game = State()
    view_participants = State()
    admin_cancel_game = State()
    edit_schedule = State()
    confirm_profile_update = State()
    admin_reminder = State()
    admin_reminder_audience = State()
    admin_reminder_custom_users = State()
    admin_broadcast = State()
    admin_broadcast_game = State()
    admin_broadcast_audience = State()
    admin_broadcast_custom_users = State()
    admin_broadcast_message = State()
    restore_game = State()

# Главное меню
def main_menu_keyboard(user_id):
    builder = ReplyKeyboardBuilder()
    builder.button(text="📝Записаться на игру")
    builder.button(text="❌Отменить запись")
    builder.button(text="📝 Обновить профиль")
    builder.button(text="📅Расписание игр")
    builder.button(text="👥Список участников")
    builder.button(text="📍Как до нас добраться?")
    if user_id == ADMIN_ID:
        builder.button(text="⚙️ Админ-панель")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def admin_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="➕ Добавить игру")
    builder.button(text="❌ Удалить игру")
    builder.button(text="♻️ Восстановить игру")
    builder.button(text="🚫 Отмена игры")
    builder.button(text="🔔 Напомнить об игре")
    builder.button(text="📢 Рассылка")
    builder.button(text="👥 Список участников")
    builder.button(text="🏠 Главное меню")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)


def vk_main_menu_keyboard(user_id: int = None):
    keyboard = VkKeyboard(one_time=False)
    keyboard.add_button("📝Записаться на игру", color=VkKeyboardColor.PRIMARY, payload={"command": "register"})
    keyboard.add_button("❌Отменить запись", color=VkKeyboardColor.SECONDARY, payload={"command": "cancel_registration"})
    keyboard.add_line()
    keyboard.add_button("📝 Обновить профиль", color=VkKeyboardColor.SECONDARY, payload={"command": "edit_profile"})
    keyboard.add_line()
    keyboard.add_button("📅Расписание игр", color=VkKeyboardColor.SECONDARY, payload={"command": "schedule"})
    keyboard.add_button("👥Список участников", color=VkKeyboardColor.SECONDARY, payload={"command": "participants"})
    keyboard.add_line()
    keyboard.add_button("📍Как до нас добраться?", color=VkKeyboardColor.SECONDARY, payload={"command": "location"})
    if user_id == make_internal_user_id(PLATFORM_VK, VK_ADMIN_ID):
        keyboard.add_line()
        keyboard.add_button("⚙️ Админ-панель", color=VkKeyboardColor.POSITIVE, payload={"command": "admin_panel"})
    return keyboard.get_keyboard()


def vk_admin_menu_keyboard():
    keyboard = VkKeyboard(one_time=False)
    keyboard.add_button("➕ Добавить игру", color=VkKeyboardColor.POSITIVE, payload={"command": "admin_add_game"})
    keyboard.add_button("❌ Удалить игру", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_delete_game"})
    keyboard.add_line()
    keyboard.add_button("♻️ Восстановить игру", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_restore_game"})
    keyboard.add_button("🚫 Отмена игры", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_cancel_game"})
    keyboard.add_line()
    keyboard.add_button("👥 Список участников", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_view_participants"})
    keyboard.add_button("🔔 Напомнить об игре", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_reminder"})
    keyboard.add_line()
    keyboard.add_button("📢 Рассылка", color=VkKeyboardColor.SECONDARY, payload={"command": "admin_broadcast"})
    keyboard.add_line()
    keyboard.add_button("🏠 Главное меню", color=VkKeyboardColor.PRIMARY, payload={"command": "main_menu"})
    return keyboard.get_keyboard()

# Helper для "думающих" (теперь в БД)
async def mark_thinking(user_id: int, game_id: int):
    execute_query("INSERT INTO thinking_players (user_id, game_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, game_id))

async def get_thinking(game_id: int):
    rows = execute_query("SELECT user_id FROM thinking_players WHERE game_id = %s", (game_id,), fetch=True)
    return [r[0] for r in rows]

async def mark_late(user_id: int, game_id: int):
    try:
        execute_query(
            """
            UPDATE registrations
            SET is_late = TRUE
            WHERE user_id = %s AND game_id = %s AND status = 'registered'
            """,
            (user_id, game_id)
        )
    except Exception as e:
        logging.warning(f"Не удалось обновить registrations.is_late: {e}")

    # legacy-совместимость со старыми данными
    try:
        execute_query(
            "INSERT INTO late_players (user_id, game_id) VALUES (%s, %s) ON CONFLICT DO NOTHING",
            (user_id, game_id)
        )
    except Exception as e:
        logging.warning(f"Не удалось записать late_players: {e}")

async def unmark_late(user_id: int, game_id: int):
    try:
        execute_query(
            "UPDATE registrations SET is_late = FALSE WHERE user_id = %s AND game_id = %s",
            (user_id, game_id)
        )
    except Exception as e:
        logging.warning(f"Не удалось сбросить registrations.is_late: {e}")

    try:
        execute_query(
            "DELETE FROM late_players WHERE user_id = %s AND game_id = %s",
            (user_id, game_id)
        )
    except Exception as e:
        logging.warning(f"Не удалось удалить late_players: {e}")

async def get_late_players(game_id: int):
    late_ids = set()

    try:
        rows = execute_query(
            "SELECT user_id FROM registrations WHERE game_id = %s AND status = 'registered' AND is_late = TRUE",
            (game_id,),
            fetch=True
        )
        late_ids.update(int(uid) for (uid,) in rows)
    except Exception as e:
        logging.warning(f"Не удалось прочитать registrations.is_late: {e}")

    # fallback + совместимость с уже сохраненными отметками
    try:
        rows = execute_query("SELECT user_id FROM late_players WHERE game_id = %s", (game_id,), fetch=True)
        late_ids.update(int(uid) for (uid,) in rows)
    except Exception as e:
        logging.warning(f"Не удалось прочитать late_players: {e}")

    return late_ids

def late_button_keyboard(game_id: int):
    builder = InlineKeyboardBuilder()
    builder.button(text="⏰ Опоздаю", callback_data=f"late_{game_id}")
    return builder.as_markup()

def parse_game_date(game_date: str):
    if not game_date:
        return None

    normalized = game_date.strip()
    parts = normalized.split()
    if len(parts) >= 2 and parts[0] in ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']:
        normalized = parts[1]

    for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M", "%Y-%m-%d %H:%M:%S", "%d.%m"):
        try:
            parsed = datetime.datetime.strptime(normalized, fmt).date()
            if fmt == "%d.%m":
                return parsed.replace(year=datetime.date.today().year)
            return parsed
        except ValueError:
            continue
    return None

def is_upcoming_game(game_date: str) -> bool:
    parsed = parse_game_date(game_date)
    if not parsed:
        return True
    return parsed >= datetime.date.today()

def filter_upcoming_games(games):
    return [game for game in games if is_upcoming_game(game[2])]


def sort_games_by_date(games, date_index: int = 2):
    def sort_key(game):
        parsed = parse_game_date(game[date_index])
        if parsed is None:
            return (1, datetime.date.max, str(game[date_index]))
        return (0, parsed, str(game[date_index]))

    return sorted(games, key=sort_key)

def get_game_limit(game_name: str) -> int:
    if "Рейтинговая игра" in game_name:
        return 12
    return 15

async def is_game_full(game_id: int, game_name: str, user_id: int) -> bool:
    existing = execute_query(
        "SELECT status FROM registrations WHERE user_id=%s AND game_id=%s",
        (user_id, game_id),
        fetchone=True
    )
    if existing and existing[0] == "registered":
        return False

    registered_users = execute_query(
        "SELECT user_id FROM registrations WHERE game_id=%s AND status='registered'",
        (game_id,),
        fetch=True
    )
    registered_ids = {uid for (uid,) in registered_users}
    late_users = await get_late_players(game_id)
    late_registered_count = len(registered_ids & late_users)
    limit = get_game_limit(game_name) + late_registered_count
    return len(registered_ids) >= limit

def get_game_rules(game_name):
    sport_rules = "18:00 – сбор и объяснение правил\n18:30 – начало игр\n\n"
    city_rules = "18:00 – сбор и объяснение правил\n18:30 – начало игр\n\n"
    rating_rules = "19:00 – начало игр\n\n"

    if "Спортивная мафия" in game_name:
        return sport_rules
    elif "Рейтинговая игра" in game_name:
        return rating_rules
    elif "Городская мафия" in game_name:
        return city_rules
    return "\n"

def get_game_cost(game_name):
    sport_rules = "💵Стоимость игр 600 руб. с человека💵\n\n"
    city_rules = "💵Стоимость игр 600 руб. с человека💵\n\n"
    rating_rules = "💵Стоимость игр 800 руб. с человека💵\n\n"

    if "Спортивная мафия" in game_name:
        return sport_rules
    elif "Рейтинговая игра" in game_name:
        return rating_rules
    elif "Городская мафия" in game_name:
        return city_rules
    return "\n"


def build_registration_success_text(game_date: str, game_name: str) -> str:
    return (
        f"Ты успешно записался на игру {game_date} {game_name}!\n\n"
        f"{get_game_rules(game_name)}"
        f"{get_game_cost(game_name)}"
        "Оплачиваете после игры\n\n"
        "🎁 Если ты первый раз в Тайной Комнате - тебе скидка 200 руб.\n"
        "🎁 Если вы пришли вдвоем - 1000 руб. за двоих (одним платежом)\n"
        "❗️Скидки и акции не суммируются\n\n"
        "P.S. На улице снег, поэтому возьмите, пожалуйста, с собой сменку или пользуйтесь тапочками ТК🙏\n\n"
        "❗️Игра не состоится, если придут меньше 10 человек❗️\n\n"
        "Предупреди, если опоздаешь"
    )


def get_age_restriction_notice() -> str:
    return (
        "В Тайной комнате действуют возрастные ограничения для игры в мафию:\n"
        "• 18+ для Спортивной мафии\n"
        "• 16+ для Городской мафии"
    )


def get_min_age_for_game(game_name: str):
    if "Спортивная мафия" in game_name:
        return 18
    if "Городская мафия" in game_name:
        return 16
    return None


def get_registration_age_rejection(game_name: str, user_age: int):
    min_age = get_min_age_for_game(game_name)
    if min_age is None:
        return None
    if user_age is None:
        return "Не удалось определить твой возраст. Пожалуйста, обнови профиль и укажи корректный возраст."
    if user_age < min_age:
        return f"К сожалению, на игру {game_name} можно записаться только с {min_age} лет."
    return None

async def wake_up_all_users():
    users = execute_query("SELECT user_id FROM users", fetch=True)
    if not users:
        logging.info("Нет пользователей для wake-up уведомления")
        return

    sent = 0
    for (user_id,) in users:
        try:
            await send_text_to_user(
                user_id,
                "✅ Бот снова на связи после обновления. Можно пользоваться как обычно."
            )
            sent += 1
            await asyncio.sleep(0.03)
        except Exception as e:
            logging.warning(f"Wake-up: не удалось отправить сообщение пользователю {user_id}: {e}")

    logging.info(f"Wake-up завершен: уведомлено {sent}/{len(users)} пользователей")

# ===================== /start и профиль =====================
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    internal_user_id = telegram_internal_user_id(message.from_user)
    user = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id = %s", (internal_user_id,), fetchone=True)

    if user:
        builder = ReplyKeyboardBuilder()
        builder.button(text="✅ Оставить как есть")
        builder.button(text="📝 Обновить профиль")
        builder.adjust(1)

        await message.answer(
            f"С возвращением, {user[2]}!\n"
            "Вижу, что мы с тобой уже знакомились☺️ Хочешь изменить свое имя, фамилию или ник?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(Form.confirm_profile_update)
        return

    builder = ReplyKeyboardBuilder()
    builder.button(text="Да")
    builder.button(text="Нет")
    await message.answer(
        "Привет!👋\n"
        "Я бот, который поможет тебе записываться на мафию в клубе настольных игр Тайная комната.\n\n"
        "Если возникнут вопросы - пиши Нате @natabordo\n\n"
        "Готов познакомиться?",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.start)

@dp.message(Form.confirm_profile_update)
async def process_confirm_profile_update(message: types.Message, state: FSMContext):
    if message.text == "📝 Обновить профиль":
        await message.answer("Хорошо! Давай обновим твою анкету. Какой твой игровой ник в мафии?")
        await state.set_state(Form.get_nick)
    elif message.text == "✅ Оставить как есть":
        await message.answer("Отлично! Переходим в главное меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
    else:
        await message.answer("Пожалуйста, воспользуйся кнопками для выбора.")

@dp.message(Form.start)
async def process_start(message: types.Message, state: FSMContext):
    if message.text and message.text.lower() == "да":
        await message.answer(
        "Какой твой игровой ник в мафии?\n\n"
        "P.S. В мафии используют ники для того, чтобы разделять игру и реальную жизнь, и не переносить негативные эмоции на личности игроков"
    )
        await state.set_state(Form.get_nick)
    elif message.text and message.text.lower() == "нет":
        await message.answer("Хорошо, запускай бота снова, когда будешь готов.")
    else:
        await message.answer("Пожалуйста, воспользуйся кнопками для выбора.")

@dp.message(Form.get_nick)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(mafia_nick=message.text)
    await message.answer(
        "Спасибо! Также напиши, пожалуйста, свое имя.\n\n"
        "P.S. Его увидит только админ😉"
    )
    await state.set_state(Form.get_name)

@dp.message(Form.get_name)
async def process_lastname(message: types.Message, state: FSMContext):
    await state.update_data(first_name=message.text)
    await message.answer(
        "Отлично! А теперь, напиши, пожалуйста, свою фамилию🙏"
    )
    await state.set_state(Form.get_lastname)

@dp.message(Form.get_lastname)
async def process_nick(message: types.Message, state: FSMContext):
    await state.update_data(last_name=message.text)
    await message.answer("И последнее: сколько тебе лет?")
    await state.set_state(Form.get_age)

@dp.message(Form.get_age)
async def process_age(message: types.Message, state: FSMContext):
    try:
        age = int(message.text)
    except (ValueError, TypeError):
        await message.answer("Пожалуйста, введи корректный возраст цифрами.")
        return
    await state.update_data(age=age)
    data = await state.get_data()
    upsert_user(
        platform=PLATFORM_TELEGRAM,
        platform_user_id=message.from_user.id,
        first_name=data['first_name'],
        last_name=data['last_name'],
        mafia_nick=data['mafia_nick'],
        age=age,
        telegram_username=message.from_user.username,
    )

    if age < 18:
        await message.answer(get_age_restriction_notice())

    await message.answer(
        "Спасибо за знакомство!☺️\n\n"
        "Обрати внимание на кнопки меню ниже. С их помощью ты сможешь:\n"
        "• Записаться на игру\n"
        "• Отменить запись на игру\n"
        "• Посмотреть, кто записался на ближайшие игры\n"
        "• Посмотреть расписание ближайших игр\n"
        "• Узнать, как до нас добраться\n\n"
        "Если возникнут вопросы - пиши Нате @natabordo",
        reply_markup=main_menu_keyboard(message.from_user.id)
    )
    await state.set_state(Form.menu)


@dp.message(Form.menu, F.text == "📝 Обновить профиль")
async def edit_profile_start(message: types.Message, state: FSMContext):
    await message.answer("Давай обновим профиль. Какой у тебя сейчас игровой ник в мафии?")
    await state.set_state(Form.edit_profile_nick)


@dp.message(Form.edit_profile_nick)
async def edit_profile_nick_handler(message: types.Message, state: FSMContext):
    await state.update_data(edit_mafia_nick=message.text)
    await message.answer("Отлично! Теперь напиши своё имя.")
    await state.set_state(Form.edit_profile_name)


@dp.message(Form.edit_profile_name)
async def edit_profile_name_handler(message: types.Message, state: FSMContext):
    await state.update_data(edit_first_name=message.text)
    await message.answer("И напоследок напиши свою фамилию.")
    await state.set_state(Form.edit_profile_lastname)


@dp.message(Form.edit_profile_lastname)
async def edit_profile_lastname_handler(message: types.Message, state: FSMContext):
    await state.update_data(edit_last_name=message.text)
    await message.answer("И последнее: сколько тебе лет?")
    await state.set_state(Form.edit_profile_age)


@dp.message(Form.edit_profile_age)
async def edit_profile_age_handler(message: types.Message, state: FSMContext):
    try:
        age = int(message.text)
    except (ValueError, TypeError):
        await message.answer("Пожалуйста, введи корректный возраст цифрами.")
        return

    data = await state.get_data()
    save_platform_profile(
        platform=PLATFORM_TELEGRAM,
        platform_user_id=message.from_user.id,
        first_name=data["edit_first_name"],
        last_name=data["edit_last_name"],
        mafia_nick=data["edit_mafia_nick"],
        age=age,
        telegram_username=message.from_user.username,
    )

    if age < 18:
        await message.answer(get_age_restriction_notice())

    await message.answer("Профиль обновлен.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.message(Form.menu, F.text == "⚙️ Админ-панель")
async def admin_panel(message: types.Message, state: FSMContext):
    if message.from_user.id != ADMIN_ID:
        return
    await message.answer("Добро пожаловать в админ-панель!", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.admin_menu)
async def admin_menu_handler(message: types.Message, state: FSMContext):
    if message.text == "➕ Добавить игру":
        await message.answer("Выберите дату игры:", reply_markup=await SimpleCalendar().start_calendar())
        await state.set_state(Form.add_game_date)
    elif message.text == "❌ Удалить игру":
        games = sort_games_by_date(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True))
        if not games:
            await message.answer("Список активных игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{name} {date}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Какую игру удалить?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.delete_game)
    elif message.text == "♻️ Восстановить игру":
        games = sort_games_by_date(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = TRUE", fetch=True))
        if not games:
            await message.answer("Нет удаленных игр для восстановления.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{name} {date}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Какую игру восстановить?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.restore_game)
    elif message.text == "👥 Список участников":
        games = sort_games_by_date(execute_query("SELECT game_id, game_name, game_date FROM games", fetch=True))
        if not games:
            await message.answer("Список игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру для просмотра списка участников:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.view_participants)
    elif message.text == "🚫 Отмена игры":
        games = sort_games_by_date(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True))
        if not games:
            await message.answer("Список игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру для отмены и уведомления игроков:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_cancel_game)
    elif message.text == "🔔 Напомнить об игре":
        games = sort_games_by_date(filter_upcoming_games(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)))
        if not games:
            await message.answer("Список игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру, о которой нужно напомнить:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_reminder)
    elif message.text == "📢 Рассылка":
        builder = ReplyKeyboardBuilder()
        builder.button(text="👥 Всем пользователям")
        builder.button(text="✅ Только записавшимся")
        builder.button(text="❌ Только не записавшимся")
        builder.button(text="👤 Выбрать пользователей")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите аудиторию для рассылки:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_broadcast)
    elif message.text == "🏠 Главное меню":
        await message.answer("Вы вернулись в главное меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)

@dp.message(Form.edit_schedule)
async def process_edit_schedule(message: types.Message, state: FSMContext):
    execute_query("UPDATE settings SET value = %s WHERE key = 'schedule'", (message.text,))
    await message.answer("Расписание успешно обновлено!", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.callback_query(SimpleCalendarCallback.filter())
async def process_simple_calendar(callback_query: types.CallbackQuery, callback_data: SimpleCalendarCallback, state: FSMContext):
    if callback_data.act == SimpleCalAct.cancel:
        current_state = await state.get_state()
        if current_state == Form.add_game_date.state:
            await callback_query.message.answer("Добавление игры отменено.", reply_markup=admin_menu_keyboard())
            await state.set_state(Form.admin_menu)
        await callback_query.answer()
        await callback_query.message.edit_reply_markup(reply_markup=None)
        return

    selected, date = await SimpleCalendar().process_selection(callback_query, callback_data)
    if selected:
        # Форматируем дату: Сб 21.02
        days = ['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс']
        day_str = days[date.weekday()]
        formatted_date = f"{day_str} {date.strftime('%d.%m')}"

        await state.update_data(game_date=formatted_date)

        builder = ReplyKeyboardBuilder()
        builder.button(text="🏙️Городская мафия")
        builder.button(text="🌃Спортивная мафия")
        builder.button(text="🏆Рейтинговая игра")
        builder.adjust(1)

        await callback_query.message.answer(
            f"Выбрана дата: {formatted_date}\nТеперь выберите тип игры:",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(Form.add_game_type)

@dp.message(Form.add_game_type)
async def process_add_game_type(message: types.Message, state: FSMContext):
    if message.text not in ["🏙️Городская мафия", "🌃Спортивная мафия", "🏆Рейтинговая игра"]:
        await message.answer("Пожалуйста, выберите один из вариантов кнопками.")
        return

    data = await state.get_data()
    date = data['game_date']
    name = message.text

    execute_query("INSERT INTO games (game_date, game_name) VALUES (%s, %s)", (date, name))
    await message.answer(f"Игра '{date} {name}' успешно добавлена!", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.delete_game)
async def delete_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Ты вернулся в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    result = execute_query("SELECT game_id FROM games WHERE game_name || ' ' || game_date = %s AND is_deleted = FALSE", (message.text,), fetchone=True)
    if result:
        game_id = result[0]
        execute_query("UPDATE games SET is_deleted = TRUE WHERE game_id = %s", (game_id,))
        await message.answer(f"Игра '{message.text}' удалена. Ты можешь восстановить её через меню восстановления.", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.restore_game)
async def restore_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Ты вернулся в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    result = execute_query("SELECT game_id FROM games WHERE game_name || ' ' || game_date = %s AND is_deleted = TRUE", (message.text,), fetchone=True)
    if result:
        game_id = result[0]
        execute_query("UPDATE games SET is_deleted = FALSE WHERE game_id = %s", (game_id,))
        await message.answer(f"Игра '{message.text}' успешно восстановлена вместе со всеми участниками!", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

# Для админ-панели
@dp.message(Form.view_participants)
async def admin_view_participants_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    # Текст кнопки = "date name", поэтому ищем так же
    clean_text = message.text.replace("📅", "").strip() if message.text else ""
    result = execute_query(
        "SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR game_name || ' ' || game_date = %s",
        (clean_text, clean_text),
        fetchone=True
    )

    if not result:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    game_id = result[0]

    # Получаем зарегистрированных участников
    participants = execute_query("""
        SELECT u.user_id, u.first_name, u.last_name, u.mafia_nick, u.telegram_username, u.vk_username, u.platform, u.platform_user_id
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s
            AND r.status = %s
    """, (game_id,'registered'), fetch=True)

    # Получаем думающих через Redis
    thinking_users = await get_thinking(game_id)
    thinking_users = set(map(int, thinking_users))  # строки в int
    late_users = await get_late_players(game_id)

    if not participants and not thinking_users:
        await message.answer(f"На игру '{message.text}' пока никто не записался.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    # Формируем текст с участниками
    response = f"Список участников на игру {message.text}:\n"

    # Основные участники
    regular_participants = [p for p in participants if p[0] not in late_users]
    late_participants = [p for p in participants if p[0] in late_users]
    ordered_participants = regular_participants + late_participants

    for i, (user_id, fn, ln, nick, tg_username, vk_username, platform, platform_user_id) in enumerate(ordered_participants, 1):
        username_text = build_profile_link(platform, platform_user_id, tg_username, vk_username)
        mark = " (думает)" if user_id in thinking_users else ""
        late_mark = " (опоздает)" if user_id in late_users else ""
        response += f"{i}. {fn} {ln} ({nick}, {username_text}){mark}{late_mark}\n"

    # Добавляем думающих, которых нет среди зарегистрированных
    for uid in thinking_users:
        if not any(uid == user_id for user_id, *_ in participants):
            ud = execute_query(
                "SELECT first_name, last_name, mafia_nick, telegram_username, vk_username, platform, platform_user_id FROM users WHERE user_id=%s",
                (uid,),
                fetchone=True
            )
            if ud:
                username_text = build_profile_link(ud[5], ud[6], ud[3], ud[4])
                response += f"- {ud[0]} {ud[1]} ({ud[2]}, {username_text}) (думает)\n"

    await message.answer(response, reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.menu)
async def menu_handler(message: types.Message, state: FSMContext):
    if message.text == "📝Записаться на игру":
        games = sort_games_by_date(filter_upcoming_games(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)))
        if not games:
            await message.answer("К сожалению, на данный момент игр для записи нет.", reply_markup=main_menu_keyboard(message.from_user.id))
            return

        builder = InlineKeyboardBuilder()
        for game_id, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}", callback_data=f"reg_{game_id}")
        builder.button(text="🔙 В меню", callback_data="menu_back")
        builder.adjust(1)

        await message.answer("На какую игру ты хочешь записаться?", reply_markup=builder.as_markup())
        await state.set_state(Form.menu)
    elif message.text == "❌Отменить запись":
        internal_user_id = telegram_internal_user_id(message.from_user)
        games = execute_query("""
            SELECT g.game_id, g.game_name, g.game_date 
            FROM registrations r
            JOIN games g ON r.game_id=g.game_id
            WHERE r.user_id=%s
        """, (internal_user_id,), fetch=True)
        games = sort_games_by_date(filter_upcoming_games(games))
        if not games:
            await message.answer("Ты пока не записан ни на какую игру.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = InlineKeyboardBuilder()
        for game_id, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}", callback_data=f"cancel_{game_id}")
        builder.button(text="🔙 В меню", callback_data="menu_back")
        builder.adjust(1)

        await message.answer("Запись на какую игру ты хочешь отменить?", reply_markup=builder.as_markup())
        await state.set_state(Form.menu)
    elif message.text == "📅Расписание игр":
        games = sort_games_by_date(
            [g for g in execute_query("SELECT game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True) if is_upcoming_game(g[1])],
            date_index=1
        )
        if not games:
            await message.answer("<b>Расписание ближайших игр:</b>\n\nИгр пока не запланировано.", parse_mode="HTML")
            return
        schedule_text = "<b>Расписание ближайших игр:</b>\n\n"
        for name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            schedule_text += f"📆{date} {display_name}\n"
            schedule_text += get_game_rules(display_name)
        await message.answer(schedule_text.strip(), parse_mode="HTML")
    elif message.text == "📍Как до нас добраться?":
        await message.answer(
            "<b>Мы находимся по адресу</b>\n\n"
            "г. Королев, ул. Декабристов, д. 8\n"
            "Вход со стороны дороги (не со двора), ищи стеклянную дверь с надписью «Тайная комната» и спускайся по лестнице в самый низ.",
            parse_mode="HTML"
        )
    elif message.text == "👥Список участников":
        games = sort_games_by_date(filter_upcoming_games(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)))
        if not games:
            await message.answer("К сожалению, на данный момент игр нет.", reply_markup=main_menu_keyboard(message.from_user.id))
            return

        builder = InlineKeyboardBuilder()
        for game_id, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📅{date} {display_name}", callback_data=f"participants_{game_id}")
        builder.button(text="🔙 В меню", callback_data="menu_back")
        builder.adjust(1)

        await message.answer("Список участников какой игры ты хочешь посмотреть?", reply_markup=builder.as_markup())
        await state.set_state(Form.menu)

@dp.callback_query(F.data.startswith("participants_"))
async def callback_participants(callback: types.CallbackQuery, state: FSMContext):
    game_id = int(callback.data.split("_")[1])

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s AND is_deleted = FALSE", (game_id,), fetchone=True)
    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    game_name, game_date = game

    participants = execute_query("""
        SELECT u.user_id, u.mafia_nick
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s
            AND r.status = %s
    """, (game_id,'registered'), fetch=True)

    thinking_users = await get_thinking(game_id)
    thinking_users = set(map(int, thinking_users))
    late_users = await get_late_players(game_id)

    title = f"📅{game_date} {game_name}"
    if not participants and not thinking_users:
        await callback.message.answer(f"На игру {title} пока никто не записался.", reply_markup=main_menu_keyboard(callback.from_user.id))
    else:
        response = f"Список участников на игру {title}:\n"
        participant_ids = {uid for uid, _ in participants}
        regular_participants = [p for p in participants if p[0] not in late_users]
        late_participants = [p for p in participants if p[0] in late_users]

        idx = 1
        for uid, nick in regular_participants + late_participants:
            mark = " (думает)" if uid in thinking_users else ""
            late_mark = " (опоздает)" if uid in late_users else ""
            response += f"{idx}. {nick}{mark}{late_mark}\n"
            idx += 1

        for uid in thinking_users:
            if uid not in participant_ids:
                ud = execute_query("SELECT mafia_nick FROM users WHERE user_id=%s", (uid,), fetchone=True)
                if ud:
                    response += f"- {ud[0]} (думает)\n"

        await callback.message.answer(response, reply_markup=main_menu_keyboard(callback.from_user.id))

    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Form.menu)

@dp.callback_query(F.data == "menu_back")
async def callback_menu_back(callback: types.CallbackQuery, state: FSMContext):
    await callback.answer()
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(callback.from_user.id))
    await state.set_state(Form.menu)

@dp.callback_query(F.data.startswith("cancel_"))
async def callback_cancel(callback: types.CallbackQuery, state: FSMContext):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    game_name, game_date = game

    execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (user_id, game_id))
    await unmark_late(user_id, game_id)
    execute_query("DELETE FROM registrations WHERE user_id=%s AND game_id=%s", (user_id, game_id))

    await callback.message.answer(
        "Запись отменена.\n"
        "Спасибо за то, что уважаешь клуб и других игроков!☺️\n"
        "Будем ждать тебя на следующих играх.",
        reply_markup=main_menu_keyboard(user_id),
        parse_mode="HTML"
    )

    ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if ud:
        await notify_admin(f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]}) на {game_date} {game_name}")

    await callback.answer("Запись отменена")
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Form.menu)

@dp.message(Form.user_view_participants)
async def user_view_participants_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    clean_text = message.text.replace("📅", "").strip() if message.text else ""
    result = execute_query("SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR game_name || ' ' || game_date = %s", (clean_text, clean_text), fetchone=True)
    if result:
        game_id = result[0]
        participants = execute_query("""
            SELECT u.user_id, u.mafia_nick
            FROM registrations r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.game_id = %s
                AND r.status = %s
        """, (game_id,'registered'), fetch=True)

        # Получаем думающих через Redis
        thinking_users = await get_thinking(game_id)
        thinking_users = set(map(int, thinking_users))
        late_users = await get_late_players(game_id)

        if not participants and not thinking_users:
            await message.answer(f"На игру {message.text} пока никто не записался.", reply_markup=main_menu_keyboard(message.from_user.id))
        else:
            response = f"Список участников на игру {message.text}:\n"
            regular_participants = [p for p in participants if p[0] not in late_users]
            late_participants = [p for p in participants if p[0] in late_users]
            participant_ids = {uid for uid, _ in participants}
            idx = 1
            for uid, nick in regular_participants + late_participants:
                mark = " (думает)" if uid in thinking_users else ""
                late_mark = " (опоздает)" if uid in late_users else ""
                response += f"{idx}. {nick}{mark}{late_mark}\n"
                idx += 1

            for uid in thinking_users:
                # Проверяем, что не в списке основных
                if uid not in participant_ids:
                    ud = execute_query("SELECT mafia_nick FROM users WHERE user_id=%s", (uid,), fetchone=True)
                    if ud:
                        response += f"- {ud[0]} (думает)\n"

            await message.answer(response, reply_markup=main_menu_keyboard(message.from_user.id))
    else:
        await message.answer("Игра не найдена.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.message(Form.game_registration)
async def register_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    internal_user_id = telegram_internal_user_id(message.from_user)
    clean_text = message.text.replace("📆", "").strip() if message.text else ""
    result = execute_query(
        """
        SELECT game_id, game_name, game_date
        FROM games
        WHERE is_deleted = FALSE
          AND %s LIKE '%' || game_date || '%'
          AND (
              %s LIKE '%' || game_name || '%'
              OR %s LIKE '%' || REPLACE(game_name, '🏆', '🌃') || '%'
          )
        """,
        (clean_text, clean_text, clean_text),
        fetchone=True
        )
    if result:
        game_id, game_name, game_date = result
        user_age_row = execute_query("SELECT age FROM users WHERE user_id = %s", (internal_user_id,), fetchone=True)
        user_age = user_age_row[0] if user_age_row else None
        age_rejection = get_registration_age_rejection(game_name, user_age)
        if age_rejection:
            await message.answer(age_rejection, reply_markup=main_menu_keyboard(message.from_user.id))
            await state.set_state(Form.menu)
            return

        if await is_game_full(game_id, game_name, internal_user_id):
            await message.answer(
                "К сожалению, на данную игру записалось максимальное количество участников😢\n"
                "Попробуй записаться на другую игру или напиши Нате @natabordo, возможно она сможет что-то придумать☺️",
                reply_markup=main_menu_keyboard(message.from_user.id)
            )
            await state.set_state(Form.menu)
            return

        # Удаляем из списка думающих при регистрации
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (internal_user_id, game_id))
        execute_query("""
            INSERT INTO registrations (user_id, game_id, status, is_late)
            VALUES (%s, %s, 'registered', FALSE)
            ON CONFLICT (user_id, game_id)
            DO UPDATE SET status = 'registered', is_late = FALSE
        """, (internal_user_id, game_id))
        await message.answer(
            build_registration_success_text(game_date, game_name),
            reply_markup=late_button_keyboard(game_id)
        )
        ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
        if ud:
            await notify_admin(f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    else:
        await message.answer("Не удалось найти выбранную игру. Попробуй выбрать её из списка ещё раз.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.message(Form.game_cancellation)
async def cancel_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    internal_user_id = telegram_internal_user_id(message.from_user)
    clean_text = message.text.replace("📆", "").strip() if message.text else ""
    result = execute_query(
        """
        SELECT game_id
        FROM games
        WHERE is_deleted = FALSE
          AND (
              game_date || ' ' || game_name = %s
              OR game_name || ' ' || game_date = %s
              OR game_date || ' ' || REPLACE(game_name, '🏆', '🌃') = %s
          )
        """,
        (clean_text, clean_text, clean_text),
        fetchone=True
    )
    if result:
        game_id = result[0]
        # Удаляем из всех списков
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (internal_user_id, game_id))
        await unmark_late(internal_user_id, game_id)
        execute_query("DELETE FROM registrations WHERE user_id=%s AND game_id=%s", (internal_user_id, game_id))
        await message.answer("Запись отменена.\n"
                             "Спасибо за то, что уважаешь клуб и других игроков!☺️\n"
                             "Будем ждать тебя на следующих играх.",
                             reply_markup=main_menu_keyboard(message.from_user.id),
                             parse_mode="HTML"
                            )
        ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
        if ud:
            await notify_admin(f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    else:
        await message.answer("Не удалось найти выбранную игру. Попробуй выбрать её из списка ещё раз.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.callback_query(F.data.startswith("think_"))
async def callback_think(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)

    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    # Сохраняем игрока в БД как думающего
    await mark_thinking(user_id, game_id)

    await callback.answer("Админ уведомлен, что вы думаете! 😊")
    await callback.message.edit_reply_markup(reply_markup=None)

    # Notify admin
    ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if ud:
        await notify_admin(f"🤔 Игрок думает: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("reg_"))
async def callback_reg(callback: types.CallbackQuery, state: FSMContext):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    game = execute_query(
        "SELECT game_name, game_date FROM games WHERE game_id = %s",
        (game_id,),
        fetchone=True
    )

    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    game_name, game_date = game
    user_age_row = execute_query("SELECT age FROM users WHERE user_id = %s", (user_id,), fetchone=True)
    user_age = user_age_row[0] if user_age_row else None
    age_rejection = get_registration_age_rejection(game_name, user_age)
    if age_rejection:
        await callback.message.answer(age_rejection, reply_markup=main_menu_keyboard(callback.from_user.id))
        await callback.answer("Возрастное ограничение", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=None)
        await state.set_state(Form.menu)
        return

    if await is_game_full(game_id, game_name, user_id):
        await callback.message.answer(
            "К сожалению, на данную игру записалось максимальное количество участников😢\n"
            "Попробуй записаться на другую игру или напиши Нате @natabordo, возможно она сможет что-то придумать☺️",
            reply_markup=main_menu_keyboard(user_id)
        )
        await callback.answer("На игру больше нельзя записаться", show_alert=True)
        await callback.message.edit_reply_markup(reply_markup=None)
        await state.set_state(Form.menu)
        return

    # Удаляем из списка думающих
    execute_query(
        "DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s",
        (user_id, game_id)
    )

    # Регистрируем или обновляем статус
    execute_query("""
        INSERT INTO registrations (user_id, game_id, status, is_late)
        VALUES (%s, %s, 'registered', FALSE)
        ON CONFLICT (user_id, game_id)
        DO UPDATE SET status = 'registered', is_late = FALSE
    """, (user_id, game_id))

    await callback.message.answer(
        build_registration_success_text(game_date, game_name),
        reply_markup=late_button_keyboard(game_id)
    )

    await callback.answer("Запись подтверждена! 😊")
    await callback.message.edit_reply_markup(reply_markup=None)
    await state.set_state(Form.menu)

    # Уведомление админу
    ud = execute_query(
        "SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s",
        (user_id,),
        fetchone=True
    )

    if ud:
        await notify_admin(f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {game_date} {game_name}")

@dp.callback_query(F.data.startswith("late_"))
async def callback_late(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    reg = execute_query(
        "SELECT 1 FROM registrations WHERE user_id=%s AND game_id=%s AND status='registered'",
        (user_id, game_id),
        fetchone=True
    )
    if not reg:
        await callback.answer("Сначала нужно записаться на игру.", show_alert=True)
        return

    await mark_late(user_id, game_id)
    await callback.answer("Отметили, что вы опоздаете ⏰")
    await callback.message.edit_reply_markup(reply_markup=None)

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
    ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if game and ud:
        await notify_admin(f"⏰ Опоздает: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("decline_"))
async def callback_decline(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    execute_query("""
        INSERT INTO registrations (user_id, game_id, status)
        VALUES (%s, %s, 'declined')
        ON CONFLICT (user_id, game_id)
        DO UPDATE SET status = 'declined'
    """, (user_id, game_id))
    await unmark_late(user_id, game_id)

    await callback.answer("Спасибо за ответ!")
    await callback.message.edit_reply_markup(reply_markup=None)

    ud = execute_query(
        "SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s",
        (user_id,),
        fetchone=True
    )

    if ud:
        await notify_admin(f"❌ Отказ от игры: {ud[0]} {ud[1]} ({ud[2]})")

@dp.callback_query(F.data.startswith("cancelreg_"))
async def callback_cancel_registration(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = telegram_internal_user_id(callback.from_user)

    execute_query("""
        UPDATE registrations
        SET status = 'declined'
        WHERE user_id=%s AND game_id=%s
    """, (user_id, game_id))
    await unmark_late(user_id, game_id)

    await callback.answer("Запись отменена!")
    await callback.message.edit_reply_markup(reply_markup=None)

    ud = execute_query(
        "SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s",
        (user_id,),
        fetchone=True
    )

    if ud:
        await notify_admin(f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]})")

@dp.message(Form.admin_cancel_game)
async def admin_cancel_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    result = execute_query("SELECT game_id, game_name, game_date FROM games WHERE game_date || ' ' || game_name = %s", (message.text,), fetchone=True)
    if result:
        game_id = result[0]
        game_info = message.text
        participants = execute_query("SELECT user_id FROM registrations WHERE game_id = %s", (game_id,), fetch=True)
        for (user_id,) in participants:
            try:
                await send_text_to_user(user_id, f"⚠️ Внимание! Отмена игры на {game_info}! ⚠️")
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        execute_query("DELETE FROM registrations WHERE game_id = %s", (game_id,))
        execute_query("DELETE FROM games WHERE game_id = %s", (game_id,))
        await message.answer(f"Игра '{game_info}' отменена. Игроки ({len(participants)} чел.) уведомлены.", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.admin_reminder)
async def admin_reminder_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    clean_text = message.text.replace("📆", "").strip() if message.text else ""
    result = execute_query("SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR '📆' || game_date || ' ' || game_name = %s OR game_date || ' ' || game_name = %s", (clean_text, message.text, message.text), fetchone=True)

    if result:
        await state.update_data(reminder_game_id=result[0], reminder_game_text=message.text)
        builder = ReplyKeyboardBuilder()
        builder.button(text="👥 Всем пользователям")
        builder.button(text="✅ Только записавшимся")
        builder.button(text="❌ Только не записавшимся")
        builder.button(text="👤 Выбрать пользователей")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Кому отправить напоминание?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_reminder_audience)
    else:
        await message.answer("Игра не найдена. Попробуйте выбрать игру из списка еще раз.")

@dp.message(Form.admin_reminder_audience)
async def admin_reminder_audience_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        games = sort_games_by_date(execute_query("SELECT game_id, game_name, game_date FROM games", fetch=True))
        if not games:
            await message.answer("Список игр пуст.", reply_markup=admin_menu_keyboard())
            await state.set_state(Form.admin_menu)
            return

        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру, о которой нужно напомнить:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_reminder)
        return

    data = await state.get_data()
    game_id = data.get('reminder_game_id')

    target_users = []
    if message.text == "👥 Всем пользователям":
        rows = execute_query("SELECT user_id FROM users", fetch=True)
        target_users = [r[0] for r in rows]
    elif message.text == "✅ Только записавшимся":
        rows = execute_query("SELECT user_id FROM registrations WHERE game_id = %s", (game_id,), fetch=True)
        target_users = [r[0] for r in rows]
    elif message.text == "❌ Только не записавшимся":
        rows = execute_query("SELECT user_id FROM users WHERE user_id NOT IN (SELECT user_id FROM registrations WHERE game_id = %s)", (game_id,), fetch=True)
        target_users = [r[0] for r in rows]
    elif message.text == "👤 Выбрать пользователей":
        users = execute_query("SELECT user_id, first_name, last_name, mafia_nick FROM users", fetch=True)
        if not users:
            await message.answer("Пользователей не найдено.")
            return

        await state.update_data(all_users_for_selection=users, selected_users=[])

        builder = InlineKeyboardBuilder()
        for uid, fn, ln, nick in users:
            builder.button(text=f"{fn} {ln} ({nick})", callback_data=f"seluser_{uid}")
        builder.button(text="✅ Готово", callback_data="seluser_done")
        builder.adjust(1)

        await message.answer("Выберите пользователей из списка:", reply_markup=builder.as_markup())
        await state.set_state(Form.admin_reminder_custom_users)
        return
    else:
        await message.answer("Пожалуйста, используйте кнопки.")
        return

    if not target_users:
        await message.answer("Нет пользователей, подходящих под критерии.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    count = await send_game_reminders(target_users, game_id)
    await message.answer(f"Напоминания отправлены {count} пользователям.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.callback_query(Form.admin_reminder_custom_users, F.data.startswith("seluser_"))
async def process_user_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('selected_users', [])
    all_users = data.get('all_users_for_selection', [])

    action = callback.data.split("_")[1]

    if action == "done":
        if not selected:
            await callback.answer("Никто не выбран!", show_alert=True)
            return

        game_id = data.get('reminder_game_id')
        count = await send_game_reminders(selected, game_id)
        await callback.message.edit_text(f"Напоминания отправлены {count} выбранным пользователям.")
        await callback.message.answer("Возвращаюсь в админ-меню.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        await callback.answer()
        return

    user_id = int(action)
    if user_id in selected:
        selected.remove(user_id)
        await callback.answer("Пользователь удален из списка")
    else:
        selected.append(user_id)
        await callback.answer("Пользователь добавлен в список")

    await state.update_data(selected_users=selected)

    builder = InlineKeyboardBuilder()
    for uid, fn, ln, nick in all_users:
        mark = "✅ " if uid in selected else ""
        builder.button(text=f"{mark}{fn} {ln} ({nick})", callback_data=f"seluser_{uid}")
    builder.button(text="✅ Готово", callback_data="seluser_done")
    builder.adjust(1)

    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())

async def send_game_reminders(user_ids, game_id):
    count = 0

    game_data = execute_query(
        "SELECT game_name, game_date FROM games WHERE game_id = %s",
        (game_id,),
        fetchone=True
    )

    if not game_data:
        return 0

    g_name, g_date = game_data

    for uid in user_ids:
        try:
            row = execute_query(
                "SELECT status FROM registrations WHERE user_id=%s AND game_id=%s",
                (uid, game_id),
                fetchone=True
            )

            # Если пользователь отказался — не шлём повторно
            if row and row[0] == "declined":
                continue

            builder = InlineKeyboardBuilder()

            if row and row[0] == "registered":
                builder.button(
                    text="❌ Отменить запись",
                    callback_data=f"cancelreg_{game_id}"
                )
                builder.button(text="⏰ Опоздаю", callback_data=f"late_{game_id}")
            else:
                builder.button(text="📝 Записаться", callback_data=f"reg_{game_id}")
                builder.button(text="🤔 Думаю", callback_data=f"think_{game_id}")
                builder.button(text="❌ Не приду", callback_data=f"decline_{game_id}")
                builder.adjust(2)

            if detect_platform_by_user_id(uid) == PLATFORM_TELEGRAM:
                await bot.send_message(
                    get_platform_user_id(uid),
                    f"🔔 Напоминание об игре: {g_date} {g_name}\nБудем вас ждать! 😊",
                    reply_markup=builder.as_markup()
                )
            else:
                await send_text_to_user(
                    uid,
                    f"🔔 Напоминание об игре: {g_date} {g_name}\nБудем вас ждать! 😊",
                    reply_markup=vk_reminder_actions_keyboard(game_id, bool(row and row[0] == "registered"))
                )

            count += 1

        except Exception as e:
            logging.error(f"Не удалось отправить напоминание {uid}: {e}")

    return count

@dp.message(Form.admin_broadcast)
async def admin_broadcast_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    if message.text == "👥 Всем пользователям":
        users = execute_query("SELECT user_id FROM users", fetch=True)
        target_users = [uid for (uid,) in users]
        await state.update_data(broadcast_target_users=target_users)
        await message.answer("Введите сообщение для рассылки:")
        await state.set_state(Form.admin_broadcast_message)
        return

    if message.text in ["✅ Только записавшимся", "❌ Только не записавшимся"]:
        games = sort_games_by_date(filter_upcoming_games(execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)))
        if not games:
            await message.answer("Нет доступных игр для выбора.", reply_markup=admin_menu_keyboard())
            await state.set_state(Form.admin_menu)
            return

        await state.update_data(broadcast_filter_type=message.text)
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру для фильтра аудитории:", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.admin_broadcast_game)
        return

    if message.text == "👤 Выбрать пользователей":
        users = execute_query("SELECT user_id, first_name, last_name, mafia_nick FROM users", fetch=True)
        if not users:
            await message.answer("Пользователей не найдено.")
            return

        await state.update_data(all_users_for_broadcast_selection=users, selected_broadcast_users=[])
        builder = InlineKeyboardBuilder()
        for uid, fn, ln, nick in users:
            builder.button(text=f"{fn} {ln} ({nick})", callback_data=f"bseluser_{uid}")
        builder.button(text="✅ Готово", callback_data="bseluser_done")
        builder.adjust(1)
        await message.answer("Выберите пользователей из списка:", reply_markup=builder.as_markup())
        await state.set_state(Form.admin_broadcast_custom_users)
        return

    await message.answer("Пожалуйста, используйте кнопки.")

@dp.message(Form.admin_broadcast_game)
async def admin_broadcast_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    clean_text = message.text.strip() if message.text else ""
    result = execute_query(
        "SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR game_name || ' ' || game_date = %s",
        (clean_text, clean_text),
        fetchone=True
    )
    if not result:
        await message.answer("Игра не найдена. Выбери игру кнопкой из списка.")
        return

    game_id = result[0]
    data = await state.get_data()
    filter_type = data.get("broadcast_filter_type")

    if filter_type == "✅ Только записавшимся":
        rows = execute_query("SELECT user_id FROM registrations WHERE game_id = %s AND status = 'registered'", (game_id,), fetch=True)
        target_users = [r[0] for r in rows]
    else:
        rows = execute_query(
            "SELECT user_id FROM users WHERE user_id NOT IN (SELECT user_id FROM registrations WHERE game_id = %s AND status = 'registered')",
            (game_id,),
            fetch=True
        )
        target_users = [r[0] for r in rows]

    await state.update_data(broadcast_target_users=target_users)
    await message.answer("Введите сообщение для рассылки:")
    await state.set_state(Form.admin_broadcast_message)

@dp.callback_query(Form.admin_broadcast_custom_users, F.data.startswith("bseluser_"))
async def process_broadcast_user_selection(callback: types.CallbackQuery, state: FSMContext):
    data = await state.get_data()
    selected = data.get('selected_broadcast_users', [])
    all_users = data.get('all_users_for_broadcast_selection', [])

    action = callback.data.split("_")[1]
    if action == "done":
        if not selected:
            await callback.answer("Никто не выбран!", show_alert=True)
            return

        await state.update_data(broadcast_target_users=selected)
        await callback.message.edit_text("Пользователи выбраны.")
        await callback.message.answer("Введите сообщение для рассылки:")
        await state.set_state(Form.admin_broadcast_message)
        await callback.answer()
        return

    user_id = int(action)
    if user_id in selected:
        selected.remove(user_id)
        await callback.answer("Пользователь удален из списка")
    else:
        selected.append(user_id)
        await callback.answer("Пользователь добавлен в список")

    await state.update_data(selected_broadcast_users=selected)

    builder = InlineKeyboardBuilder()
    for uid, fn, ln, nick in all_users:
        mark = "✅ " if uid in selected else ""
        builder.button(text=f"{mark}{fn} {ln} ({nick})", callback_data=f"bseluser_{uid}")
    builder.button(text="✅ Готово", callback_data="bseluser_done")
    builder.adjust(1)
    await callback.message.edit_reply_markup(reply_markup=builder.as_markup())

@dp.message(Form.admin_broadcast_message)
async def admin_broadcast_message_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    data = await state.get_data()
    users = data.get("broadcast_target_users", [])
    if not users:
        await message.answer("Нет пользователей для рассылки.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    count = 0
    for user_id in users:
        try:
            await send_text_to_user(user_id, message.text)
            count += 1
        except Exception as e:
            logging.error(f"Error sending broadcast to {user_id}: {e}")

    await message.answer(f"Сообщение отправлено {count} пользователям.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)


def set_vk_state(user_id: int, state_name: str, **data):
    vk_states[user_id] = {"state": state_name, **data}


def clear_vk_state(user_id: int):
    vk_states.pop(user_id, None)


def get_vk_state(user_id: int):
    return vk_states.get(user_id, {"state": "menu"})


def send_vk_message(user_id: int, text: str, keyboard: str = None):
    if not vk_api_client:
        logging.warning("VK API client недоступен, сообщение не отправлено пользователю %s", user_id)
        return
    vk_api_client.messages.send(
        user_id=user_id,
        random_id=uuid.uuid4().int & 0x7FFFFFFF,
        message=text,
        keyboard=keyboard
    )


def prompt_vk_main_menu(vk_user_id: int):
    send_vk_message(vk_user_id, "Выбери действие в меню ниже.", vk_main_menu_keyboard(make_internal_user_id(PLATFORM_VK, vk_user_id)))
    clear_vk_state(make_internal_user_id(PLATFORM_VK, vk_user_id))


def parse_vk_payload(payload_raw):
    if not payload_raw:
        return {}
    if isinstance(payload_raw, dict):
        return payload_raw
    if isinstance(payload_raw, str):
        try:
            parsed = json.loads(payload_raw)
            return parsed if isinstance(parsed, dict) else {}
        except json.JSONDecodeError:
            logging.warning("Не удалось распарсить payload VK: %s", payload_raw)
    return {}


def vk_number_choice_keyboard(items_count: int, back_label: str = "🔙 Назад"):
    keyboard = VkKeyboard(one_time=True)
    per_row = 4

    for index in range(items_count):
        if index > 0 and index % per_row == 0:
            keyboard.add_line()
        keyboard.add_button(
            str(index + 1),
            color=VkKeyboardColor.PRIMARY,
            payload={"select_index": index}
        )

    if back_label:
        keyboard.add_line()
        keyboard.add_button(back_label, color=VkKeyboardColor.SECONDARY, payload={"command": "back"})

    return keyboard.get_keyboard()


def vk_option_keyboard(labels, back_label: str = "🔙 Назад"):
    keyboard = VkKeyboard(one_time=True)
    for index, label in enumerate(labels):
        if index > 0:
            keyboard.add_line()
        keyboard.add_button(label, color=VkKeyboardColor.PRIMARY, payload={"select_label": label})
    if back_label:
        keyboard.add_line()
        keyboard.add_button(back_label, color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_games_keyboard(games, back_label: str = "🔙 Назад"):
    keyboard = VkKeyboard(one_time=True)
    for index, (game_id, game_name, game_date) in enumerate(games):
        if index > 0:
            keyboard.add_line()
        keyboard.add_button(
            f"{game_date} {game_name}",
            color=VkKeyboardColor.PRIMARY,
            payload={"game_id": game_id}
        )
    if back_label:
        keyboard.add_line()
        keyboard.add_button(back_label, color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_late_button_keyboard(game_id: int):
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button("⏰ Опоздаю", color=VkKeyboardColor.SECONDARY, payload={"command": "mark_late", "game_id": game_id})
    keyboard.add_line()
    keyboard.add_button("🏠 В меню", color=VkKeyboardColor.PRIMARY, payload={"command": "main_menu"})
    return keyboard.get_keyboard()


def vk_reminder_actions_keyboard(game_id: int, is_registered: bool):
    keyboard = VkKeyboard(one_time=True)
    if is_registered:
        keyboard.add_button("❌ Отменить запись", color=VkKeyboardColor.SECONDARY, payload={"command": "reminder_cancel", "game_id": game_id})
        keyboard.add_line()
        keyboard.add_button("⏰ Опоздаю", color=VkKeyboardColor.SECONDARY, payload={"command": "reminder_late", "game_id": game_id})
    else:
        keyboard.add_button("📝 Записаться", color=VkKeyboardColor.PRIMARY, payload={"command": "reminder_register", "game_id": game_id})
        keyboard.add_line()
        keyboard.add_button("🤔 Думаю", color=VkKeyboardColor.SECONDARY, payload={"command": "reminder_think", "game_id": game_id})
        keyboard.add_line()
        keyboard.add_button("❌ Не приду", color=VkKeyboardColor.NEGATIVE, payload={"command": "reminder_decline", "game_id": game_id})
    keyboard.add_line()
    keyboard.add_button("🏠 В меню", color=VkKeyboardColor.PRIMARY, payload={"command": "main_menu"})
    return keyboard.get_keyboard()


def vk_reminder_user_selection_keyboard(users, selected_ids, page: int = 0, page_size: int = VK_REMINDER_USERS_PAGE_SIZE):
    keyboard = VkKeyboard(one_time=True)
    safe_page_size = max(1, min(page_size, VK_MAX_LINES - 2))
    total_pages = max(1, (len(users) + safe_page_size - 1) // safe_page_size)
    current_page = max(0, min(page, total_pages - 1))
    start = current_page * safe_page_size
    end = start + safe_page_size
    users_on_page = users[start:end]

    for index, (uid, first_name, last_name, nick) in enumerate(users_on_page):
        if index > 0:
            keyboard.add_line()
        mark = "✅ " if uid in selected_ids else ""
        keyboard.add_button(
            f"{mark}{first_name} {last_name} ({nick})",
            color=VkKeyboardColor.PRIMARY,
            payload={"command": "rem_sel_toggle", "user_id": uid, "page": current_page}
        )

    if total_pages > 1:
        if users_on_page:
            keyboard.add_line()
        if current_page > 0:
            keyboard.add_button("◀️", color=VkKeyboardColor.SECONDARY, payload={"command": "rem_sel_page", "page": current_page - 1})
        if current_page < total_pages - 1:
            keyboard.add_button("▶️", color=VkKeyboardColor.SECONDARY, payload={"command": "rem_sel_page", "page": current_page + 1})

    if users_on_page or total_pages > 1:
        keyboard.add_line()
    keyboard.add_button("✅ Готово", color=VkKeyboardColor.POSITIVE, payload={"command": "rem_sel_done", "page": current_page})
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_game_type_keyboard():
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button("🏙️Городская мафия", color=VkKeyboardColor.PRIMARY, payload={"game_type": "🏙️Городская мафия"})
    keyboard.add_line()
    keyboard.add_button("🌃Спортивная мафия", color=VkKeyboardColor.PRIMARY, payload={"game_type": "🌃Спортивная мафия"})
    keyboard.add_line()
    keyboard.add_button("🏆Рейтинговая игра", color=VkKeyboardColor.PRIMARY, payload={"game_type": "🏆Рейтинговая игра"})
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_calendar_keyboard(year: int, month: int):
    keyboard = VkKeyboard(one_time=True)
    days_in_month = calendar.monthrange(year, month)[1]

    for day in range(1, days_in_month + 1):
        if day > 1 and (day - 1) % VK_MAX_BUTTONS_ON_LINE == 0:
            keyboard.add_line()
        keyboard.add_button(str(day), color=VkKeyboardColor.PRIMARY, payload={"calendar_day": day})

    keyboard.add_line()
    keyboard.add_button("◀️ Месяц", color=VkKeyboardColor.SECONDARY, payload={"calendar_shift": -1})
    keyboard.add_button("▶️ Месяц", color=VkKeyboardColor.SECONDARY, payload={"calendar_shift": 1})
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def shift_year_month(year: int, month: int, delta: int):
    new_month = month + delta
    new_year = year
    if new_month < 1:
        new_month = 12
        new_year -= 1
    elif new_month > 12:
        new_month = 1
        new_year += 1
    return new_year, new_month


def vk_audience_keyboard():
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button("👥 Всем пользователям", color=VkKeyboardColor.PRIMARY, payload={"audience": "all"})
    keyboard.add_line()
    keyboard.add_button("✅ Только записавшимся", color=VkKeyboardColor.SECONDARY, payload={"audience": "registered"})
    keyboard.add_line()
    keyboard.add_button("❌ Только не записавшимся", color=VkKeyboardColor.SECONDARY, payload={"audience": "not_registered"})
    keyboard.add_line()
    keyboard.add_button("👤 Выбрать пользователей", color=VkKeyboardColor.SECONDARY, payload={"audience": "custom"})
    keyboard.add_line()
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_back_keyboard():
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button("🔙 Назад", color=VkKeyboardColor.SECONDARY, payload={"command": "back"})
    return keyboard.get_keyboard()


def vk_yes_no_keyboard():
    keyboard = VkKeyboard(one_time=True)
    keyboard.add_button("Да", color=VkKeyboardColor.SECONDARY, payload={"answer": "yes"})
    keyboard.add_button("Нет", color=VkKeyboardColor.SECONDARY, payload={"answer": "no"})
    return keyboard.get_keyboard()


def fetch_vk_user_profile(vk_user_id: int):
    if not vk_api_client:
        return {}
    try:
        users = vk_api_client.users.get(user_ids=vk_user_id, fields="screen_name")
        if users:
            return users[0]
    except Exception as e:
        logging.warning("Не удалось получить данные профиля VK для %s: %s", vk_user_id, e)
    return {}


def save_platform_profile(
    platform: str,
    platform_user_id: int,
    first_name: str,
    last_name: str,
    mafia_nick: str,
    age: int = None,
    telegram_username: str = None,
):
    internal_user_id = make_internal_user_id(platform, platform_user_id)
    existing = execute_query(
        "SELECT age, vk_username FROM users WHERE user_id = %s",
        (internal_user_id,),
        fetchone=True
    )
    saved_age = existing[0] if existing and existing[0] is not None else 18
    vk_username = existing[1] if existing and len(existing) > 1 else None
    resolved_age = saved_age if age is None else age
    return upsert_user(
        platform=platform,
        platform_user_id=platform_user_id,
        first_name=first_name,
        last_name=last_name,
        mafia_nick=mafia_nick,
        age=resolved_age,
        telegram_username=telegram_username,
        vk_username=vk_username,
    )


def send_vk_games_list(
    vk_user_id: int,
    games,
    action: str,
    title: str,
    back_label: str = "🔙 Назад",
    use_game_buttons: bool = False
):
    if not games:
        send_vk_message(vk_user_id, "Список игр сейчас пуст.", vk_main_menu_keyboard(make_internal_user_id(PLATFORM_VK, vk_user_id)))
        return

    if use_game_buttons:
        send_vk_message(vk_user_id, title, vk_games_keyboard(games, back_label=back_label))
    else:
        lines = [title]
        for index, (_, game_name, game_date) in enumerate(games, start=1):
            lines.append(f"{index}. {game_date} {game_name}")
        lines.append("")
        lines.append("Выбери игру кнопкой ниже или отправь её номер сообщением.")
        send_vk_message(vk_user_id, "\n".join(lines), vk_number_choice_keyboard(len(games), back_label=back_label))
    set_vk_state(make_internal_user_id(PLATFORM_VK, vk_user_id), action, games=games)


def get_vk_selected_game(state, selected_text: str, payload: dict = None):
    payload = payload or {}
    games = state.get("games", [])

    game_id = payload.get("game_id")
    if isinstance(game_id, int):
        for game in games:
            if int(game[0]) == game_id:
                return game
        return None

    if isinstance(payload.get("select_index"), int):
        index = payload["select_index"]
    elif selected_text.isdigit():
        index = int(selected_text) - 1
    else:
        normalized = selected_text.strip().lower()
        for game in games:
            game_label = f"{game[2]} {game[1]}".strip().lower()
            if normalized == game_label:
                return game
        return None

    if index < 0 or index >= len(games):
        return None
    return games[index]


def send_vk_user_selection_list(vk_user_id: int, users, title: str):
    if not users:
        send_vk_message(vk_user_id, "Пользователи не найдены.", vk_admin_menu_keyboard())
        return

    lines = [title]
    for index, (_, first_name, last_name, nick) in enumerate(users, start=1):
        lines.append(f"{index}. {first_name} {last_name} ({nick})")
    lines.append("")
    lines.append("Отправь номера пользователей через запятую, например: 1,3,5")
    send_vk_message(vk_user_id, "\n".join(lines), vk_back_keyboard())


async def handle_vk_registration(internal_user_id: int, game_id: int):
    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s AND is_deleted = FALSE", (game_id,), fetchone=True)
    if not game:
        return "Игра не найдена."

    game_name, game_date = game
    user_age_row = execute_query("SELECT age FROM users WHERE user_id = %s", (internal_user_id,), fetchone=True)
    user_age = user_age_row[0] if user_age_row else None
    age_rejection = get_registration_age_rejection(game_name, user_age)
    if age_rejection:
        return age_rejection

    if await is_game_full(game_id, game_name, internal_user_id):
        return (
            "К сожалению, на данную игру записалось максимальное количество участников😢\n"
            "Попробуй выбрать другую игру."
        )

    execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (internal_user_id, game_id))
    execute_query(
        """
        INSERT INTO registrations (user_id, game_id, status, is_late)
        VALUES (%s, %s, 'registered', FALSE)
        ON CONFLICT (user_id, game_id)
        DO UPDATE SET status = 'registered', is_late = FALSE
        """,
        (internal_user_id, game_id)
    )

    user_row = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
    if user_row:
        await notify_admin(f"Новая запись: {user_row[0]} {user_row[1]} ({user_row[2]}) на {game_date} {game_name}")

    return build_registration_success_text(game_date, game_name)


async def handle_vk_mark_late(internal_user_id: int, game_id: int):
    reg = execute_query(
        "SELECT 1 FROM registrations WHERE user_id=%s AND game_id=%s AND status='registered'",
        (internal_user_id, game_id),
        fetchone=True
    )
    if not reg:
        return "Сначала нужно записаться на игру."

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
    if not game:
        return "Игра не найдена."

    await mark_late(internal_user_id, game_id)
    user_row = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
    if user_row:
        await notify_admin(f"⏰ Опоздает: {user_row[0]} {user_row[1]} ({user_row[2]}) на {game[1]} {game[0]}")
    return "Отметили, что ты опоздаешь ⏰"


async def handle_vk_cancel_registration(internal_user_id: int, game_id: int):
    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s AND is_deleted = FALSE", (game_id,), fetchone=True)
    if not game:
        return "Игра не найдена."

    execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (internal_user_id, game_id))
    await unmark_late(internal_user_id, game_id)
    execute_query("DELETE FROM registrations WHERE user_id=%s AND game_id=%s", (internal_user_id, game_id))

    user_row = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
    if user_row:
        await notify_admin(f"❌ Отмена записи: {user_row[0]} {user_row[1]} ({user_row[2]}) на {game[1]} {game[0]}")
    return "Запись отменена. Будем ждать тебя на следующих играх."


def handle_vk_profile_step(internal_user_id: int, vk_user_id: int, text: str):
    state = get_vk_state(internal_user_id)
    current = state.get("state")

    if current == "vk_edit_profile_nick":
        set_vk_state(internal_user_id, "vk_edit_profile_age", mafia_nick=text.strip())
        send_vk_message(vk_user_id, "Отлично! Теперь укажи свой возраст цифрами.")
        return True

    if current == "vk_edit_profile_age":
        try:
            age = int(text.strip())
        except ValueError:
            send_vk_message(vk_user_id, "Пожалуйста, введи возраст цифрами.")
            return True

        vk_profile = fetch_vk_user_profile(vk_user_id)
        first_name = vk_profile.get("first_name") or "Имя"
        last_name = vk_profile.get("last_name") or "Фамилия"
        vk_username = vk_profile.get("screen_name")

        upsert_user(
            platform=PLATFORM_VK,
            platform_user_id=vk_user_id,
            first_name=first_name,
            last_name=last_name,
            mafia_nick=state.get("mafia_nick"),
            age=age,
            vk_username=vk_username,
        )

        messages = []
        if age < 18:
            messages.append(get_age_restriction_notice())
        messages.append("Профиль обновлен.")
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, "\n\n".join(messages), vk_main_menu_keyboard(internal_user_id))
        return True

    if current == "awaiting_nick":
        set_vk_state(internal_user_id, "awaiting_age", mafia_nick=text.strip())
        send_vk_message(vk_user_id, "Спасибо! И последнее: сколько тебе лет?")
        return True

    if current == "awaiting_age":
        try:
            age = int(text.strip())
        except ValueError:
            send_vk_message(vk_user_id, "Пожалуйста, введи возраст цифрами.")
            return True

        vk_profile = fetch_vk_user_profile(vk_user_id)
        first_name = vk_profile.get("first_name") or "Имя"
        last_name = vk_profile.get("last_name") or "Фамилия"
        vk_username = vk_profile.get("screen_name")

        upsert_user(
            platform=PLATFORM_VK,
            platform_user_id=vk_user_id,
            first_name=first_name,
            last_name=last_name,
            mafia_nick=state.get("mafia_nick"),
            age=age,
            vk_username=vk_username,
        )
        clear_vk_state(internal_user_id)
        message_parts = []
        if age < 18:
            message_parts.append(get_age_restriction_notice())
        message_parts.append("Спасибо за знакомство! Теперь ты можешь записываться на игры и смотреть общие списки участников.")
        send_vk_message(
            vk_user_id,
            "\n\n".join(message_parts),
            vk_main_menu_keyboard(internal_user_id),
        )
        return True

    return False


async def handle_vk_admin_flow(internal_user_id: int, vk_user_id: int, text: str, payload: dict = None):
    state = get_vk_state(internal_user_id)
    current = state.get("state")
    normalized_text = text.strip()
    payload = payload or {}
    command = payload.get("command")
    audience = payload.get("audience")

    if command == "admin_add_game":
        today = datetime.date.today()
        set_vk_state(internal_user_id, "admin_add_date", calendar_year=today.year, calendar_month=today.month)
        send_vk_message(
            vk_user_id,
            f"Выбери дату игры: {today.month:02d}.{today.year}",
            vk_calendar_keyboard(today.year, today.month)
        )
        return True

    if normalized_text == "🏠 Главное меню" or command == "main_menu":
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, "Возвращаюсь в главное меню.", vk_main_menu_keyboard(internal_user_id))
        return True

    if normalized_text == "🔙 Назад" or command == "back":
        if current == "admin_add_type":
            selected_year = state.get("calendar_year", datetime.date.today().year)
            selected_month = state.get("calendar_month", datetime.date.today().month)
            set_vk_state(internal_user_id, "admin_add_date", calendar_year=selected_year, calendar_month=selected_month)
            send_vk_message(
                vk_user_id,
                f"Выбери дату игры: {selected_month:02d}.{selected_year}",
                vk_calendar_keyboard(selected_year, selected_month)
            )
        elif current == "admin_reminder_audience":
            games = fetch_upcoming_games()
            send_vk_games_list(vk_user_id, games, "admin_reminder_game", "Для какой игры отправить напоминание?", use_game_buttons=True)
        elif current == "admin_reminder_custom_users":
            set_vk_state(internal_user_id, "admin_reminder_audience", reminder_game_id=state.get("reminder_game_id"))
            send_vk_message(vk_user_id, "Кому отправить напоминание?", vk_audience_keyboard())
        elif current in {"admin_broadcast_game", "admin_broadcast_custom_users", "admin_broadcast_message"}:
            set_vk_state(internal_user_id, "admin_broadcast_audience")
            send_vk_message(vk_user_id, "Кому отправить сообщение?", vk_audience_keyboard())
        else:
            clear_vk_state(internal_user_id)
            send_vk_message(vk_user_id, "Возвращаюсь в админ-меню.", vk_admin_menu_keyboard())
        return True

    if current == "admin_add_date":
        selected_year = state.get("calendar_year", datetime.date.today().year)
        selected_month = state.get("calendar_month", datetime.date.today().month)

        if normalized_text in {"◀️ Месяц", "▶️ Месяц"} or isinstance(payload.get("calendar_shift"), int):
            delta = payload.get("calendar_shift")
            if not isinstance(delta, int):
                delta = -1 if normalized_text == "◀️ Месяц" else 1
            new_year, new_month = shift_year_month(selected_year, selected_month, delta)
            set_vk_state(internal_user_id, "admin_add_date", calendar_year=new_year, calendar_month=new_month)
            send_vk_message(
                vk_user_id,
                f"Выбери дату игры: {new_month:02d}.{new_year}",
                vk_calendar_keyboard(new_year, new_month)
            )
            return True

        if isinstance(payload.get("calendar_day"), int) or normalized_text.isdigit():
            day = payload["calendar_day"] if isinstance(payload.get("calendar_day"), int) else int(normalized_text)
            try:
                parsed = datetime.date(selected_year, selected_month, day)
            except ValueError:
                send_vk_message(
                    vk_user_id,
                    "Такой даты нет в текущем месяце. Выбери день кнопкой ниже.",
                    vk_calendar_keyboard(selected_year, selected_month)
                )
                return True
        else:
            parsed = parse_game_date(normalized_text)

        if not parsed:
            send_vk_message(
                vk_user_id,
                "Не удалось распознать дату. Выбери день кнопкой ниже или введи дату в формате ДД.ММ.ГГГГ.",
                vk_calendar_keyboard(selected_year, selected_month)
            )
            return True
        formatted_date = f"{['Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'][parsed.weekday()]} {parsed.strftime('%d.%m')}"
        set_vk_state(
            internal_user_id,
            "admin_add_type",
            game_date=formatted_date,
            calendar_year=selected_year,
            calendar_month=selected_month
        )
        send_vk_message(
            vk_user_id,
            "Выбери тип игры кнопкой ниже.",
            vk_game_type_keyboard()
        )
        return True

    if current == "admin_add_type":
        game_types = {
            "1": "🏙️Городская мафия",
            "2": "🌃Спортивная мафия",
            "3": "🏆Рейтинговая игра",
            "🏙️городская мафия": "🏙️Городская мафия",
            "🌃спортивная мафия": "🌃Спортивная мафия",
            "🏆рейтинговая игра": "🏆Рейтинговая игра",
        }
        selected_type = payload.get("game_type") or game_types.get(text.strip().lower())
        if not selected_type:
            send_vk_message(vk_user_id, "Пожалуйста, выбери тип игры кнопкой ниже.", vk_game_type_keyboard())
            return True
        game_date = state.get("game_date")
        execute_query("INSERT INTO games (game_date, game_name) VALUES (%s, %s)", (game_date, selected_type))
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, f"Игра '{game_date} {selected_type}' успешно добавлена.", vk_admin_menu_keyboard())
        return True

    if current == "admin_reminder_game":
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return True
        game_id, game_name, game_date = selected_game
        set_vk_state(
            internal_user_id,
            "admin_reminder_audience",
            reminder_game_id=game_id,
            reminder_game_text=f"{game_date} {game_name}"
        )
        send_vk_message(vk_user_id, "Кому отправить напоминание?", vk_audience_keyboard())
        return True

    if current == "admin_reminder_audience":
        game_id = state.get("reminder_game_id")
        if normalized_text == "👥 Всем пользователям" or audience == "all":
            rows = execute_query("SELECT user_id FROM users", fetch=True)
            target_users = [r[0] for r in rows]
        elif normalized_text == "✅ Только записавшимся" or audience == "registered":
            rows = execute_query("SELECT user_id FROM registrations WHERE game_id = %s", (game_id,), fetch=True)
            target_users = [r[0] for r in rows]
        elif normalized_text == "❌ Только не записавшимся" or audience == "not_registered":
            rows = execute_query("SELECT user_id FROM users WHERE user_id NOT IN (SELECT user_id FROM registrations WHERE game_id = %s)", (game_id,), fetch=True)
            target_users = [r[0] for r in rows]
        elif normalized_text == "👤 Выбрать пользователей" or audience == "custom":
            users = execute_query("SELECT user_id, first_name, last_name, mafia_nick FROM users", fetch=True)
            set_vk_state(
                internal_user_id,
                "admin_reminder_custom_users",
                reminder_game_id=game_id,
                selectable_users=users,
                selected_user_ids=[],
                selected_user_page=0
            )
            send_vk_message(
                vk_user_id,
                "Выбери пользователей для напоминания:",
                vk_reminder_user_selection_keyboard(users, [], page=0)
            )
            return True
        else:
            send_vk_message(vk_user_id, "Выбери аудиторию кнопкой ниже.", vk_audience_keyboard())
            return True

        count = await send_game_reminders(target_users, game_id)
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, f"Напоминания отправлены {count} пользователям.", vk_admin_menu_keyboard())
        return True

    if current == "admin_reminder_custom_users":
        users = state.get("selectable_users", [])
        selected = set(state.get("selected_user_ids", []))
        current_page = state.get("selected_user_page", 0)

        if command == "rem_sel_page":
            requested_page = payload.get("page")
            if not isinstance(requested_page, int):
                send_vk_message(vk_user_id, "Не удалось переключить страницу. Попробуй ещё раз.")
                return True
            set_vk_state(
                internal_user_id,
                "admin_reminder_custom_users",
                reminder_game_id=state.get("reminder_game_id"),
                selectable_users=users,
                selected_user_ids=list(selected),
                selected_user_page=requested_page
            )
            send_vk_message(
                vk_user_id,
                "Выбери пользователей для напоминания:",
                vk_reminder_user_selection_keyboard(users, selected, page=requested_page)
            )
            return True

        if command == "rem_sel_toggle":
            user_id = payload.get("user_id")
            page = payload.get("page", current_page)
            if not isinstance(user_id, int):
                send_vk_message(vk_user_id, "Не удалось определить пользователя. Попробуй ещё раз.")
                return True
            if user_id in selected:
                selected.remove(user_id)
            else:
                selected.add(user_id)
            set_vk_state(
                internal_user_id,
                "admin_reminder_custom_users",
                reminder_game_id=state.get("reminder_game_id"),
                selectable_users=users,
                selected_user_ids=list(selected),
                selected_user_page=page
            )
            send_vk_message(
                vk_user_id,
                "Выбери пользователей для напоминания:",
                vk_reminder_user_selection_keyboard(users, selected, page=page)
            )
            return True

        if command != "rem_sel_done":
            send_vk_message(
                vk_user_id,
                "Выбирай пользователей кнопками ниже.",
                vk_reminder_user_selection_keyboard(users, selected, page=current_page)
            )
            return True

        if not selected:
            send_vk_message(vk_user_id, "Никто не выбран. Выбери хотя бы одного пользователя.")
            return True

        count = await send_game_reminders(list(selected), state.get("reminder_game_id"))
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, f"Напоминания отправлены {count} выбранным пользователям.", vk_admin_menu_keyboard())
        return True

    if current == "admin_broadcast_audience":
        if normalized_text == "👥 Всем пользователям" or audience == "all":
            rows = execute_query("SELECT user_id FROM users", fetch=True)
            target_users = [r[0] for r in rows]
            set_vk_state(internal_user_id, "admin_broadcast_message", broadcast_target_users=target_users)
            send_vk_message(vk_user_id, "Введи сообщение для рассылки.", vk_back_keyboard())
            return True
        if normalized_text in {"✅ Только записавшимся", "❌ Только не записавшимся"} or audience in {"registered", "not_registered"}:
            filter_type = normalized_text
            if audience == "registered":
                filter_type = "✅ Только записавшимся"
            elif audience == "not_registered":
                filter_type = "❌ Только не записавшимся"
            set_vk_state(internal_user_id, "admin_broadcast_game", broadcast_filter_type=filter_type)
            games = fetch_upcoming_games()
            send_vk_games_list(vk_user_id, games, "admin_broadcast_game", "Для какой игры отфильтровать аудиторию?", use_game_buttons=True)
            return True
        if normalized_text == "👤 Выбрать пользователей" or audience == "custom":
            users = execute_query("SELECT user_id, first_name, last_name, mafia_nick FROM users", fetch=True)
            set_vk_state(internal_user_id, "admin_broadcast_custom_users", selectable_users=users)
            send_vk_user_selection_list(vk_user_id, users, "Выбери пользователей для рассылки:")
            return True
        send_vk_message(vk_user_id, "Выбери аудиторию кнопкой ниже.", vk_audience_keyboard())
        return True

    if current == "admin_broadcast_custom_users":
        users = state.get("selectable_users", [])
        try:
            indexes = [int(item.strip()) - 1 for item in normalized_text.split(",") if item.strip()]
        except ValueError:
            send_vk_message(vk_user_id, "Не удалось распознать номера. Отправь их через запятую, например: 1,3,5", vk_back_keyboard())
            return True
        if not indexes or any(index < 0 or index >= len(users) for index in indexes):
            send_vk_message(vk_user_id, "Проверь номера пользователей и попробуй еще раз.", vk_back_keyboard())
            return True
        target_users = [users[index][0] for index in indexes]
        set_vk_state(internal_user_id, "admin_broadcast_message", broadcast_target_users=target_users)
        send_vk_message(vk_user_id, "Введи сообщение для рассылки.", vk_back_keyboard())
        return True

    if current == "admin_broadcast_game":
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return True
        game_id = selected_game[0]
        filter_type = state.get("broadcast_filter_type")
        if filter_type == "✅ Только записавшимся":
            rows = execute_query("SELECT user_id FROM registrations WHERE game_id = %s AND status = 'registered'", (game_id,), fetch=True)
            target_users = [r[0] for r in rows]
        else:
            rows = execute_query(
                "SELECT user_id FROM users WHERE user_id NOT IN (SELECT user_id FROM registrations WHERE game_id = %s AND status = 'registered')",
                (game_id,),
                fetch=True
            )
            target_users = [r[0] for r in rows]
        set_vk_state(internal_user_id, "admin_broadcast_message", broadcast_target_users=target_users)
        send_vk_message(vk_user_id, "Введи сообщение для рассылки.", vk_back_keyboard())
        return True

    if current == "admin_broadcast_message":
        users = state.get("broadcast_target_users", [])
        count = 0
        for user_id in users:
            try:
                await send_text_to_user(user_id, normalized_text)
                count += 1
            except Exception as e:
                logging.error(f"Error sending VK broadcast to {user_id}: {e}")
        clear_vk_state(internal_user_id)
        send_vk_message(vk_user_id, f"Сообщение отправлено {count} пользователям.", vk_admin_menu_keyboard())
        return True

    if current in {"admin_delete_game", "admin_restore_game", "admin_cancel_game", "admin_view_participants"}:
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return True
        game_id, game_name, game_date = selected_game[:3]
        if current == "admin_delete_game":
            execute_query("UPDATE games SET is_deleted = TRUE WHERE game_id = %s", (game_id,))
            clear_vk_state(internal_user_id)
            send_vk_message(vk_user_id, f"Игра '{game_date} {game_name}' удалена.", vk_admin_menu_keyboard())
            return True
        if current == "admin_restore_game":
            execute_query("UPDATE games SET is_deleted = FALSE WHERE game_id = %s", (game_id,))
            clear_vk_state(internal_user_id)
            send_vk_message(vk_user_id, f"Игра '{game_date} {game_name}' восстановлена.", vk_admin_menu_keyboard())
            return True
        if current == "admin_cancel_game":
            participants = execute_query("SELECT user_id FROM registrations WHERE game_id = %s", (game_id,), fetch=True)
            for (participant_id,) in participants:
                await send_text_to_user(participant_id, f"⚠️ Внимание! Отмена игры на {game_date} {game_name}! ⚠️")
            execute_query("DELETE FROM registrations WHERE game_id = %s", (game_id,))
            execute_query("DELETE FROM games WHERE game_id = %s", (game_id,))
            clear_vk_state(internal_user_id)
            send_vk_message(vk_user_id, f"Игра '{game_date} {game_name}' отменена.", vk_admin_menu_keyboard())
            return True
        send_vk_message(
            vk_user_id,
            await format_admin_participants_async(game_id, build_game_title(game_name, game_date)),
            vk_admin_menu_keyboard()
        )
        clear_vk_state(internal_user_id)
        return True

    return False


async def handle_vk_message(vk_user_id: int, text: str, payload_raw=None):
    normalized_text = (text or "").strip()
    payload = parse_vk_payload(payload_raw)
    command = payload.get("command")
    internal_user_id = make_internal_user_id(PLATFORM_VK, vk_user_id)
    user_exists = execute_query("SELECT 1 FROM users WHERE user_id = %s", (internal_user_id,), fetchone=True)

    if normalized_text.lower() in {"start", "начать", "/start"}:
        if user_exists:
            send_vk_message(vk_user_id, "С возвращением! Можешь пользоваться меню.", vk_main_menu_keyboard(internal_user_id))
            clear_vk_state(internal_user_id)
        else:
            set_vk_state(internal_user_id, "awaiting_nick")
            send_vk_message(vk_user_id, "Привет! Какой у тебя игровой ник в мафии?")
        return

    if not user_exists:
        if handle_vk_profile_step(internal_user_id, vk_user_id, normalized_text):
            return
        set_vk_state(internal_user_id, "awaiting_nick")
        send_vk_message(vk_user_id, "Для начала знакомства напиши, пожалуйста, свой игровой ник.")
        return

    if handle_vk_profile_step(internal_user_id, vk_user_id, normalized_text):
        return

    if await handle_vk_admin_flow(internal_user_id, vk_user_id, normalized_text, payload):
        return

    state = get_vk_state(internal_user_id)
    current = state.get("state")
    if normalized_text == "🔙 Назад" or command == "back":
        prompt_vk_main_menu(vk_user_id)
        return

    if normalized_text == "🏠 Главное меню" or command == "main_menu":
        prompt_vk_main_menu(vk_user_id)
        return

    if current == "vk_register_select":
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return
        response = await handle_vk_registration(internal_user_id, selected_game[0])
        keyboard = vk_late_button_keyboard(selected_game[0]) if response.startswith("Ты успешно записался на игру") else vk_main_menu_keyboard(internal_user_id)
        send_vk_message(vk_user_id, response, keyboard)
        clear_vk_state(internal_user_id)
        return

    if current == "vk_cancel_select":
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return
        response = await handle_vk_cancel_registration(internal_user_id, selected_game[0])
        send_vk_message(vk_user_id, response, vk_main_menu_keyboard(internal_user_id))
        clear_vk_state(internal_user_id)
        return

    if current == "vk_participants_select":
        selected_game = get_vk_selected_game(state, normalized_text, payload)
        if not selected_game:
            send_vk_message(vk_user_id, "Пожалуйста, выбери игру кнопкой ниже.")
            return
        game_id, game_name, game_date = selected_game
        send_vk_message(
            vk_user_id,
            await format_user_participants_async(game_id, build_game_title(game_name, game_date)),
            vk_main_menu_keyboard(internal_user_id)
        )
        clear_vk_state(internal_user_id)
        return

    if normalized_text == "📝Записаться на игру" or command == "register":
        send_vk_games_list(vk_user_id, fetch_upcoming_games(), "vk_register_select", "Выбери игру для записи:", use_game_buttons=True)
        return

    if normalized_text == "❌Отменить запись" or command == "cancel_registration":
        games = execute_query(
            """
            SELECT g.game_id, g.game_name, g.game_date
            FROM registrations r
            JOIN games g ON r.game_id = g.game_id
            WHERE r.user_id = %s AND g.is_deleted = FALSE AND r.status = 'registered'
            """,
            (internal_user_id,),
            fetch=True
        )
        games = sort_games_by_date(filter_upcoming_games(games))
        send_vk_games_list(vk_user_id, games, "vk_cancel_select", "Выбери игру, запись на которую хочешь отменить:", use_game_buttons=True)
        return

    if normalized_text == "📝 Обновить профиль" or command == "edit_profile":
        set_vk_state(internal_user_id, "vk_edit_profile_nick")
        send_vk_message(vk_user_id, "Давай обновим профиль. Какой у тебя сейчас игровой ник в мафии?", vk_back_keyboard())
        return

    if normalized_text == "📅Расписание игр" or command == "schedule":
        games = fetch_upcoming_games()
        if not games:
            send_vk_message(vk_user_id, "Игр пока не запланировано.", vk_main_menu_keyboard(internal_user_id))
            return
        lines = ["Расписание ближайших игр:\n"]
        for _, game_name, game_date in games:
            lines.append(f"📆{game_date} {game_name}")
            lines.append(get_game_rules(game_name).strip())
        send_vk_message(vk_user_id, "\n".join(line for line in lines if line), vk_main_menu_keyboard(internal_user_id))
        return

    if normalized_text == "👥Список участников" or command == "participants":
        send_vk_games_list(vk_user_id, fetch_upcoming_games(), "vk_participants_select", "Выбери игру, список участников которой хочешь посмотреть:", use_game_buttons=True)
        return

    if normalized_text == "⏰ Опоздаю" or command == "mark_late":
        game_id = payload.get("game_id")
        if not isinstance(game_id, int):
            send_vk_message(vk_user_id, "Не удалось определить игру. Попробуй отметить опоздание заново.", vk_main_menu_keyboard(internal_user_id))
            return
        response = await handle_vk_mark_late(internal_user_id, game_id)
        send_vk_message(vk_user_id, response, vk_main_menu_keyboard(internal_user_id))
        return

    if command in {"reminder_register", "reminder_cancel", "reminder_late", "reminder_think", "reminder_decline"}:
        game_id = payload.get("game_id")
        if not isinstance(game_id, int):
            send_vk_message(vk_user_id, "Не удалось определить игру. Попробуй ещё раз.", vk_main_menu_keyboard(internal_user_id))
            return

        if command == "reminder_register":
            response = await handle_vk_registration(internal_user_id, game_id)
            keyboard = vk_late_button_keyboard(game_id) if response.startswith("Ты успешно записался на игру") else vk_main_menu_keyboard(internal_user_id)
            send_vk_message(vk_user_id, response, keyboard)
            return

        if command == "reminder_cancel":
            response = await handle_vk_cancel_registration(internal_user_id, game_id)
            send_vk_message(vk_user_id, response, vk_main_menu_keyboard(internal_user_id))
            return

        if command == "reminder_late":
            response = await handle_vk_mark_late(internal_user_id, game_id)
            send_vk_message(vk_user_id, response, vk_main_menu_keyboard(internal_user_id))
            return

        if command == "reminder_think":
            await mark_thinking(internal_user_id, game_id)
            game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
            user_row = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
            if game and user_row:
                await notify_admin(f"🤔 Игрок думает: {user_row[0]} {user_row[1]} ({user_row[2]}) на {game[1]} {game[0]}")
            send_vk_message(vk_user_id, "Админ уведомлен, что ты думаешь 😊", vk_main_menu_keyboard(internal_user_id))
            return

        execute_query(
            """
            INSERT INTO registrations (user_id, game_id, status)
            VALUES (%s, %s, 'declined')
            ON CONFLICT (user_id, game_id)
            DO UPDATE SET status = 'declined'
            """,
            (internal_user_id, game_id)
        )
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (internal_user_id, game_id))
        await unmark_late(internal_user_id, game_id)
        game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
        user_row = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (internal_user_id,), fetchone=True)
        if game and user_row:
            await notify_admin(f"❌ Отказ: {user_row[0]} {user_row[1]} ({user_row[2]}) на {game[1]} {game[0]}")
        send_vk_message(vk_user_id, "Отметили, что ты не придёшь.", vk_main_menu_keyboard(internal_user_id))
        return

    if normalized_text == "📍Как до нас добраться?" or command == "location":
        send_vk_message(
            vk_user_id,
            "Мы находимся по адресу:\nг. Королев, ул. Декабристов, д. 8\nВход со стороны дороги, стеклянная дверь с надписью «Тайная комната».",
            vk_main_menu_keyboard(internal_user_id)
        )
        return

    if (normalized_text == "⚙️ Админ-панель" or command == "admin_panel") and vk_user_id == VK_ADMIN_ID:
        send_vk_message(vk_user_id, "Добро пожаловать в админ-панель.", vk_admin_menu_keyboard())
        clear_vk_state(internal_user_id)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "➕ Добавить игру" or command == "admin_add_game"):
        today = datetime.date.today()
        set_vk_state(internal_user_id, "admin_add_date", calendar_year=today.year, calendar_month=today.month)
        send_vk_message(
            vk_user_id,
            f"Выбери дату игры: {today.month:02d}.{today.year}",
            vk_calendar_keyboard(today.year, today.month)
        )
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "❌ Удалить игру" or command == "admin_delete_game"):
        games = fetch_active_games()
        send_vk_games_list(vk_user_id, games, "admin_delete_game", "Какую игру удалить?", use_game_buttons=True)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "♻️ Восстановить игру" or command == "admin_restore_game"):
        games = fetch_active_games(include_deleted=True)
        deleted_games = [game[:3] for game in games if len(game) > 3 and game[3]]
        send_vk_games_list(vk_user_id, deleted_games, "admin_restore_game", "Какую игру восстановить?", use_game_buttons=True)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "🚫 Отмена игры" or command == "admin_cancel_game"):
        games = fetch_active_games()
        send_vk_games_list(vk_user_id, games, "admin_cancel_game", "Какую игру отменить?", use_game_buttons=True)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "👥 Список участников" or command == "admin_view_participants"):
        games = fetch_active_games()
        send_vk_games_list(vk_user_id, games, "admin_view_participants", "Для какой игры показать список участников?", use_game_buttons=True)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "🔔 Напомнить об игре" or command == "admin_reminder"):
        games = fetch_upcoming_games()
        send_vk_games_list(vk_user_id, games, "admin_reminder_game", "Для какой игры отправить напоминание?", use_game_buttons=True)
        return

    if vk_user_id == VK_ADMIN_ID and (normalized_text == "📢 Рассылка" or command == "admin_broadcast"):
        set_vk_state(internal_user_id, "admin_broadcast_audience")
        send_vk_message(vk_user_id, "Кому отправить сообщение?", vk_audience_keyboard())
        return

    send_vk_message(vk_user_id, "Не понял команду. Пожалуйста, используй кнопки меню.", vk_main_menu_keyboard(internal_user_id))


def vk_polling_loop(loop: asyncio.AbstractEventLoop):
    if not VK_TOKEN or not VK_GROUP_ID:
        logging.warning("VK_BOT_TOKEN или VK_GROUP_ID не заданы. VK-бот не будет запущен.")
        return

    if VK_BOT_TOKEN_ENV and VK_TOKEN_ENV and VK_BOT_TOKEN_ENV != VK_TOKEN_ENV:
        logging.warning(
            "Одновременно заданы VK_BOT_TOKEN и VK_TOKEN с разными значениями. "
            "Будет использован VK_BOT_TOKEN=%s, VK_TOKEN=%s",
            mask_secret(VK_BOT_TOKEN_ENV),
            mask_secret(VK_TOKEN_ENV),
        )

    logging.info(
        "Запуск VK long poll с group_id=%s и токеном %s",
        VK_GROUP_ID,
        mask_secret(VK_TOKEN),
    )

    global vk_session, vk_api_client, vk_longpoll
    try:
        vk_session = vk_api.VkApi(token=VK_TOKEN)
        vk_api_client = vk_session.get_api()
        vk_longpoll = VkBotLongPoll(vk_session, int(VK_GROUP_ID))
    except vk_api.exceptions.ApiError as exc:
        logging.error(
            "VK long poll не запущен: %s. Проверь, что VK_BOT_TOKEN — это токен сообщества "
            "с правами на сообщения, а VK_GROUP_ID принадлежит этому сообществу.",
            exc
        )
        return
    except Exception as exc:
        logging.exception("Не удалось инициализировать VK long poll: %s", exc)
        return

    logging.info("VK long poll запущен")

    try:
        for event in vk_longpoll.listen():
            if event.type != VkBotEventType.MESSAGE_NEW:
                continue

            message = event.object.message
            if message.get("from_id", 0) <= 0:
                continue

            text = message.get("text", "")
            payload = message.get("payload")
            future = asyncio.run_coroutine_threadsafe(handle_vk_message(message["from_id"], text, payload), loop)
            try:
                future.result()
            except Exception as exc:
                logging.exception("Ошибка обработки VK-сообщения: %s", exc)
    except Exception as exc:
        logging.exception("VK long poll остановлен из-за ошибки: %s", exc)

async def main():
    vk_thread = None
    try:
        # Удаляем вебхук перед запуском polling, чтобы избежать конфликтов
        await bot.delete_webhook(drop_pending_updates=False)
        if VK_TOKEN and VK_GROUP_ID:
            vk_thread = threading.Thread(target=vk_polling_loop, args=(asyncio.get_running_loop(),), daemon=True)
            vk_thread.start()
        # Оставляем pending updates, чтобы пользователям не приходилось заново нажимать кнопки после деплоя
        await dp.start_polling(bot, skip_updates=False)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")
