import asyncio
import logging
import os
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

# Настройка логирования
logging.basicConfig(level=logging.INFO)

API_TOKEN = (
    os.environ.get("TELEGRAM_BOT_TOKEN")
    or os.environ.get("BOT_TOKEN")
    or os.environ.get("TELEGRAM_TOKEN")
)
ADMIN_ID = 2127578673
REDIS_URL = os.environ.get("REDIS_URL")

if not API_TOKEN:
    raise ValueError("❌ Не задан токен бота. Укажи TELEGRAM_BOT_TOKEN (или BOT_TOKEN / TELEGRAM_TOKEN)")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
redis = None


async def _check_redis_connection(redis_client: Redis) -> bool:
    try:
        await asyncio.wait_for(redis_client.ping(), timeout=0.7)
        return True
    except Exception as e:
        logging.warning(f"Redis недоступен, переключаемся на MemoryStorage: {e}")
        return False


if REDIS_URL:
    candidate_redis = None
    try:
        candidate_redis = Redis.from_url(REDIS_URL, socket_connect_timeout=1, socket_timeout=1)
        redis_ok = asyncio.run(_check_redis_connection(candidate_redis))
    except Exception as e:
        logging.warning(f"Не удалось создать/проверить Redis, переключаемся на MemoryStorage: {e}")
        redis_ok = False

    if redis_ok and candidate_redis is not None:
        redis = candidate_redis
        storage = RedisStorage(redis=redis)
        logging.info("Используется RedisStorage")
    else:
        if candidate_redis is not None:
            try:
                asyncio.run(candidate_redis.aclose())
            except Exception:
                pass
        storage = MemoryStorage()
        logging.warning("Используется MemoryStorage (FSM-состояние не сохраняется между перезапусками)")
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

# Состояния FSM
class Form(StatesGroup):
    start = State()
    get_name = State()
    get_lastname = State()
    get_nick = State()
    get_age = State()
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
    sport_rules = "17:00 – сбор и объяснение правил\n17:30 – школа мафии\n18:00 – начало игр\n\n"
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

async def wake_up_all_users():
    users = execute_query("SELECT user_id FROM users", fetch=True)
    if not users:
        logging.info("Нет пользователей для wake-up уведомления")
        return

    sent = 0
    for (user_id,) in users:
        try:
            await bot.send_message(
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
    user = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id = %s", (message.from_user.id,), fetchone=True)

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

    execute_query("""
        INSERT INTO users (user_id, first_name, last_name, mafia_nick, age, telegram_username)
        VALUES (%s, %s, %s, %s, %s, %s)
        ON CONFLICT(user_id) DO UPDATE SET
        first_name=EXCLUDED.first_name,
        last_name=EXCLUDED.last_name,
        mafia_nick=EXCLUDED.mafia_nick,
        age=EXCLUDED.age,
        telegram_username=EXCLUDED.telegram_username
    """, (message.from_user.id, data['first_name'], data['last_name'], data['mafia_nick'], age, message.from_user.username))

    if age < 18:
        await message.answer(
            "В Тайной комнате действуют возрастные ограничения для игры в мафию:\n"
            "• 18+ для Спортивной мафии\n"
            "• 16+ для Городской мафии"
        )

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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = TRUE", fetch=True)
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games", fetch=True)
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
        games = filter_upcoming_games(games)
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
        SELECT u.user_id, u.first_name, u.last_name, u.mafia_nick, u.telegram_username
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

    for i, (user_id, fn, ln, nick, tg_username) in enumerate(ordered_participants, 1):
        username_text = f"@{tg_username}" if tg_username else "username не указан"
        mark = " (думает)" if user_id in thinking_users else ""
        late_mark = " (опоздает)" if user_id in late_users else ""
        response += f"{i}. {fn} {ln} ({nick}, {username_text}){mark}{late_mark}\n"

    # Добавляем думающих, которых нет среди зарегистрированных
    for uid in thinking_users:
        if not any(uid == user_id for user_id, *_ in participants):
            ud = execute_query("SELECT first_name, last_name, mafia_nick, telegram_username FROM users WHERE user_id=%s", (uid,), fetchone=True)
            if ud:
                username_text = f"@{ud[3]}" if ud[3] else "username не указан"
                response += f"- {ud[0]} {ud[1]} ({ud[2]}, {username_text}) (думает)\n"

    await message.answer(response, reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.menu)
async def menu_handler(message: types.Message, state: FSMContext):
    if message.text == "📝Записаться на игру":
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
        games = filter_upcoming_games(games)
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
        games = execute_query("""
            SELECT g.game_id, g.game_name, g.game_date 
            FROM registrations r
            JOIN games g ON r.game_id=g.game_id
            WHERE r.user_id=%s
        """, (message.from_user.id,), fetch=True)
        games = filter_upcoming_games(games)
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
        games = execute_query("SELECT game_name, game_date FROM games WHERE is_deleted = FALSE ORDER BY game_id ASC", fetch=True)
        games = [g for g in games if is_upcoming_game(g[1])]
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
        games = filter_upcoming_games(games)
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
    user_id = callback.from_user.id

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
        await bot.send_message(ADMIN_ID, f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]}) на {game_date} {game_name}")

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
        if await is_game_full(game_id, game_name, message.from_user.id):
            await message.answer(
                "К сожалению, на данную игру записалось максимальное количество участников😢\n"
                "Попробуй записаться на другую игру или напиши Нате @natabordo, возможно она сможет что-то придумать☺️",
                reply_markup=main_menu_keyboard(message.from_user.id)
            )
            await state.set_state(Form.menu)
            return

        # Удаляем из списка думающих при регистрации
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (message.from_user.id, game_id))
        execute_query("""
            INSERT INTO registrations (user_id, game_id, status, is_late)
            VALUES (%s, %s, 'registered', FALSE)
            ON CONFLICT (user_id, game_id)
            DO UPDATE SET status = 'registered', is_late = FALSE
        """, (message.from_user.id, game_id))
        rules = get_game_rules(game_name)
        cost = get_game_cost(game_name)
        await message.answer(f"<b>Ты успешно записался на игру {game_date} {game_name}!</b>\n"
                             f"{rules}"
                             f"{cost}"
                             "Оплачиваете после игры\n\n"
                             "🎁 Если ты первый раз в Тайной Комнате - тебе скидка 200 руб.\n"
                             "🎁 Если вы пришли вдвоем - 1000 руб. за двоих (одним платежом)\n"
                             "❗️Скидки и акции не суммируются\n\n"
                             "P.S. На улице снег, поэтому возьмите, пожалуйста, с собой сменку или пользуйтесь тапочками ТК🙏\n\n"
                             "❗️Игра не состоится, если придут меньше 10 человек❗️\n"
                             "Поэтому, пожалуйста, не пропускай игру или отмени запись, если планы изменятся\n\n"
                             "Предупреди, если опоздаешь", 
                             reply_markup=late_button_keyboard(game_id),
                             parse_mode="HTML"
                            )
        ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (message.from_user.id,), fetchone=True)
        if ud:
            await bot.send_message(ADMIN_ID, f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    else:
        await message.answer("Не удалось найти выбранную игру. Попробуй выбрать её из списка ещё раз.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.message(Form.game_cancellation)
async def cancel_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
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
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (message.from_user.id, game_id))
        await unmark_late(message.from_user.id, game_id)
        execute_query("DELETE FROM registrations WHERE user_id=%s AND game_id=%s", (message.from_user.id, game_id))
        await message.answer("Запись отменена.\n"
                             "Спасибо за то, что уважаешь клуб и других игроков!☺️\n"
                             "Будем ждать тебя на следующих играх.",
                             reply_markup=main_menu_keyboard(message.from_user.id),
                             parse_mode="HTML"
                            )
        ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (message.from_user.id,), fetchone=True)
        if ud:
            await bot.send_message(ADMIN_ID, f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    else:
        await message.answer("Не удалось найти выбранную игру. Попробуй выбрать её из списка ещё раз.", reply_markup=main_menu_keyboard(message.from_user.id))
    await state.set_state(Form.menu)

@dp.callback_query(F.data.startswith("think_"))
async def callback_think(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

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
        await bot.send_message(ADMIN_ID, f"🤔 Игрок думает: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("reg_"))
async def callback_reg(callback: types.CallbackQuery, state: FSMContext):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    game = execute_query(
        "SELECT game_name, game_date FROM games WHERE game_id = %s",
        (game_id,),
        fetchone=True
    )

    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    game_name, game_date = game

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

    rules = get_game_rules(game_name)
    cost = get_game_cost(game_name)
    
    await callback.message.answer(
        f"<b>Ты успешно записался на игру {game_date} {game_name}!</b>\n"
        f"{rules}"
        f"{cost}"
        "Оплачиваете после игры\n\n"
        "🎁 Если ты первый раз в Тайной Комнате - тебе скидка 200 руб.\n"
        "🎁 Если вы пришли вдвоем - 1000 руб. за двоих (одним платежом)\n"
        "❗️Скидки и акции не суммируются\n\n"
        "P.S. На улице снег, поэтому возьмите, пожалуйста, с собой сменку или пользуйтесь тапочками ТК🙏\n\n"
        "❗️Игра не состоится, если придут меньше 10 человек❗️\n\n"
        "Предупреди, если опоздаешь",
        parse_mode="HTML",
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
        await bot.send_message(
            ADMIN_ID,
            f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {game_date} {game_name}"
        )

@dp.callback_query(F.data.startswith("late_"))
async def callback_late(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

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
        await bot.send_message(
            ADMIN_ID,
            f"⏰ Опоздает: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}"
        )

@dp.callback_query(F.data.startswith("decline_"))
async def callback_decline(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

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
        await bot.send_message(
            ADMIN_ID,
            f"❌ Отказ от игры: {ud[0]} {ud[1]} ({ud[2]})"
        )

@dp.callback_query(F.data.startswith("cancelreg_"))
async def callback_cancel_registration(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

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
        await bot.send_message(
            ADMIN_ID,
            f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]})"
        )

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
                await bot.send_message(user_id, f"⚠️ Внимание! Отмена игры на {game_info}! ⚠️")
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games", fetch=True)
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

            await bot.send_message(
                uid,
                f"🔔 Напоминание об игре: {g_date} {g_name}\nБудем вас ждать! 😊",
                reply_markup=builder.as_markup()
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
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
        games = filter_upcoming_games(games)
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
            await bot.send_message(user_id, message.text)
            count += 1
        except Exception as e:
            logging.error(f"Error sending broadcast to {user_id}: {e}")

    await message.answer(f"Сообщение отправлено {count} пользователям.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

async def main():
    try:
        # Удаляем вебхук перед запуском polling, чтобы избежать конфликтов
        await bot.delete_webhook(drop_pending_updates=False)
        # Пробуждаем пользователей после обновления, чтобы не требовалось ручное переоткрытие диалога
        await wake_up_all_users()
        # Оставляем pending updates, чтобы пользователям не приходилось заново нажимать кнопки после деплоя
        await dp.start_polling(bot, skip_updates=False)
    finally:
        await bot.session.close()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except (KeyboardInterrupt, SystemExit):
        logging.info("Бот остановлен")
