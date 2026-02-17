import asyncio
import logging
import os
import database

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram_calendar import SimpleCalendar, SimpleCalendarCallback
import datetime

# Настройка логирования
logging.basicConfig(level=logging.INFO)

API_TOKEN = os.environ.get("TELEGRAM_BOT_TOKEN")
ADMIN_ID = 2127578673

if not API_TOKEN:
    raise ValueError("❌ Не задан TELEGRAM_BOT_TOKEN в переменных окружения")

# Инициализация бота и диспетчера
bot = Bot(token=API_TOKEN)
storage = MemoryStorage()
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

def get_game_rules(game_name):
    sport_rules = "17:00 – сбор и объяснение правил\n17:30 – школа мафии\n18:30 – начало игр\n\n"
    city_rules = "18:00 – сбор и объяснение правил\n18:30 – начало игр\n\n"
    rating_rules = "19:00 – начало игр\n\n"
    
    if "Спортивная мафия" in game_name:
        return sport_rules
    elif "Рейтинговая игра" in game_name:
        return rating_rules
    elif "Городская мафия" in game_name:
        return city_rules
    return "\n"

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
        await message.answer("Хорошо! Давай обновим твою анкету. Как тебя зовут?")
        await state.set_state(Form.get_name)
    elif message.text == "✅ Оставить как есть":
        await message.answer("Отлично! Переходим в главное меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
    else:
        await message.answer("Пожалуйста, воспользуйся кнопками для выбора.")

@dp.message(Form.start)
async def process_start(message: types.Message, state: FSMContext):
    if message.text and message.text.lower() == "да":
        await message.answer("Как тебя зовут?")
        await state.set_state(Form.get_name)
    elif message.text and message.text.lower() == "нет":
        await message.answer("Хорошо, запускай бота снова, когда будешь готов.")
    else:
        await message.answer("Пожалуйста, воспользуйся кнопками для выбора.")

@dp.message(Form.get_name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(first_name=message.text)
    await message.answer("А какая у тебя фамилия?")
    await state.set_state(Form.get_lastname)

@dp.message(Form.get_lastname)
async def process_lastname(message: types.Message, state: FSMContext):
    await state.update_data(last_name=message.text)
    await message.answer(
        "И какой у тебя игровой ник в мафии?\n\n"
        "P.S. В мафии используют ники для того, чтобы разделять игру и реальную жизнь, и не переносить негативные эмоции на личности игроков"
    )
    await state.set_state(Form.get_nick)

@dp.message(Form.get_nick)
async def process_nick(message: types.Message, state: FSMContext):
    await state.update_data(mafia_nick=message.text)
    await message.answer("Сколько тебе лет?")
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
        builder.button(text="🔙 Назад")
        await message.answer("Введите сообщение для рассылки всем пользователям:", reply_markup=builder.as_markup(resize_keyboard=True))
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
    clean_text = message.text.replace("👥", "").strip() if message.text else ""
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
        SELECT u.user_id, u.first_name, u.last_name, u.mafia_nick
        FROM registrations r
        JOIN users u ON r.user_id = u.user_id
        WHERE r.game_id = %s
    """, (game_id,), fetch=True)

    # Получаем думающих через Redis
    thinking_users = await get_thinking(game_id)
    thinking_users = set(map(int, thinking_users))  # строки в int

    if not participants and not thinking_users:
        await message.answer(f"На игру '{message.text}' пока никто не записался.", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return

    # Формируем текст с участниками
    response = f"Список участников на игру {message.text}:\n"

    # Основные участники
    for i, (user_id, fn, ln, nick) in enumerate(participants, 1):
        mark = " (думает)" if user_id in thinking_users else ""
        response += f"{i}. {fn} {ln} ({nick}){mark}\n"

    # Добавляем думающих, которых нет среди зарегистрированных
    for uid in thinking_users:
        if not any(uid == user_id for user_id, _, _, _ in participants):
            ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (uid,), fetchone=True)
            if ud:
                response += f"- {ud[0]} {ud[1]} ({ud[2]}) (думает)\n"

    await message.answer(response, reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.menu)
async def menu_handler(message: types.Message, state: FSMContext):
    if message.text == "📝Записаться на игру":
        games = execute_query("SELECT game_id, game_name, game_date FROM games WHERE is_deleted = FALSE", fetch=True)
        if not games:
            await message.answer("К сожалению, на данный момент игр для записи нет.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}")
        builder.button(text="🔙 В меню")
        builder.adjust(1)
        await message.answer("На какую игру ты хочешь записаться?\n\n"
                             "Время начала игр можно посмотреть в расписании.", 
                             reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.game_registration)
    elif message.text == "❌Отменить запись":
        games = execute_query("""
            SELECT g.game_id, g.game_name, g.game_date 
            FROM registrations r
            JOIN games g ON r.game_id=g.game_id
            WHERE r.user_id=%s
        """, (message.from_user.id,), fetch=True)
        if not games:
            await message.answer("Ты пока не записан ни на какую игру.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}")
        builder.button(text="🔙 В меню")
        builder.adjust(1)
        await message.answer("Запись на какую игру ты хочешь отменить?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.game_cancellation)
    elif message.text == "📅Расписание игр":
        games = execute_query("SELECT game_name, game_date FROM games WHERE is_deleted = FALSE ORDER BY game_id ASC", fetch=True)
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
        if not games:
            await message.answer("К сожалению, на данный момент игр нет.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"👥{date} {display_name}")
        builder.button(text="🔙 В меню")
        builder.adjust(1)
        await message.answer("Список участников какой игры ты хочешь посмотреть?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.user_view_participants)

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
            SELECT u.mafia_nick 
            FROM registrations r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.game_id = %s
        """, (game_id,), fetch=True)

        # Получаем думающих через Redis
        thinking_users = await get_thinking(game_id)
        thinking_users = set(map(int, thinking_users))

        if not participants and not thinking_users:
            await message.answer(f"На игру {message.text} пока никто не записался.", reply_markup=main_menu_keyboard(message.from_user.id))
        else:
            response = f"Список участников на игру {message.text}:\n"
            idx = 1
            for (nick,) in participants:
                response += f"{idx}. {nick}\n"
                idx += 1

            for uid in thinking_users:
                # Проверяем, что не в списке основных
                exists = execute_query("SELECT 1 FROM registrations WHERE user_id=%s AND game_id=%s", (uid, game_id), fetchone=True)
                if not exists:
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
    result = execute_query("SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR game_name || ' ' || game_date = %s", (clean_text, clean_text), fetchone=True)
    if result:
        game_id = result[0]
        # Удаляем из списка думающих при регистрации
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (message.from_user.id, game_id))
        execute_query("INSERT INTO registrations (user_id, game_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (message.from_user.id, game_id))
        
        game_name = message.text.replace("📆", "").strip()
        rules = get_game_rules(game_name)
        
        await message.answer(f"<b>Ты успешно записался на игру {message.text}!</b>\n"
                             f"{rules}"
                             "<b>Мы находимся по адресу</b>\n\n"
                             "г. Королев, ул. Декабристов, д. 8\n"
                             "Вход со стороны дороги (не со двора), ищи стеклянную дверь с надписью «Тайная комната» и спускайся по лестнице в самый низ.\n\n"
                             "❗️Игра не состоится, если придут меньше 10 человек.\n"
                             "Поэтому, пожалуйста, приходи обязательно, если записался или отмени запись, если планы изменятся.🙏", 
                             reply_markup=main_menu_keyboard(message.from_user.id),
                             parse_mode="HTML"
                            )
        ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (message.from_user.id,), fetchone=True)
        if ud:
            await bot.send_message(ADMIN_ID, f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    await state.set_state(Form.menu)

@dp.message(Form.game_cancellation)
async def cancel_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Ты вернулся в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    clean_text = message.text.replace("📆", "").strip() if message.text else ""
    result = execute_query("SELECT game_id FROM games WHERE game_date || ' ' || game_name = %s OR game_name || ' ' || game_date = %s", (clean_text, clean_text), fetchone=True)
    if result:
        game_id = result[0]
        # Удаляем из всех списков
        execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (message.from_user.id, game_id))
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

    # Notify admin
    ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if ud:
        await bot.send_message(ADMIN_ID, f"🤔 Игрок думает: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("reg_"))
async def callback_reg(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id

    game = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)

    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    # Удаляем из списка думающих при регистрации
    execute_query("DELETE FROM thinking_players WHERE user_id = %s AND game_id = %s", (user_id, game_id))
    execute_query("INSERT INTO registrations (user_id, game_id) VALUES (%s, %s) ON CONFLICT DO NOTHING", (user_id, game_id))

    rules = get_game_rules(game[0])

    await callback.message.answer(f"<b>Ты успешно записался на игру {game[1]} {game[0]}!</b>\n"
                         f"{rules}"
                         "❗️Игра не состоится, если придут меньше 10 человек.\n"
                         "Поэтому, пожалуйста, приходите обязательно, если записались или отмените запись, если планы изменятся.🙏",
                         parse_mode="HTML")
    await callback.answer("Запись подтверждена! 😊")

    # Notify admin
    ud = execute_query("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=%s", (user_id,), fetchone=True)
    if ud:
        await bot.send_message(ADMIN_ID, f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

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
    game_data = execute_query("SELECT game_name, game_date FROM games WHERE game_id = %s", (game_id,), fetchone=True)
    if not game_data:
        return 0

    g_name, g_date = game_data
    rules = ""
    if "Спортивная мафия" in g_name:
        rules = "\n17:00 – сбор и объяснение правил\n17:30 – школа мафии\n18:30 – начало игр\n"
    elif "Городская мафия" in g_name:
        rules = "\n18:00 – сбор и объяснение правил\n18:30 – начало игр\n"
    elif "Рейтинговая игра" in g_name:
        rules = "\n19:00 – начало игр\n"

    for uid in user_ids:
        try:
            builder = InlineKeyboardBuilder()
            builder.button(text="📝 Записаться", callback_data=f"reg_{game_id}")
            builder.button(text="🤔 Думаю", callback_data=f"think_{game_id}")
            builder.adjust(2)
            await bot.send_message(uid, f"🔔 Напоминание об игре: {g_date} {g_name}\n{rules}\nБудем вас ждать! 😊", reply_markup=builder.as_markup())
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

    users = execute_query("SELECT user_id FROM users", fetch=True)
    count = 0
    for (user_id,) in users:
        try:
            await bot.send_message(user_id, message.text)
            count += 1
        except Exception as e:
            logging.error(f"Error sending broadcast to {user_id}: {e}")

    await message.answer(f"Сообщение отправлено {count} пользователям.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
