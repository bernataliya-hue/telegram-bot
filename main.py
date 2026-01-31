import asyncio
import logging
import os
import sqlite3

from aiogram import Bot, Dispatcher, types, F
from aiogram.filters import Command
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import State, StatesGroup
from aiogram.fsm.storage.memory import MemoryStorage
from aiogram.utils.keyboard import ReplyKeyboardBuilder, InlineKeyboardBuilder

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

# Подключение к SQLite
conn = sqlite3.connect("mafia_bot.db", check_same_thread=False)
cursor = conn.cursor()

# Создаем таблицы
cursor.execute("""
CREATE TABLE IF NOT EXISTS users (
    user_id INTEGER PRIMARY KEY,
    first_name TEXT,
    last_name TEXT,
    mafia_nick TEXT,
    age INTEGER,
    telegram_username TEXT
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS games (
    game_id INTEGER PRIMARY KEY AUTOINCREMENT,
    game_name TEXT,
    game_date TEXT,
    UNIQUE(game_name, game_date)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS registrations (
    user_id INTEGER,
    game_id INTEGER,
    PRIMARY KEY(user_id, game_id)
)
""")

cursor.execute("""
CREATE TABLE IF NOT EXISTS settings (
    key TEXT PRIMARY KEY,
    value TEXT
)
""")
cursor.execute("INSERT OR IGNORE INTO settings (key, value) VALUES ('schedule', 'Расписание пока не установлено')")
conn.commit()

# Состояния FSM
class Form(StatesGroup):
    start = State()
    get_name = State()
    get_lastname = State()
    get_nick = State()
    get_age = State()
    menu = State()
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
    admin_broadcast = State()

# Главное меню
def main_menu_keyboard(user_id):
    builder = ReplyKeyboardBuilder()
    builder.button(text="📝Записаться на игру")
    builder.button(text="❌Отменить запись")
    builder.button(text="📅Расписание игр")
    builder.button(text="📍Как до нас добраться?")
    if user_id == ADMIN_ID:
        builder.button(text="⚙️ Админ-панель")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

def admin_menu_keyboard():
    builder = ReplyKeyboardBuilder()
    builder.button(text="➕ Добавить игру")
    builder.button(text="❌ Удалить игру")
    builder.button(text="🚫 Отмена игры")
    builder.button(text="🔔 Напомнить об игре")
    builder.button(text="📢 Рассылка")
    builder.button(text="👥 Список участников")
    builder.button(text="🏠 Главное меню")
    builder.adjust(2)
    return builder.as_markup(resize_keyboard=True)

# /start
@dp.message(Command("start"))
async def cmd_start(message: types.Message, state: FSMContext):
    # Проверяем, зарегистрирован ли пользователь
    cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id = ?", (message.from_user.id,))
    user = cursor.fetchone()
    
    if user:
        # Пользователь уже есть в базе
        builder = ReplyKeyboardBuilder()
        builder.button(text="✅ Оставить как есть")
        builder.button(text="📝 Обновить профиль")
        builder.adjust(1)
        
        await message.answer(
            f"С возвращением, {user[2]}!\n"
            "Ваши данные уже есть в базе. Желаете обновить информацию о себе?",
            reply_markup=builder.as_markup(resize_keyboard=True)
        )
        await state.set_state(Form.confirm_profile_update)
        return

    builder = ReplyKeyboardBuilder()
    builder.button(text="Да")
    builder.button(text="Нет")
    await message.answer(
        "Привет! Готов познакомиться?",
        reply_markup=builder.as_markup(resize_keyboard=True)
    )
    await state.set_state(Form.start)

@dp.message(Form.confirm_profile_update)
async def process_confirm_profile_update(message: types.Message, state: FSMContext):
    if message.text == "📝 Обновить профиль":
        await message.answer("Хорошо! Давайте обновим вашу анкету. Как вас зовут?")
        await state.set_state(Form.get_name)
    elif message.text == "✅ Оставить как есть":
        await message.answer("Отлично! Переходим в главное меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
    else:
        await message.answer("Пожалуйста, воспользуйтесь кнопками для выбора.")

@dp.message(Form.start)
async def process_start(message: types.Message, state: FSMContext):
    if message.text and message.text.lower() == "да":
        await message.answer("Как тебя зовут?")
        await state.set_state(Form.get_name)
    else:
        await message.answer("Хорошо, запускай бота снова, когда будешь готов.")
        await state.clear()

@dp.message(Form.get_name)
async def process_name(message: types.Message, state: FSMContext):
    await state.update_data(first_name=message.text)
    await message.answer("А какая у тебя фамилия?")
    await state.set_state(Form.get_lastname)

@dp.message(Form.get_lastname)
async def process_lastname(message: types.Message, state: FSMContext):
    await state.update_data(last_name=message.text)
    await message.answer(
        "И какой у тебя игровой ник в мафии?\n"
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

    cursor.execute("""
        INSERT INTO users (user_id, first_name, last_name, mafia_nick, age, telegram_username)
        VALUES (?, ?, ?, ?, ?, ?)
        ON CONFLICT(user_id) DO UPDATE SET
        first_name=excluded.first_name,
        last_name=excluded.last_name,
        mafia_nick=excluded.mafia_nick,
        age=excluded.age,
        telegram_username=excluded.telegram_username
    """, (message.from_user.id, data['first_name'], data['last_name'], data['mafia_nick'], age, message.from_user.username))
    conn.commit()

    if age < 18:
        await message.answer(
            "В Тайной комнате действуют возрастные ограничения для игры в мафию:\n"
            "• 18+ для Спортивной мафии\n"
            "• 16+ для Городской мафии"
        )

    await message.answer(
        "☺️Спасибо за знакомство! Обрати внимание на кнопки меню ниже.",
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
        await message.answer("Введите дату игры (например, 📆 31.01):")
        await state.set_state(Form.add_game_date)
    elif message.text == "❌ Удалить игру":
        cursor.execute("SELECT game_id, game_name, game_date FROM games")
        games = cursor.fetchall()
        if not games:
            await message.answer("Список игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{name} {date}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Какую игру удалить?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.delete_game)
    elif message.text == "👥 Список участников":
        cursor.execute("SELECT game_id, game_name, game_date FROM games")
        games = cursor.fetchall()
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
    elif message.text == "📝 Редактировать расписание":
        cursor.execute("SELECT value FROM settings WHERE key = 'schedule'")
        res = cursor.fetchone()
        current_schedule = res[0] if res else "Нет расписания"
        await message.answer(f"Текущее расписание:\n\n{current_schedule}\n\nВведите новое расписание:")
        await state.set_state(Form.edit_schedule)
    elif message.text == "🚫 Отмена игры":
        cursor.execute("SELECT game_id, game_name, game_date FROM games")
        games = cursor.fetchall()
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
        cursor.execute("SELECT game_id, game_name, game_date FROM games")
        games = cursor.fetchall()
        if not games:
            await message.answer("Список игр пуст.")
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            builder.button(text=f"{date} {name}")
        builder.button(text="🔙 Назад")
        builder.adjust(1)
        await message.answer("Выберите игру, о которой нужно напомнить незаписанным игрокам:", reply_markup=builder.as_markup(resize_keyboard=True))
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
    cursor.execute("UPDATE settings SET value = ? WHERE key = 'schedule'", (message.text,))
    conn.commit()
    await message.answer("Расписание успешно обновлено!", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.add_game_date)
async def process_add_game_date(message: types.Message, state: FSMContext):
    await state.update_data(game_date=message.text)
    builder = ReplyKeyboardBuilder()
    builder.button(text="🏙️Городская мафия")
    builder.button(text="🌃Спортивная мафия")
    builder.button(text="🏆Рейтинговая игра")
    builder.adjust(1)
    await message.answer("Выберите тип игры:", reply_markup=builder.as_markup(resize_keyboard=True))
    await state.set_state(Form.add_game_type)

@dp.message(Form.add_game_type)
async def process_add_game_type(message: types.Message, state: FSMContext):
    if message.text not in ["🏙️Городская мафия", "🌃Спортивная мафия", "🏆Рейтинговая игра"]:
        await message.answer("Пожалуйста, выберите один из вариантов кнопками.")
        return
    
    data = await state.get_data()
    date = data['game_date']
    name = message.text
    
    cursor.execute("INSERT INTO games (game_name, game_date) VALUES (?, ?)", (name, date))
    conn.commit()
    await message.answer(f"Игра '{name} {date}' успешно добавлена!", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.delete_game)
async def delete_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    cursor.execute("SELECT game_id FROM games WHERE game_name || ' ' || game_date = ?", (message.text,))
    result = cursor.fetchone()
    if result:
        game_id = result[0]
        cursor.execute("DELETE FROM registrations WHERE game_id = ?", (game_id,))
        cursor.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
        conn.commit()
        await message.answer(f"Игра '{message.text}' удалена.", reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.view_participants)
async def view_participants_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    # Check both formats (name date and date name)
    cursor.execute("SELECT game_id FROM games WHERE game_name || ' ' || game_date = ? OR game_date || ' ' || game_name = ?", (message.text, message.text))
    result = cursor.fetchone()
    if result:
        game_id = result[0]
        cursor.execute("""
            SELECT u.first_name, u.last_name, u.mafia_nick 
            FROM registrations r
            JOIN users u ON r.user_id = u.user_id
            WHERE r.game_id = ?
        """, (game_id,))
        participants = cursor.fetchall()
        if not participants:
            await message.answer(f"На игру '{message.text}' пока никто не записался.", reply_markup=admin_menu_keyboard())
        else:
            response = f"Список участников на игру {message.text}:\n"
            for i, (fn, ln, nick) in enumerate(participants, 1):
                response += f"{i}. {fn} {ln} ({nick})\n"
            await message.answer(response, reply_markup=admin_menu_keyboard())
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.message(Form.menu)
async def menu_handler(message: types.Message, state: FSMContext):
    if message.text == "📝Записаться на игру":
        cursor.execute("SELECT game_id, game_name, game_date FROM games")
        games = cursor.fetchall()
        if not games:
            await message.answer("К сожалению, на данный момент игр для записи нет.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            # Fix emoji for display if it was stored with the old one
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}")
        builder.button(text="🔙 В меню")
        builder.adjust(1)
        await message.answer("На какую игру вы хотите записаться?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.game_registration)
    elif message.text == "❌Отменить запись":
        cursor.execute("""
            SELECT g.game_id, g.game_name, g.game_date 
            FROM registrations r
            JOIN games g ON r.game_id=g.game_id
            WHERE r.user_id=?
        """, (message.from_user.id,))
        games = cursor.fetchall()
        if not games:
            await message.answer("У вас нет записей на игры.", reply_markup=main_menu_keyboard(message.from_user.id))
            return
        builder = ReplyKeyboardBuilder()
        for _, name, date in games:
            # Fix emoji for display
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            builder.button(text=f"📆{date} {display_name}")
        builder.button(text="🔙 В меню")
        builder.adjust(1)
        await message.answer("Запись на какую игру вы хотите отменить?", reply_markup=builder.as_markup(resize_keyboard=True))
        await state.set_state(Form.game_cancellation)
    elif message.text == "📅Расписание игр":
        cursor.execute("SELECT game_name, game_date FROM games ORDER BY game_date")
        games = cursor.fetchall()
        
        if not games:
            await message.answer("<b>Расписание ближайших игр:</b>\n\nИгр пока не запланировано.", parse_mode="HTML")
            return

        schedule_text = "<b>Расписание ближайших игр:</b>\n\n"
        
        sport_rules = (
            "17:00 – сбор и объяснение правил\n"
            "17:30 – школа мафии\n"
            "18:30 – начало игр\n\n"
        )
        
        city_rules = (
            "18:00 – сбор и объяснение правил\n"
            "18:30 – начало игр\n\n"
        )

        rating_rules = (
            "19:00 – начало игр\n\n"
        )

        for name, date in games:
            # Ensure we use the correct emoji for existing games in the schedule
            display_name = name
            if "Спортивная мафия" in name and "🌃" not in name:
                display_name = name.replace("🏆", "🌃")
            
            schedule_text += f"📆{date} {display_name}\n"
            if "Спортивная мафия" in display_name:
                schedule_text += sport_rules
            elif "Рейтинговая игра" in display_name:
                schedule_text += rating_rules
            elif "Городская мафия" in display_name:
                schedule_text += city_rules
            else:
                schedule_text += "\n"
        
        await message.answer(schedule_text.strip(), parse_mode="HTML")
    elif message.text == "📍Как до нас добраться?":
        await message.answer(
            "г. Королев, ул. Декабристов, д. 8\n"
            "Вход со стороны дороги (не со двора), ищите стеклянную дверь с надписью «Тайная комната». Спускайтесь по лестнице в самый низ.\n\n"
            "Пожалуйста, отмените запись в этом боте, если планы изменятся!"
        )

@dp.message(Form.game_registration)
async def register_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Вы вернулись в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    # Remove emoji for lookup
    clean_text = message.text.replace("📆", "") if message.text else ""
    cursor.execute("SELECT game_id FROM games WHERE game_date || ' ' || game_name = ?", (clean_text,))
    result = cursor.fetchone()
    if result:
        game_id = result[0]
        cursor.execute("INSERT OR IGNORE INTO registrations (user_id, game_id) VALUES (?, ?)", (message.from_user.id, game_id))
        conn.commit()
        await message.answer(f"Вы успешно записаны на {message.text}!", reply_markup=main_menu_keyboard(message.from_user.id))
        # Notify admin
        cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=?", (message.from_user.id,))
        ud = cursor.fetchone()
        if ud:
            await bot.send_message(ADMIN_ID, f"Новая запись: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    await state.set_state(Form.menu)

@dp.message(Form.game_cancellation)
async def cancel_game(message: types.Message, state: FSMContext):
    if message.text == "🔙 В меню":
        await message.answer("Вы вернулись в меню.", reply_markup=main_menu_keyboard(message.from_user.id))
        await state.set_state(Form.menu)
        return
    # Remove emoji for lookup
    clean_text = message.text.replace("📆", "") if message.text else ""
    cursor.execute("SELECT game_id FROM games WHERE game_date || ' ' || game_name = ?", (clean_text,))
    result = cursor.fetchone()
    if result:
        game_id = result[0]
        cursor.execute("DELETE FROM registrations WHERE user_id=? AND game_id=?", (message.from_user.id, game_id))
        conn.commit()
        await message.answer("Запись отменена.", reply_markup=main_menu_keyboard(message.from_user.id))
        
        # Notify admin
        cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=?", (message.from_user.id,))
        ud = cursor.fetchone()
        if ud:
            await bot.send_message(ADMIN_ID, f"❌ Отмена записи: {ud[0]} {ud[1]} ({ud[2]}) на {message.text}")
    await state.set_state(Form.menu)

@dp.message(Form.admin_cancel_game)
async def admin_cancel_game_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
        
    cursor.execute("SELECT game_id, game_name, game_date FROM games WHERE game_date || ' ' || game_name = ?", (message.text,))
    result = cursor.fetchone()
    if result:
        game_id = result[0]
        game_info = message.text
        
        # Получаем всех записавшихся игроков
        cursor.execute("SELECT user_id FROM registrations WHERE game_id = ?", (game_id,))
        participants = cursor.fetchall()
        
        # Уведомляем игроков
        for (user_id,) in participants:
            try:
                await bot.send_message(
                    user_id, 
                    f"⚠️ Внимание! Отмена игры на {game_info}! ⚠️"
                )
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        
        # Удаляем регистрации и саму игру
        cursor.execute("DELETE FROM registrations WHERE game_id = ?", (game_id,))
        cursor.execute("DELETE FROM games WHERE game_id = ?", (game_id,))
        conn.commit()
        
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
        
    cursor.execute("SELECT game_id, game_name, game_date FROM games WHERE game_date || ' ' || game_name = ?", (message.text,))
    result = cursor.fetchone()
    if result:
        game_id, g_name, g_date = result[0], result[1], result[2]
        
        # Определяем расписание в зависимости от типа игры
        rules = ""
        if "Спортивная мафия" in g_name:
            rules = (
                "\n17:00 – сбор и объяснение правил\n"
                "17:30 – школа мафии\n"
                "18:30 – начало игр\n"
            )
        elif "Городская мафия" in g_name:
            rules = (
                "\n18:00 – сбор и объяснение правил\n"
                "18:30 – начало игр\n"
            )
        elif "Рейтинговая игра" in g_name:
            rules = "\n19:00 – начало игр\n"

        # Находим всех пользователей
        cursor.execute("SELECT user_id FROM users")
        all_users = [row[0] for row in cursor.fetchall()]
        
        # Находим тех, кто записан
        cursor.execute("SELECT user_id FROM registrations WHERE game_id = ?", (game_id,))
        registered_users = [row[0] for row in cursor.fetchall()]
        
        reg_count = 0
        cancel_remind_count = 0
        
        for user_id in all_users:
            try:
                if user_id in registered_users:
                    # Для тех, кто записан
                    inline_builder = InlineKeyboardBuilder()
                    inline_builder.button(text="❌ Отменить запись", callback_data=f"unreg_{game_id}")
                    
                    await bot.send_message(
                        user_id,
                        f"Привет!\nНапоминаю, что ты записан на игру {g_date} в {g_name}!\nЕсли передумал, отпишись, пожалуйста 🙏",
                        reply_markup=inline_builder.as_markup()
                    )
                    cancel_remind_count += 1
                else:
                    # Для тех, кто не записан
                    inline_builder = InlineKeyboardBuilder()
                    inline_builder.button(text="📝 Записаться", callback_data=f"reg_{game_id}")
                    
                    await bot.send_message(
                        user_id,
                        f"Привет!\nНапоминаю, что {g_date} состоится игра {g_name}!\n{rules}\nЗапишись, пожалуйста, если планируешь прийти!",
                        reply_markup=inline_builder.as_markup()
                    )
                    reg_count += 1
            except Exception as e:
                logging.error(f"Не удалось отправить уведомление пользователю {user_id}: {e}")
        
        await message.answer(
            f"Рассылка завершена!\n"
            f"🔔 Напоминание записавшимся: {cancel_remind_count} чел.\n"
            f"📝 Предложение записаться: {reg_count} чел.",
            reply_markup=admin_menu_keyboard()
        )
    else:
        await message.answer("Игра не найдена.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

@dp.callback_query(F.data.startswith("unreg_"))
async def callback_unregister(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    cursor.execute("SELECT game_name, game_date FROM games WHERE game_id = ?", (game_id,))
    game = cursor.fetchone()
    
    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    cursor.execute("DELETE FROM registrations WHERE user_id = ? AND game_id = ?", (user_id, game_id))
    conn.commit()
    
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"Запись на {game[1]} {game[0]} отменена.", reply_markup=main_menu_keyboard(user_id))
    await callback.answer("Запись отменена.")
    
    # Уведомляем админа
    cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=?", (user_id,))
    ud = cursor.fetchone()
    if ud:
        await bot.send_message(ADMIN_ID, f"❌ Отмена записи (через напоминание): {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("unreg_"))
async def callback_unregister(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    cursor.execute("SELECT game_name, game_date FROM games WHERE game_id = ?", (game_id,))
    game = cursor.fetchone()
    
    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    cursor.execute("DELETE FROM registrations WHERE user_id = ? AND game_id = ?", (user_id, game_id))
    conn.commit()
    
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"Запись на {game[1]} {game[0]} отменена.", reply_markup=main_menu_keyboard(user_id))
    await callback.answer("Запись отменена.")
    
    # Уведомляем админа
    cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=?", (user_id,))
    ud = cursor.fetchone()
    if ud:
        await bot.send_message(ADMIN_ID, f"❌ Отмена записи (через напоминание): {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.callback_query(F.data.startswith("reg_"))
async def callback_register(callback: types.CallbackQuery):
    game_id = int(callback.data.split("_")[1])
    user_id = callback.from_user.id
    
    # Получаем данные об игре для уведомлений
    cursor.execute("SELECT game_name, game_date FROM games WHERE game_id = ?", (game_id,))
    game = cursor.fetchone()
    
    if not game:
        await callback.answer("Игра не найдена.", show_alert=True)
        return

    # Записываем пользователя
    cursor.execute("INSERT OR IGNORE INTO registrations (user_id, game_id) VALUES (?, ?)", (user_id, game_id))
    conn.commit()
    
    await callback.message.edit_reply_markup(reply_markup=None)
    await callback.message.answer(f"Вы успешно записаны на {game[1]} {game[0]}!", reply_markup=main_menu_keyboard(user_id))
    await callback.answer("Вы успешно записаны!")
    
    # Уведомляем админа
    cursor.execute("SELECT first_name, last_name, mafia_nick FROM users WHERE user_id=?", (user_id,))
    ud = cursor.fetchone()
    if ud:
        await bot.send_message(ADMIN_ID, f"Новая запись (через напоминание): {ud[0]} {ud[1]} ({ud[2]}) на {game[1]} {game[0]}")

@dp.message(Form.admin_broadcast)
async def admin_broadcast_handler(message: types.Message, state: FSMContext):
    if message.text == "🔙 Назад":
        await message.answer("Вы вернулись в админ-меню", reply_markup=admin_menu_keyboard())
        await state.set_state(Form.admin_menu)
        return
    
    broadcast_text = message.text
    cursor.execute("SELECT user_id FROM users")
    users = cursor.fetchall()
    
    count = 0
    for (user_id,) in users:
        try:
            await bot.send_message(user_id, broadcast_text)
            count += 1
        except Exception as e:
            logging.error(f"Не удалось отправить рассылку пользователю {user_id}: {e}")
            
    await message.answer(f"Рассылка завершена! Сообщение получили {count} пользователей.", reply_markup=admin_menu_keyboard())
    await state.set_state(Form.admin_menu)

async def main():
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())
