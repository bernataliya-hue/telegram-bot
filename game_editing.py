import datetime
import html
import re


GAME_TYPES = (
    "🏙️Городская мафия",
    "🌃Спортивная мафия",
    "🏆Рейтинговая игра",
)


def normalize_game_time(value: str) -> str | None:
    value = (value or "").strip()
    if not re.fullmatch(r"\d{1,2}:\d{2}", value):
        return None
    try:
        parsed = datetime.datetime.strptime(value, "%H:%M")
    except ValueError:
        return None
    return parsed.strftime("%H:%M")


def format_schedule_change(game_date, game_name, gathering_time, start_time, changed_fields):
    def display(value, field):
        escaped = html.escape(value)
        return f"<b>{escaped}</b>" if field in changed_fields else escaped

    return (
        "❗️Внимание❗️Изменение в рассписании❗️\n"
        f"📆 {html.escape(game_date)} играем в {display(game_name, 'game_name')}\n"
        f"{display(gathering_time, 'gathering_time')} – сбор и объяснение правил\n"
        f"{display(start_time, 'start_time')} – начало игры"
    )
