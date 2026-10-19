import datetime

from game_dates import display_game_date, parse_date


def format_reminder_game_date(game_date: str) -> str:
    """Return a game date as ``Пн 01.01`` when it can be parsed."""
    return display_game_date(game_date)


def game_week_phrase(game_date: str, today=None) -> str:
    """Return the announcement/reminder phrase for the game's calendar week."""
    today = today or datetime.date.today()
    parsed = parse_date(game_date, default_year=today.year)
    if parsed:
        this_monday = today - datetime.timedelta(days=today.weekday())
        game_monday = parsed - datetime.timedelta(days=parsed.weekday())
        if game_monday == this_monday:
            return "На этой неделе играем"
    return "На следующей неделе играем"
