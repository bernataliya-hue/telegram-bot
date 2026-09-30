from game_dates import display_game_date


def format_reminder_game_date(game_date: str) -> str:
    """Return a game date as ``Пн 01.01`` when it can be parsed."""
    return display_game_date(game_date)
