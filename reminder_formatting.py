from game_dates import parse_date


def format_reminder_game_date(game_date: str) -> str:
    """Return a game date as ``Пн 01.01`` when it can be parsed."""
    value = (game_date or "").strip()
    parts = value.split()
    if len(parts) >= 2 and parts[0] in ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'):
        value = parts[1]

    parsed = parse_date(value)

    if not parsed:
        return (game_date or "").strip()

    weekdays = ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс')
    return f"{weekdays[parsed.weekday()]} {parsed.strftime('%d.%m')}"
