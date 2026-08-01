import datetime


def format_reminder_game_date(game_date: str) -> str:
    """Return a game date as ``Пн 01.01`` when it can be parsed."""
    value = (game_date or "").strip()
    parts = value.split()
    if len(parts) >= 2 and parts[0] in ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс'):
        value = parts[1]

    parsed = None
    for date_format in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m"):
        try:
            parsed = datetime.datetime.strptime(value, date_format).date()
            if date_format == "%d.%m":
                parsed = parsed.replace(year=datetime.date.today().year)
            break
        except ValueError:
            continue

    if not parsed:
        return (game_date or "").strip()

    weekdays = ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс')
    return f"{weekdays[parsed.weekday()]} {parsed.strftime('%d.%m')}"
