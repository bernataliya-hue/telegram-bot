"""Calendar dates are stored with a year; yearless input means this year."""
import datetime

WEEKDAYS = ('Пн', 'Вт', 'Ср', 'Чт', 'Пт', 'Сб', 'Вс')


def parse_date(value, default_year=None):
    value = (value or '').strip()
    parts = value.split()
    if len(parts) >= 2 and parts[0] in WEEKDAYS:
        value = ' '.join(parts[1:])
    if len(value.split('.')) == 2:
        value = f"{value}.{default_year or datetime.date.today().year}"
    for fmt in ('%d.%m.%Y', '%Y-%m-%d', '%d.%m.%Y %H:%M', '%Y-%m-%d %H:%M:%S'):
        try:
            return datetime.datetime.strptime(value, fmt).date()
        except ValueError:
            continue
    return None


def stored_game_date(value):
    return f"{WEEKDAYS[value.weekday()]} {value:%d.%m.%Y}"


def display_game_date(value):
    """Format a stored date for people without changing its stored year."""
    parsed = parse_date(value)
    if not parsed:
        return (value or '').strip()
    return f"{WEEKDAYS[parsed.weekday()]} {parsed:%d.%m}"


def game_choice_label(game, games):
    """Reply buttons need distinct labels even for matching dates in different years."""
    game_id, name, date = game[:3]
    label = f"{display_game_date(date)} {name}"
    duplicates = sum(f"{display_game_date(row[2])} {row[1]}" == label for row in games)
    return f"{label} · №{game_id}" if duplicates > 1 else label


def match_game_label(games, text):
    normalized = (text or '').replace('📆', '').replace('📅', '').strip().casefold()
    matches = []
    for game in games:
        _, name, date = game[:3]
        labels = {game_choice_label(game, games)}
        for shown_name in (name, name.replace('🏆', '🌃')):
            for shown_date in (date, display_game_date(date)):
                labels.update((f"{shown_date} {shown_name}", f"{shown_name} {shown_date}"))
        if normalized in {label.casefold() for label in labels}:
            matches.append(game)
    return matches[0] if len(matches) == 1 else None
