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
