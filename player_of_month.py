PLAYER_TITLES = {
    "month": "👑Игрок месяца",
    "mvp": "🏅 MVP",
    "sheriff": "🕵🏻 Лучший шериф",
    "don": "💍 Лучший дон",
    "red": "♥️ Лучший красный",
    "black": "🖤 Лучший чёрный",
}
PLAYER_OF_MONTH_BADGE = PLAYER_TITLES["month"]


def decorate_player_of_month(name: str, user_id: int, title_holders) -> str:
    """Append every title held by this participant."""
    if isinstance(title_holders, dict):
        badges = [PLAYER_TITLES[key] for key, holder_id in title_holders.items()
                  if key in PLAYER_TITLES and holder_id is not None and int(user_id) == int(holder_id)]
    elif title_holders is not None and int(user_id) == int(title_holders):
        badges = [PLAYER_OF_MONTH_BADGE]
    else:
        badges = []
    return f"{name} {' '.join(badges)}" if badges else name


def clamp_page(item_count: int, requested_page: int, page_size: int = 10) -> tuple[int, int]:
    """Return a safe zero-based page and the total number of pages."""
    total_pages = max(1, (item_count + page_size - 1) // page_size)
    return max(0, min(requested_page, total_pages - 1)), total_pages
