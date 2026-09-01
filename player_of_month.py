PLAYER_OF_MONTH_BADGE = "👑Игрок месяца"


def decorate_player_of_month(name: str, user_id: int, player_of_month_id: int | None) -> str:
    """Add the award badge to a participant name when the IDs match."""
    if player_of_month_id is not None and int(user_id) == int(player_of_month_id):
        return f"{name} {PLAYER_OF_MONTH_BADGE}"
    return name


def clamp_page(item_count: int, requested_page: int, page_size: int = 10) -> tuple[int, int]:
    """Return a safe zero-based page and the total number of pages."""
    total_pages = max(1, (item_count + page_size - 1) // page_size)
    return max(0, min(requested_page, total_pages - 1)), total_pages
