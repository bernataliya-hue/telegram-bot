def format_club_member_label(user_row) -> str:
    """Return a compact club-member label containing their nick and full name."""
    user_id = user_row[0]
    first_name = (user_row[3] or "").strip()
    last_name = (user_row[4] or "").strip()
    mafia_nick = (user_row[5] or "").strip()

    full_name = " ".join(part for part in (first_name, last_name) if part)
    if mafia_nick and full_name:
        return f"{mafia_nick} — {full_name}"
    return mafia_nick or full_name or f"ID {user_id}"
