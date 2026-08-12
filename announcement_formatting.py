from reminder_formatting import format_reminder_game_date


def format_announcement(games, game_rules, cost_info) -> str:
    """Format a weekly announcement for the selected game records."""
    game_blocks = []
    cost_lines = []
    for _, game_name, game_date in games:
        game_blocks.append(
            f"📅 {format_reminder_game_date(game_date)} {game_name}\n"
            f"{game_rules(game_name, game_date).strip()}"
        )
        game_cost = cost_info(game_name)
        if game_cost not in cost_lines:
            cost_lines.append(game_cost)

    games_text = "\n\n".join(game_blocks)
    costs_text = " ".join(cost_lines)
    return (
        "Всем привет! 👋\n"
        "На следующей неделе играем в мафию!\n\n"
        f"{games_text}\n\n"
        f"💵{costs_text} 💵\n\n"
        "🎁 Если ты первый раз в Тайной Комнате - тебе скидка 200 руб.\n"
        "🎁 Если вы пришли вдвоем - платите одним переводом 1 000 руб. \n"
        "❗️Скидки и акции не суммируются❗️\n\n"
        "Запись через ботов: 🤖\n"
        "- в ТГ: https://t.me/mafiya_TK_bot\n"
        "- в ВК: https://vk.ru/club236985675\n\n"
        "P.S. Если на улице мокро, возьмите, пожалуйста, с собой сменку или пользуйтесь тапочками ТК🙏"
    )
