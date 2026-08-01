import unittest

from announcement_formatting import format_announcement


class AnnouncementFormattingTests(unittest.TestCase):
    def test_formats_multiple_games_and_deduplicates_cost(self):
        games = [
            (1, "Городская мафия 🏙", "02.08.2026"),
            (2, "Спортивная мафия 🏆", "03.08.2026"),
        ]

        announcement = format_announcement(
            games,
            lambda _name, _date: "19:00 – сбор и объяснение правил\n19:30 – начало игр\n\n",
            lambda _name: "Стоимость игр 600 руб. с человека. По абонементу - бесплатно.",
        )

        self.assertIn("Всем привет! 👋\nНа следующей неделе играем мафию!", announcement)
        self.assertIn("📅 Вс 02.08 Городская мафия 🏙", announcement)
        self.assertIn("📅 Пн 03.08 Спортивная мафия 🏆", announcement)
        self.assertEqual(announcement.count("Стоимость игр 600 руб."), 1)
        self.assertIn("- в ТГ: https://t.me/mafiya_TK_bot", announcement)
        self.assertIn("- в ВК: https://vk.ru/club236985675", announcement)

    def test_includes_each_distinct_price_for_mixed_games(self):
        games = [(1, "Городская", "02.08.2026"), (2, "Рейтинговая", "03.08.2026")]

        announcement = format_announcement(
            games,
            lambda _name, _date: "19:00 – начало игр",
            lambda name: "800 руб." if "Рейтинговая" in name else "600 руб.",
        )

        self.assertIn("💵600 руб. 800 руб. 💵", announcement)


if __name__ == "__main__":
    unittest.main()
