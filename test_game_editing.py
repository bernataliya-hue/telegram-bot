import unittest

from game_editing import format_schedule_change, normalize_game_time


class GameEditingTests(unittest.TestCase):
    def test_normalize_game_time(self):
        self.assertEqual(normalize_game_time("9:05"), "09:05")
        self.assertEqual(normalize_game_time(" 19:30 "), "19:30")
        self.assertIsNone(normalize_game_time("24:00"))
        self.assertIsNone(normalize_game_time("evening"))

    def test_only_changed_values_are_bold(self):
        text = format_schedule_change(
            "Сб 08.08", "🌃Спортивная мафия", "18:15", "18:30", {"game_name", "gathering_time"}
        )
        self.assertIn("играем в <b>🌃Спортивная мафия</b>", text)
        self.assertIn("<b>18:15</b> – сбор", text)
        self.assertIn("\n18:30 – начало игры", text)
        self.assertNotIn("<b>18:30</b>", text)

    def test_values_are_html_escaped(self):
        text = format_schedule_change("Пт 07.08", "<игра>", "18:00", "18:30", {"game_name"})
        self.assertIn("<b>&lt;игра&gt;</b>", text)


if __name__ == "__main__":
    unittest.main()
