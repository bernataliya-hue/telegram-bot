import unittest

from reminder_formatting import format_reminder_game_date


class ReminderFormattingTests(unittest.TestCase):
    def test_adds_weekday_to_legacy_date(self):
        self.assertEqual(format_reminder_game_date("02.08.2026"), "Вс 02.08")

    def test_does_not_duplicate_stored_weekday(self):
        self.assertEqual(format_reminder_game_date("Вс 02.08"), "Вс 02.08")

    def test_keeps_unrecognized_date(self):
        self.assertEqual(format_reminder_game_date("дата уточняется"), "дата уточняется")


if __name__ == "__main__":
    unittest.main()
