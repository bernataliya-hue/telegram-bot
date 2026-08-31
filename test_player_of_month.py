import unittest

from player_of_month import clamp_page, decorate_player_of_month


class PlayerOfMonthTests(unittest.TestCase):
    def test_decorates_selected_player(self):
        self.assertEqual(
            decorate_player_of_month("Мориарти", 42, 42),
            "Мориарти 👑 игрок месяца",
        )

    def test_does_not_decorate_other_player(self):
        self.assertEqual(decorate_player_of_month("Ирен", 7, 42), "Ирен")

    def test_clamps_pagination_to_ten_players(self):
        self.assertEqual(clamp_page(21, 0), (0, 3))
        self.assertEqual(clamp_page(21, 99), (2, 3))
        self.assertEqual(clamp_page(0, -1), (0, 1))


if __name__ == "__main__":
    unittest.main()
