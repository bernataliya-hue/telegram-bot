import unittest

from game_hosting import add_host_label, order_with_host_first, participant_number


class GameHostingTests(unittest.TestCase):
    def test_host_is_first_and_other_order_is_preserved(self):
        players = [(1, "One"), (2, "Two"), (3, "Three")]
        self.assertEqual(order_with_host_first(players, 2), [players[1], players[0], players[2]])

    def test_host_has_zero_number_and_label(self):
        self.assertEqual(participant_number(1, 2, 2), 0)
        self.assertEqual(add_host_label("Two", 2, 2), "Two - Ведуший")

    def test_regular_player_is_unchanged(self):
        self.assertEqual(participant_number(3, 3, 2), 3)
        self.assertEqual(add_host_label("Three", 3, 2), "Three")


if __name__ == "__main__":
    unittest.main()
