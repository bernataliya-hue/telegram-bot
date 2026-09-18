import unittest

from club_members import format_club_member_label


class ClubMemberLabelTests(unittest.TestCase):
    def test_label_contains_nick_first_name_and_last_name(self):
        user = (42, "telegram", 42, "Иван", "Иванов", "Шериф", 30, "ivan", None)

        self.assertEqual(format_club_member_label(user), "Шериф — Иван Иванов")

    def test_label_uses_available_name_when_nick_is_missing(self):
        user = (42, "telegram", 42, "Иван", "Иванов", None, 30, "ivan", None)

        self.assertEqual(format_club_member_label(user), "Иван Иванов")

    def test_label_handles_incomplete_profile(self):
        user = (42, "telegram", 42, None, None, None, None, None, None)

        self.assertEqual(format_club_member_label(user), "ID 42")


if __name__ == "__main__":
    unittest.main()
