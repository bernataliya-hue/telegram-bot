"""Exercise real handler bodies without starting polling or connecting to production.

AST loading omits module startup and decorators only. SQLite executes the small
SQL subset used below; this does not replace a PostgreSQL deployment smoke test.
"""
import ast
import datetime
import logging
from pathlib import Path
import sqlite3
from types import SimpleNamespace as NS
import unittest
from unittest.mock import AsyncMock, MagicMock, Mock

from game_dates import parse_date, stored_game_date, display_game_date, game_choice_label, match_game_label
from user_identity import MANUAL_ID_START, PLATFORM_MANUAL, detect_platform_by_user_id


def load_functions(filename, namespace):
    tree = ast.parse(Path(__file__).with_name(filename).read_text(encoding='utf-8'))
    for node in tree.body:
        if isinstance(node, ast.Assign):
            try:
                value = ast.literal_eval(node.value)
            except (ValueError, TypeError):
                continue
            for target in node.targets:
                if isinstance(target, ast.Name):
                    namespace.setdefault(target.id, value)
    functions = [node for node in tree.body if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))]
    for node in functions:
        node.decorator_list = []
    module = ast.Module(body=functions, type_ignores=[])
    exec('from __future__ import annotations\n' + ast.unparse(module), namespace)
    return namespace


class Cursor:
    def __init__(self, connection):
        self.raw = connection.cursor()

    def execute(self, sql, params=()):
        self.raw.execute(sql.replace('%s', '?'), params)

    def fetchone(self):
        return self.raw.fetchone()

    def fetchall(self):
        return self.raw.fetchall()

    def close(self):
        self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


class Connection:
    def __init__(self, raw):
        self.raw = raw

    def cursor(self):
        return Cursor(self.raw)

    def commit(self):
        self.raw.commit()

    def rollback(self):
        self.raw.rollback()

    def close(self):
        pass  # Tests share the in-memory database between production queries.

    def __enter__(self):
        return self

    def __exit__(self, error_type, *args):
        self.rollback() if error_type else self.commit()


class BotRegressionTests(unittest.IsolatedAsyncioTestCase):
    def setUp(self):
        self.db = sqlite3.connect(':memory:')
        self.addCleanup(self.db.close)
        self.db.executescript('''
            CREATE TABLE games (game_id INTEGER PRIMARY KEY, game_name TEXT, game_date TEXT, is_deleted BOOLEAN DEFAULT FALSE);
            CREATE TABLE users (user_id INTEGER PRIMARY KEY, platform TEXT, platform_user_id INTEGER,
                first_name TEXT, last_name TEXT, mafia_nick TEXT, age INTEGER);
            CREATE TABLE registrations (user_id INTEGER, game_id INTEGER, status TEXT,
                is_late BOOLEAN DEFAULT FALSE, registered_at TEXT DEFAULT 'initial', PRIMARY KEY(user_id, game_id));
            CREATE TABLE thinking_players (user_id INTEGER, game_id INTEGER, PRIMARY KEY(user_id, game_id));
            CREATE TABLE late_players (user_id INTEGER, game_id INTEGER, PRIMARY KEY(user_id, game_id));
        ''')
        self.db.create_function('clock_timestamp', 0, lambda: 'new')
        self.manual_id = MANUAL_ID_START

        def nextval(name):
            result = self.manual_id
            self.manual_id -= 1
            return result

        self.db.create_function('nextval', 1, nextval)
        self.ns = load_functions('main.py', {
            'database': NS(get_connection=lambda: Connection(self.db)),
            'datetime': datetime, 'logging': logging, 'parse_date': parse_date,
            'stored_game_date': stored_game_date, 'display_game_date': display_game_date,
            'game_choice_label': game_choice_label, 'match_game_label': match_game_label,
            'LEGACY_GAME_YEAR': 2026,
            'PLATFORM_TELEGRAM': 'telegram', 'PLATFORM_VK': 'vk', 'PLATFORM_MANUAL': PLATFORM_MANUAL,
            'detect_platform_by_user_id': detect_platform_by_user_id,
            'Form': NS(menu='menu', admin_menu='admin_menu', add_game_type='add_game_type'),
        })
        self.ns.update(
            ADMIN_IDS={self.ns['ADMIN_ID'], self.ns['SECOND_ADMIN_ID']},
            notify_admin=AsyncMock(), is_game_full=AsyncMock(return_value=False),
            late_button_keyboard=Mock(return_value=None), main_menu_keyboard=Mock(return_value=None),
            admin_menu_keyboard=Mock(return_value=None), build_registration_success_text=lambda *args: 'success',
            bot=NS(send_message=AsyncMock()), vk_api_client=Mock(),
        )
        self.db.execute("INSERT INTO users (user_id, age) VALUES (42, 30)")
        self.db.commit()
        self.state = NS(set_state=AsyncMock(), update_data=AsyncMock())

    def game(self, date=None, deleted=False):
        date = date or str(datetime.date.today() + datetime.timedelta(days=1))
        self.db.execute('INSERT INTO games VALUES (1, ?, ?, ?)', ('🌃Спортивная мафия', date, deleted))
        self.db.commit()

    def callback(self, data):
        return NS(data=data, from_user=NS(id=42), answer=AsyncMock(),
                  message=NS(answer=AsyncMock(), edit_reply_markup=AsyncMock()))

    async def test_old_registration_buttons_reject_deleted_and_past_games(self):
        for handler, payload in [('callback_reg', 'reg_1'), ('callback_thinking_reminder_yes', 'thinkrem_yes_1')]:
            for date, deleted in [('2000-01-01', False), ('2099-01-01', True)]:
                with self.subTest(handler=handler, deleted=deleted):
                    self.db.execute('DELETE FROM games')
                    self.game(date, deleted)
                    callback = self.callback(payload)
                    await self.ns[handler](callback, self.state)
                    self.assertEqual(self.db.execute('SELECT count(*) FROM registrations').fetchone()[0], 0)
                    self.assertTrue(callback.answer.call_args.kwargs['show_alert'])

    async def test_valid_game_registers_and_clears_thinking_and_legacy_lateness(self):
        self.game()
        self.db.execute('INSERT INTO thinking_players VALUES (42, 1)')
        self.db.execute('INSERT INTO late_players VALUES (42, 1)')
        await self.ns['callback_reg'](self.callback('reg_1'), self.state)
        self.assertEqual(self.db.execute('SELECT status, is_late FROM registrations').fetchone(), ('registered', 0))
        self.assertEqual(self.db.execute('SELECT count(*) FROM thinking_players').fetchone()[0], 0)
        self.assertEqual(self.db.execute('SELECT count(*) FROM late_players').fetchone()[0], 0)

    async def test_vk_rejects_past_game(self):
        self.game('2000-01-01')
        response = await self.ns['handle_vk_registration'](-42, 1)
        self.assertIn('недоступна', response)
        self.assertEqual(self.db.execute('SELECT count(*) FROM registrations').fetchone()[0], 0)

    async def test_thinking_releases_seat_and_late_flags(self):
        self.game()
        self.db.execute("INSERT INTO registrations (user_id, game_id, status, is_late) VALUES (42, 1, 'registered', TRUE)")
        self.db.execute('INSERT INTO late_players VALUES (42, 1)')
        self.db.commit()
        await self.ns['callback_think'](self.callback('think_1'))
        self.assertEqual(self.db.execute('SELECT status, is_late FROM registrations').fetchone(), ('declined', 0))
        self.assertEqual(self.db.execute('SELECT * FROM thinking_players').fetchall(), [(42, 1)])
        self.assertEqual(self.db.execute('SELECT * FROM late_players').fetchall(), [])
        await self.ns['mark_thinking'](42, 1)
        self.assertEqual(self.db.execute('SELECT count(*) FROM thinking_players').fetchone()[0], 1)

    async def test_thinking_rejects_deleted_game(self):
        self.game(deleted=True)
        await self.ns['callback_think'](self.callback('think_1'))
        self.assertEqual(self.db.execute('SELECT * FROM thinking_players').fetchall(), [])

    async def test_thinking_transaction_rolls_back_on_failure(self):
        self.db.execute("INSERT INTO registrations (user_id, game_id, status, is_late) VALUES (42, 1, 'registered', TRUE)")
        self.db.execute('INSERT INTO late_players VALUES (42, 1)')
        self.db.execute('DROP TABLE thinking_players')
        self.db.commit()
        with self.assertRaises(sqlite3.OperationalError):
            await self.ns['mark_thinking'](42, 1)
        self.assertEqual(self.db.execute('SELECT status, is_late FROM registrations').fetchone(), ('registered', 1))
        self.assertEqual(self.db.execute('SELECT * FROM late_players').fetchall(), [(42, 1)])

    async def test_cancellation_menu_excludes_declined_and_deleted(self):
        for deleted, status in [(False, 'declined'), (True, 'registered')]:
            with self.subTest(deleted=deleted, status=status):
                self.db.execute('DELETE FROM games')
                self.db.execute('DELETE FROM registrations')
                self.game(deleted=deleted)
                self.db.execute('INSERT INTO registrations (user_id, game_id, status) VALUES (42, 1, ?)', (status,))
                message = NS(text='❌Отменить запись', from_user=NS(id=42), answer=AsyncMock())
                await self.ns['menu_handler'](message, self.state)
                self.assertIn('пока не записан', message.answer.call_args.args[0])

    async def test_delete_back_accepts_both_keyboard_versions(self):
        for label in ['🔙Назад', '🔙 Назад']:
            message = NS(text=label, answer=AsyncMock())
            await self.ns['delete_game_handler'](message, self.state)
            self.assertIn('вернулся', message.answer.call_args.args[0])
            self.state.set_state.assert_awaited_with('admin_menu')

    def test_manual_members_have_distinct_ids_and_no_platform_account(self):
        first = self.ns['create_manual_user']('A', 'B', 'C')
        second = self.ns['create_manual_user']('D', 'E', 'F')
        self.assertNotEqual(first, second)
        self.assertEqual(detect_platform_by_user_id(first), 'manual')
        self.assertEqual(self.db.execute('SELECT platform, platform_user_id FROM users WHERE user_id = ?', (first,)).fetchone(), ('manual', None))
        self.assertFalse(self.db.in_transaction, 'INSERT RETURNING must be committed')

    async def test_manual_members_never_receive_network_messages(self):
        with self.assertRaises(ValueError):
            await self.ns['send_text_to_user'](MANUAL_ID_START, 'hello')
        self.ns['bot'].send_message.assert_not_awaited()
        self.ns['vk_api_client'].messages.send.assert_not_called()
        link = self.ns['build_profile_link']('manual', None)
        self.assertNotIn('://', link)

    async def test_date_entry_preserves_explicit_year(self):
        self.ns.update(ReplyKeyboardBuilder=Mock, GAME_TYPES=['game'])
        message = NS(text='01.01.2027', answer=AsyncMock())
        await self.ns['process_add_game_date_text'](message, self.state)
        stored = self.state.update_data.call_args.kwargs['game_date']
        self.assertEqual(self.ns['parse_game_date'](stored), datetime.date(2027, 1, 1))

    async def test_calendar_cannot_enter_admin_flow_for_regular_user(self):
        callback = self.callback('calendar')
        await self.ns['process_simple_calendar'](callback, NS(), self.state)
        self.assertTrue(callback.answer.call_args.kwargs['show_alert'])
        self.state.set_state.assert_not_awaited()

    async def test_manual_reminder_is_not_sent_or_counted(self):
        self.ns['execute_query'] = Mock(return_value=[(1, 'game', '2099-01-01')])
        count = await self.ns['send_game_reminders']([MANUAL_ID_START], [1])
        self.assertEqual(count, 0)
        self.ns['bot'].send_message.assert_not_awaited()
        self.ns['vk_api_client'].messages.send.assert_not_called()


class MigrationTests(unittest.TestCase):
    def migrate(self, games, year='2026', duplicate=False):
        cursor = MagicMock()
        cursor.fetchall.return_value = games

        def fetchone():
            sql = cursor.execute.call_args.args[0]
            if "key = 'legacy_game_year'" in sql:
                return (year,)
            return (1,) if duplicate else None

        cursor.fetchone.side_effect = fetchone
        connection = MagicMock()
        connection.cursor.return_value = cursor
        ns = load_functions('database.py', {
            'datetime': datetime, 'logging': logging, 'parse_date': parse_date,
            'stored_game_date': stored_game_date,
        })
        ns['get_connection'] = lambda: connection
        self.assertEqual(ns['init_db'](), int(year))
        connection.commit.assert_called_once()
        return [call.args[1] for call in cursor.execute.call_args_list
                if call.args[0].startswith('UPDATE games SET game_date')]

    def test_only_yearless_dates_are_migrated(self):
        updates = self.migrate([(1, 'game', 'Пт 01.01'), (2, 'game', '01.01.2027')])
        self.assertEqual(updates, [('Чт 01.01.2026', 1)])

    def test_restarting_in_another_year_preserves_anchor(self):
        updates = self.migrate([(1, 'game', '01.01')], year='2025')
        self.assertEqual(updates, [('Ср 01.01.2025', 1)])

    def test_duplicate_does_not_abort_startup(self):
        with self.assertLogs(level='WARNING'):
            self.assertEqual(self.migrate([(1, 'game', '01.01')], duplicate=True), [])


class DateAndIdentityTests(unittest.TestCase):
    def test_full_date_does_not_change_with_default_year(self):
        value = stored_game_date(datetime.date(2027, 1, 1))
        self.assertEqual(parse_date(value, 2030), datetime.date(2027, 1, 1))

    def test_legacy_year_is_explicit(self):
        self.assertEqual(parse_date('Пт 01.01', 2026), datetime.date(2026, 1, 1))

    def test_leap_day_and_invalid_dates(self):
        self.assertEqual(parse_date('29.02', 2028), datetime.date(2028, 2, 29))
        self.assertIsNone(parse_date('29.02', 2027))
        self.assertIsNone(parse_date('31.02.2027'))
        self.assertIsNone(parse_date(None))

    def test_network_id_namespaces_remain_unchanged(self):
        self.assertEqual(detect_platform_by_user_id(42), 'telegram')
        self.assertEqual(detect_platform_by_user_id(-42), 'vk')
        self.assertEqual(detect_platform_by_user_id(MANUAL_ID_START - 1), 'manual')


if __name__ == '__main__':
    unittest.main()
