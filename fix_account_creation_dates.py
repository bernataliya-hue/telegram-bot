"""Set each player's account creation date from their latest game registration."""

import argparse

import database


FALLBACK_DATE = "2026-10-20"


def update_account_creation_dates(apply: bool = False) -> tuple[int, int]:
    """Return (players with registrations, players without registrations).

    By default this only reports the number of rows that would be updated.
    Pass ``apply=True`` to perform the update in a single transaction.
    """
    conn = database.get_connection()
    try:
        with conn:
            with conn.cursor() as cursor:
                cursor.execute(
                    """
                    SELECT
                        count(*) FILTER (WHERE last_registered_on IS NOT NULL),
                        count(*) FILTER (WHERE last_registered_on IS NULL)
                    FROM (
                        SELECT u.user_id, max(r.registered_at::date) AS last_registered_on
                        FROM users u
                        LEFT JOIN registrations r ON r.user_id = u.user_id
                        GROUP BY u.user_id
                    ) player_dates
                    """
                )
                with_registrations, without_registrations = cursor.fetchone()

                if apply:
                    cursor.execute(
                        """
                        UPDATE users u
                        SET created_at = COALESCE(
                            (
                                SELECT max(r.registered_at::date)::timestamp
                                FROM registrations r
                                WHERE r.user_id = u.user_id
                            ),
                            %s::date::timestamp
                        )
                        """,
                        (FALLBACK_DATE,),
                    )

        return with_registrations, without_registrations
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--apply",
        action="store_true",
        help="write the calculated dates to the users table",
    )
    args = parser.parse_args()

    with_registrations, without_registrations = update_account_creation_dates(args.apply)
    action = "Updated" if args.apply else "Would update"
    print(
        f"{action} {with_registrations + without_registrations} players: "
        f"{with_registrations} from latest registration date, "
        f"{without_registrations} set to {FALLBACK_DATE}."
    )
    if not args.apply:
        print("No changes written. Add --apply to update the database.")


if __name__ == "__main__":
    main()
