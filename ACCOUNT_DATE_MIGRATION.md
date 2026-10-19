# Updating player account creation dates

The one-time migration in `fix_account_creation_dates.py` sets each player's
`users.created_at` to the calendar date of their latest registration event in
`registrations.registered_at`. Players with no registration receive
`2026-10-20`.

Run it from the bot service in Railway, where `DATABASE_URL` is already
configured:

```sh
python fix_account_creation_dates.py
python fix_account_creation_dates.py --apply
```

The first command only reports how many rows would be updated. The second
performs the update in a transaction. Running it again is safe and recalculates
the dates from the current registration records.
