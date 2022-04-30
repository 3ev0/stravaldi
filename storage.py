import sqlite3
import os.path
from typing import Generator, Tuple
import logging
import datetime
import json

log = logging.getLogger(__name__)


class SqliteStorage:
    def __init__(self, db_file: str, schema_file: str) -> None:
        self.db_file = db_file
        self._cur = None
        if not os.path.exists(self.db_file):
            log.info(f"Database file {self.db_file} does not exist")
            self._init_database(schema_file)

    def __repr__(self) -> str:
        return f"<SqliteStorage db_file={self.db_file}>"

    def __enter__(self) -> None:
        self._con = sqlite3.connect(self.db_file)
        self._con.row_factory = sqlite3.Row
        self._cur = self._con.cursor()
        log.debug("Context opened.")

    def __exit__(self, *args) -> None:
        self._con.commit()
        self._con.close()
        self._cur = None
        log.debug("Context closed.")

    def _init_database(self, schema_file: str) -> None:
        """
        Create the database schema.
        """
        con = sqlite3.connect(self.db_file)
        cur = con.cursor()
        with con:
            cur.executescript(open(schema_file, "r").read())
        log.info("Database initialized.")

    def _get_curs(self):
        if not self._cur:
            con = sqlite3.connect(self.db_file)
            con.row_factory = sqlite3.Row
            cur = con.cursor()
        else:
            cur = self._cur
        return cur

    def _close_curs(self, cur):
        if not self._cur:
            cur.connection.commit()
            cur.connection.close()

    def store_activity(self, activity: dict, user_id: str) -> str:
        cur = self._get_curs()
        values = (activity["id"],
                  activity.get("private_note", None),
                  activity.get("description", None),
                  activity.get("name", None),
                  activity.get("type", None),
                  user_id,
                  datetime.datetime.now().timestamp(),
                  json.dumps(activity))
        cur.execute("INSERT INTO activities (id, private_note, description, name, type, user_id, last_updated, raw) "
                        "VALUES (?, ?, ?, ?, ?, ?, ?, ?)"
                        , values)
        log.debug(f"Activity inserted @{cur.lastrowid}: '{activity['name']}' ({activity['id']})")
        self._close_curs(cur)
        return activity["id"]

    def store_athlete(self, athlete: dict, user_id: str) -> str:
        cur = self._get_curs()
        values = (athlete["id"], user_id, datetime.datetime.now().timestamp(), json.dumps(athlete))
        cur.execute("INSERT INTO athletes (athlete_id, user_id, last_updated, raw) VALUES (?, ?, ?, ?)"
                        "ON CONFLICT(athlete_id) DO UPDATE SET user_id=excluded.user_id, "
                        "last_updated=excluded.last_updated,"
                        "raw=excluded.raw", values)
        log.info(f"Athlete inserted/updated in row {cur.lastrowid}: athlete_id {athlete['id']}")
        self._close_curs(cur)
        return athlete["id"]

    def get_activities(self, user_id: str) -> Generator[tuple[dict, int], None, None]:
        """
        Generator for iterating over the stored activities.
        :return:
        """

        cur = self._get_curs()
        for row in cur.execute("SELECT * FROM activities WHERE user_id=?", user_id):
            yield json.loads(row["raw"]), row["last_updated"]
        self._close_curs(cur)

    def get_activity(self, activity_id: str, user_id: str) -> (dict, int):
        """
        Retrieve an activity based on the id or return None
        :param activity_id:
        :return: (Activity dict, last_updated ts) or None if not found
        """
        cur = self._get_curs()
        cur.execute("SELECT * FROM activities WHERE (id=? AND user_id=?)", (activity_id, user_id))
        row = cur.fetchone()
        self._close_curs(cur)
        if not row:
            return None
        else:
            return json.loads(row["raw"]), row["last_updated"]

    def lookup_access_token(self, user_id: str) -> tuple[int, str, dict]:
        cur = self._get_curs()
        cur.execute("SELECT * FROM access_tokens WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        self._close_curs(cur)
        return row['athlete_id'], row['scope'], json.loads(row['raw'])

    def lookup_refresh_token(self, user_id: str) -> tuple[int, str, dict]:
        cur = self._get_curs()
        cur.execute("SELECT * FROM refresh_tokens WHERE user_id=?", (user_id,))
        row = cur.fetchone()
        if not row:
            return None
        self._close_curs()
        return row['athlete_id'], row['scope'], json.loads(row['raw'])

    def store_token(self, user_id: str, athlete_id: int, token_data: dict, scope: str) -> None:
        """

        :param user_id:
        :param athlete_id;
        :param token_data:
        :param scope:
        :return:
        """
        cur = self._get_curs()
        values = (user_id, athlete_id, token_data['refresh_token'], scope, json.dumps(token_data))
        cur.execute("INSERT INTO refresh_tokens (user_id, athlete_id, token, scope, raw) VALUES (?, ?, ?, ?, ?)",
                    values)
        values = (user_id, athlete_id, token_data['expires_at'], token_data['refresh_token'], scope, json.dumps(token_data))
        cur.execute("INSERT INTO access_tokens (user_id, athlete_id, expires_at, token, scope, raw) "
                    "VALUES (?, ?, ?, ?, ?, ?)", values)
        log.info(f"New tokens stored for {user_id}: {token_data}")
        self._close_curs(cur)