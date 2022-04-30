CREATE TABLE activities
(id INTEGER PRIMARY KEY,
private_note TEXT,
description TEXT,
name TEXT,
type TEXT,
user_id TEXT NOT NULL,
last_updated INTEGER NOT NULL,
raw TEXT NOT NULL);

CREATE TABLE athletes
(athlete_id INTEGER PRIMARY KEY,
user_id TEXT NOT NULL,
last_updated INTEGER NOT NULL,
raw TEXT NOT NULL);

CREATE TABLE refresh_tokens
(user_id TEXT PRIMARY KEY,
athlete_id INTEGER,
token TEXT NOT NULL,
scope TEXT,
raw TEXT);

CREATE TABLE access_tokens
(user_id TEXT PRIMARY KEY,
athlete_id INTEGER,
expires_at INTEGER,
token TEXT,
scope TEXT,
raw TEXT);