import logging
import argparse
import os
import datetime
import pprint
from typing import Generator
from urllib.parse import urlparse, parse_qs
import re

from strava import StravaClient
from storage import SqliteStorage
from google_sheets import GoogleSheetClient
import pandas

log = logging.getLogger()


def cl_acquire_token(user_id: str) -> tuple[int, str, dict]:
    """
    The command-line flow for acquiring a Strava access token.
    :param user_id: The local user_id. May be different than Strava athlete_id
    :return:
    """
    url = sclient.request_access_url(scope=["read_all", "activity:read_all"], state=user_id)
    print(
        "Visit the URL below and authorize this app to access your account. Your browser will be redirected to a URL.")
    print("************")
    print(url)
    print("************")
    redirect_url = input("Copy that URL here:")
    log.info(f"Callback URL received: {redirect_url}")
    handle_access_response(redirect_url)
    return storage.lookup_access_token(user_id)


def handle_access_response(url: str) -> None:
    """
    Parse the redirect URL response and initiate the token exchange. Raise exception if error=access_denied.
    Otherwise return the authorization_code
    :param url: the call-back URL
    :return:
    """
    global sclient, token_store
    get_params = parse_qs(urlparse(url).query)
    if get_params.get("error", None) == "access_denied":
        log.error(f"Received access_denied in redirect_url: {url}")
        raise Exception(f"Received access_denied in redirect_url: {url}")
    log.info(f"Received get params: {get_params}")
    token_data = sclient.exchange_token(get_params['code'][0])
    user_id = get_params["state"][0]
    storage.store_token(user_id, token_data['athlete']['id'], token_data, get_params["scope"][0])


def refresh_token(user_id: str) -> tuple[int, str, dict]:
    """
    Do a refresh routine for this athlete. There should be a refresh token available, else Exceptions will occur.
    :param user_id: The strava athlete id
    :return: the new access_token
    """
    log.info("Refreshing token...")
    (athlete_id, scope, refresh_token) = storage.lookup_refresh_token(user_id)
    token_data = sclient.refresh_token(refresh_token['refresh_token'])
    storage.store_token(user_id, athlete_id, token_data, scope)
    return storage.lookup_access_token(user_id)


def get_timeline_events() -> Generator[dict, None, None]:
    gsheet_client = GoogleSheetClient(os.getenv("GOOGLE_TOKEN_FILE"), os.getenv("GOOGLE_CREDS_FILE"))
    gsheet_client.authenticate()
    for record in gsheet_client.read_from_sheet(os.getenv("GOOGLE_SPREADSHEET_ID"), "Blad1"):
        if record["Event"]:
            event = {"type": "event"}
            event["timestamp"] = datetime.datetime.strptime(f"{record['Day']} {record['Time']}", "%d-%m-%Y %H:%M")
            event["name"] = record["Event"]
            event["description"] = record["description"]
            yield event


def get_strava_activities(user_id: str) -> Generator[dict, None, None]:
    selection = ["id", "name", "distance", "moving_time",
                 "elapsed_time", "total_elevation_gain", "type",
                 "start_date", "start_latlng", "average_speed",
                 "average_temp", "average_cadence", "calories",
                 "description", "average_heartrate", "max_heartrate",
                 "suffer_score"]
    for record in storage.get_activities(user_id):
        priv_note = record.get("private_note", "")
        pains = {}
        matches = None
        try:
            if priv_note:
                matches = re.findall(r"(.+):(.+)", priv_note)
            if matches:
                pains = {f"pains.{m[0].lower()}": float(m[1]) for m in matches}
        except Exception as err:
            log.error(f"Exception during activities parsing: {err}")
        activity = {k: v for k, v in record.items() if k in selection}
        activity.update(pains)
        yield activity


def update_strava_activities() -> None:
    token_data = storage.lookup_access_token(args.id)
    if not token_data:
        log.info(f"No access token found for {args.id}.")
        token_data = cl_acquire_token(args.id)
    else:
        (athlete_id, scope, token) = token_data
        log.info(f"Access token found for {args.id}: {token}")
        if datetime.datetime.now().timestamp() > token["expires_at"]:
            log.info(f"Access token expired.")
            token_data = refresh_token(args.id)
    (athlete_id, scope, token) = token_data
    athlete_data = sclient.get_athlete_info(token["access_token"])
    print(f"Here is your athlete:\n{pprint.pformat(athlete_data)}")
    storage.store_athlete(athlete_data, args.id)
    print("Refreshing activities. These are your new activities:")
    with storage:
        for activity in sclient.get_activities(token["access_token"]):
            cached_activity = storage.get_activity(activity["id"], args.id)
            if not cached_activity:
                log.info(f"Activity '{activity['name']}' ({activity['id']}) not found in cache.")
                full_activity = sclient.get_activity_detailed(token["access_token"], activity['id'])
                storage.store_activity(full_activity, args.id)
                print(f"'{activity['name']}' ({activity['id']})")
            else:
                log.info(f"Activity '{activity['name']}' ({activity['id']}) found in cache!")


if __name__ == "__main__":
    argparser = argparse.ArgumentParser(description="Run straavaldi.")
    argparser.add_argument("-u", "--update", required=False, action="store_true",
                           help="Update the Strava activites cache.")
    argparser.add_argument("-v", "--verbose", required=False, help="Enable verbose logging.")
    argparser.add_argument("-i", "--id", required=False, default=os.getenv("DEFAULT_ACCOUNT_ID"),
                           help=f"Account id. Default: {os.getenv('DEFAULT_ACCOUNT_ID')}")
    args = argparser.parse_args()
    loglevel = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
                        level=loglevel)

    sclient = StravaClient(client_id=os.getenv("STRAVA_CLIENT_ID"), client_secret=os.getenv("STRAVA_CLIENT_SECRET"))
    storage = SqliteStorage(os.getenv("STORAGE_DB"), os.getenv("STORAGE_SCHEMA_FILE"))

    if args.update:
        update_strava_activities()
    else:
        dataframe = pandas.DataFrame.from_records(get_strava_activities(args.id))
        print(dataframe.to_csv())
