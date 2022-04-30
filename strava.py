import requests
import urllib.parse
import logging
import pprint
from typing import Generator

log = logging.getLogger(__name__)

STRAVA_API_URL = "https://www.strava.com/api/v3"
PAGE_SIZE = 50


class StravaClient:
    def __init__(self, client_id: str, client_secret: str) -> None:
        self.client_id = client_id
        self.client_secret = client_secret

    def request_access_url(self, redirect_uri="https://localhost", approval_prompt="auto", scope=[], state=None) -> str:
        """
        Return the authorization URL. This URL can be provided to the user to visit or automatically redirected to
        by the calling function.
        :param redirect_uri:
        :param approval_prompt:
        :param scope:
        :param state:
        :return: str: the full URL for the client to access
        """
        auth_url = "https://www.strava.com/oauth/authorize?"
        params = {
            "client_id": self.client_id,
            "redirect_uri": redirect_uri,
            "response_type": "code",
            "approval_prompt": approval_prompt,
            "scope": ",".join(scope),
            "state": state
        }
        return auth_url + urllib.parse.urlencode(params)

    def exchange_token(self, authorization_code: str) -> dict:
        """
        Perform the token exchange
        :param authorization_code: the authorization code from the access request.
        :return: The token_data received from Strava.
        """
        url = "https://www.strava.com/api/v3/oauth/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "code": authorization_code,
            "grant_type": "authorization_code"
        }
        log.info(f"Requesting token_exchange @{url}...")
        log.debug(f"With data:\n{pprint.pprint(params)}")
        resp = requests.post(url, data=params)
        resp.raise_for_status()
        resp_data = resp.json()
        log.info(f"Response received:\n{pprint.pformat(resp_data)}")
        return resp_data

    def refresh_token(self, refresh_token: str) -> dict:
        """
        Refresh a short lived access token.
        :param refresh_token: the refresh token
        :return: the new token_data dict
        """
        url = f"{STRAVA_API_URL}/oauth/token"
        params = {
            "client_id": self.client_id,
            "client_secret": self.client_secret,
            "grant_type": "refresh_token",
            "refresh_token": refresh_token
        }
        log.info(f"Requesting token refresh @{url}...")
        log.debug(f"With data:\n{pprint.pprint(params)}")
        resp = requests.post(url, data=params)
        resp.raise_for_status()
        resp_data = resp.json()
        log.info(f"Response received:\n{pprint.pformat(resp_data)}")
        return resp_data

    def get_athlete_info(self, token: str) -> dict:
        url = f"{STRAVA_API_URL}/athlete"
        headers = {"Authorization": f"Bearer {token}"}
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def get_activities(self, token: str) -> Generator[dict, None, None]:
        url = f"{STRAVA_API_URL}/athlete/activities"
        headers = {"Authorization": f"Bearer {token}"}
        params = {}
        for activity in self._paginated_results(url, headers, params):
            yield self.get_activity_detailed(token, activity["id"])

    def get_activity_detailed(self, token: str, activity_id: str) -> dict:
        url = f"{STRAVA_API_URL}/activities/{activity_id}"
        headers = {"Authorization": f"Bearer {token}"}
        params = {
            "include_all_efforts": "False"
        }
        resp = requests.get(url, headers=headers)
        resp.raise_for_status()
        return resp.json()

    def _paginated_results(self, url: str, headers: dict, params: dict) -> Generator[dict, None, None]:
        page_num = 1
        params["per_page"] = PAGE_SIZE
        while True:
            params.update({"page": page_num})
            resp = requests.get(url, headers=headers, params=params)
            resp.raise_for_status()
            resp_data = resp.json()
            if not resp_data:
                log.info("Empty response received. Must be no more pages..")
                break
            for record in resp_data:
                log.debug(f"Retrieved result: {pprint.pformat(record)}")
                yield record
            page_num += 1

