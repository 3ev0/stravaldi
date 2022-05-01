import os.path
import logging
from typing import Generator

from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build

log = logging.getLogger(__name__)

SCOPES = ['https://www.googleapis.com/auth/spreadsheets.readonly']

class GoogleSheetClient:

    def __init__(self, token_file: str, creds_file: str) -> None:
        self.token_file = token_file
        self.creds_file = creds_file
        self.creds = None

    def authenticate(self) -> None:
        if os.path.exists(self.token_file):
            self.creds = Credentials.from_authorized_user_file(self.token_file, SCOPES)

        if not self.creds or not self.creds.valid:
            if self.creds and self.creds.expired and self.creds.refresh_token:
                self.creds.refresh(Request())
            else:
                flow = InstalledAppFlow.from_client_secrets_file(
                    self.creds_file, SCOPES)
                self.creds = flow.run_local_server(port=0)
            with open(self.token_file, 'w') as token:
                token.write(self.creds.to_json())
        log.info("Google client authenticated successfully.")

    def read_from_sheet(self, spreadsheet_id: str, sheet_id: str) -> Generator[dict, None, None]:
        log.info("Retrieving rows from spreadsheet")
        service = build('sheets', 'v4', credentials=self.creds)
        sheet = service.spreadsheets()
        result = sheet.values().get(spreadsheetId=spreadsheet_id,
                                    range=sheet_id).execute()
        values = result.get('values', [])

        header = values[0]
        log.info(f"Sheet header (1st row): {header}")
        for row in values[1:]:
            num_cols = len(header) if len(header) < len(row) else len(row)
            yield {header[i]: row[i] for i in range(num_cols)}
