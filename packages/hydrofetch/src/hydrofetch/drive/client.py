"""Thin Google Drive v3 client for hydrofetch.

Authentication uses the OAuth 2.0 installed-app flow.  On first use a browser
window opens for user consent; the resulting token is saved to the path
returned by :func:`hydrofetch.config.get_token_file`.

Subsequent calls refresh the token automatically without user interaction.
"""

from __future__ import annotations

import io
import json
import logging
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

_SCOPES = ["https://www.googleapis.com/auth/drive"]

_FIELDS_LIST = "files(id,name,size,mimeType)"


def _build_service(credentials_file: Path, token_file: Path) -> Any:
    """Return an authenticated Google Drive v3 service resource.

    Runs the OAuth browser flow once if no saved token is found.
    """
    from google.auth.transport.requests import Request  # pylint: disable=import-outside-toplevel
    from google.oauth2.credentials import Credentials  # pylint: disable=import-outside-toplevel
    from google_auth_oauthlib.flow import InstalledAppFlow  # pylint: disable=import-outside-toplevel
    from googleapiclient.discovery import build  # pylint: disable=import-outside-toplevel

    creds: Credentials | None = None

    if token_file.is_file():
        with token_file.open(encoding="utf-8") as fh:
            token_data = json.load(fh)
        creds = Credentials.from_authorized_user_info(token_data, _SCOPES)

    if creds is None or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            log.debug("Refreshing Drive token")
            creds.refresh(Request())
        else:
            log.info("Starting Drive OAuth browser flow")
            flow = InstalledAppFlow.from_client_secrets_file(str(credentials_file), _SCOPES)
            creds = flow.run_local_server(port=0)

        token_file.parent.mkdir(parents=True, exist_ok=True)
        with token_file.open("w", encoding="utf-8") as fh:
            json.dump(json.loads(creds.to_json()), fh, indent=2)
        log.debug("Saved Drive token to %s", token_file)

    return build("drive", "v3", credentials=creds, cache_discovery=False)


class DriveClient:
    """Minimal Google Drive client for hydrofetch export artefacts.

    Args:
        credentials_file: Path to the OAuth client-secrets JSON from Google Cloud Console.
        token_file: Path where the saved token is stored / loaded.
    """

    def __init__(self, credentials_file: Path, token_file: Path) -> None:
        self._credentials_file = credentials_file
        self._token_file = token_file
        self._service: Any = None

    # ------------------------------------------------------------------
    # Connection management
    # ------------------------------------------------------------------

    def connect(self) -> None:
        """Build (or rebuild) the Drive service.  Called lazily on first use."""
        self._service = _build_service(self._credentials_file, self._token_file)
        log.info("Drive service connected")

    def _svc(self) -> Any:
        """Return the service, connecting on first access."""
        if self._service is None:
            self.connect()
        return self._service

    # ------------------------------------------------------------------
    # File discovery
    # ------------------------------------------------------------------

    def find_files_by_name_prefix(
        self, prefix: str, folder_name: str | None = None
    ) -> list[dict]:
        """Return Drive file metadata dicts whose names start with *prefix*.

        Args:
            prefix: File name prefix to match (case-sensitive).
            folder_name: Optional Drive folder name to restrict the search.

        Returns:
            List of ``{"id": ..., "name": ..., "size": ..., "mimeType": ...}`` dicts.
        """
        query_parts = [f"name contains '{prefix}'", "trashed = false"]
        if folder_name:
            folder_id = self._resolve_folder_id(folder_name)
            if folder_id:
                query_parts.append(f"'{folder_id}' in parents")

        query = " and ".join(query_parts)
        log.debug("Drive query: %s", query)

        results: list[dict] = []
        page_token: str | None = None
        while True:
            kwargs: dict[str, Any] = {
                "q": query,
                "fields": _FIELDS_LIST,
                "pageSize": 100,
            }
            if page_token:
                kwargs["pageToken"] = page_token
            resp = self._svc().files().list(**kwargs).execute()
            results.extend(resp.get("files", []))
            page_token = resp.get("nextPageToken")
            if not page_token:
                break

        log.debug("Found %d file(s) matching prefix %r", len(results), prefix)
        return results

    def _resolve_folder_id(self, folder_name: str) -> str | None:
        """Return the Drive folder ID for *folder_name*, or None if not found."""
        resp = (
            self._svc()
            .files()
            .list(
                q=(
                    f"name = '{folder_name}' and "
                    "mimeType = 'application/vnd.google-apps.folder' and "
                    "trashed = false"
                ),
                fields="files(id,name)",
                pageSize=1,
            )
            .execute()
        )
        files = resp.get("files", [])
        if not files:
            log.warning("Drive folder %r not found; searching without folder restriction", folder_name)
            return None
        return files[0]["id"]

    # ------------------------------------------------------------------
    # Download
    # ------------------------------------------------------------------

    def download_file(self, file_id: str, dest: Path) -> None:
        """Download the Drive file with *file_id* to *dest*.

        Args:
            file_id: Drive file ID.
            dest: Local path to write to (parent directories must exist).
        """
        from googleapiclient.http import MediaIoBaseDownload  # pylint: disable=import-outside-toplevel

        request = self._svc().files().get_media(fileId=file_id)
        dest.parent.mkdir(parents=True, exist_ok=True)
        with dest.open("wb") as fh:
            downloader = MediaIoBaseDownload(io.FileIO(fh.name, "wb"), request)
            done = False
            while not done:
                status, done = downloader.next_chunk()
                if status:
                    log.debug(
                        "Downloading %s → %s  %.0f%%",
                        file_id,
                        dest.name,
                        status.progress() * 100,
                    )
        log.info("Downloaded Drive file %s to %s", file_id, dest)

    # ------------------------------------------------------------------
    # Delete
    # ------------------------------------------------------------------

    def delete_file(self, file_id: str) -> None:
        """Permanently delete a Drive file by ID.

        Args:
            file_id: Drive file ID.
        """
        self._svc().files().delete(fileId=file_id).execute()
        log.info("Deleted Drive file %s", file_id)

    # ------------------------------------------------------------------
    # Factory
    # ------------------------------------------------------------------

    @classmethod
    def from_config(cls) -> "DriveClient":
        """Build a :class:`DriveClient` from environment configuration."""
        from hydrofetch.config import (  # pylint: disable=import-outside-toplevel
            get_credentials_file,
            get_token_file,
        )

        return cls(
            credentials_file=get_credentials_file(),
            token_file=get_token_file(),
        )


__all__ = ["DriveClient"]
