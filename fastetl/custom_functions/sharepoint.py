"""
Utility functions for accessing SharePoint via Microsoft Graph API.

Replaces the ACS (Azure Control Service) method discontinued in April 2026
with the modern flow: Entra ID + MSAL + Microsoft Graph with Sites.Selected
permission.

Usage:
    from fastetl.custom_functions.sharepoint import (
        get_sharepoint_client,
        list_sharepoint_files,
        upload_to_sharepoint,
        download_from_sharepoint,
    )
"""

import os
import json
import logging

import msal
import requests
from airflow.hooks.base import BaseHook


def get_sharepoint_client(conn_id: str, site_host: str, site_name: str):
    """
    Authenticates to SharePoint via Microsoft Graph API and returns
    the token, site_id, and drive_id ready for use.

    The Airflow Connection must contain the following fields in the extra field:
        CLIENT_ID, CLIENT_SECRET, and TENANT_ID from the app registration
        created in the Entra ID of the target tenant.

    Args:
        conn_id: Airflow Connection ID containing the credentials.
        site_host: SharePoint hostname, e.g.: colaboragov.sharepoint.com
        site_name: Site name after /sites/, e.g.: CDATASEGES

    Returns:
        tuple: (token, site_id, drive_id)

    Raises:
        ValueError: if the token cannot be obtained successfully.
        HTTPError: if the site or drive are not found.
    """
    hook = BaseHook.get_connection(conn_id)
    client = json.loads(hook.extra)

    app = msal.ConfidentialClientApplication(
        client_id=client["CLIENT_ID"],
        client_credential=client["CLIENT_SECRET"],
        authority=f"https://login.microsoftonline.com/{client['TENANT_ID']}"
    )
    result = app.acquire_token_for_client(
        scopes=["https://graph.microsoft.com/.default"]
    )
    if "access_token" not in result:
        raise ValueError(f"Failed to obtain token: {result.get('error_description')}")

    token = result["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Get SITE_ID
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_host}:/sites/{site_name}",
        headers=headers
    )
    r.raise_for_status()
    site_id = r.json()["id"]

    # Get DRIVE_ID (Documents library)
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
        headers=headers
    )
    r.raise_for_status()
    drives = r.json()["value"]
    drive_id = next(d["id"] for d in drives if d["name"] == "Documents")

    return token, site_id, drive_id


def list_sharepoint_files(
    conn_id: str,
    site_host: str,
    site_name: str,
    folder: str,
) -> list:
    """
    Lists files inside a SharePoint folder.

    Args:
        conn_id: Airflow Connection ID containing the credentials.
        site_host: SharePoint hostname, e.g.: colaboragov.sharepoint.com
        site_name: Site name after /sites/, e.g.: CDATASEGES
        folder: Folder path relative to the drive root,
                e.g.: legado or General/2024.
                Do not include Shared Documents.

    Returns:
        list: list of dicts with file metadata.
    """
    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{folder}:/children",
        headers=headers
    )
    r.raise_for_status()
    files = r.json().get("value", [])

    for item in files:
        logging.info("  %s (%s bytes)", item["name"], item.get("size", 0))

    return files


def upload_to_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    local_path: str,
    folder: str,
    delete_after_upload: bool = False,
) -> str:
    """
    Uploads a local file to a SharePoint folder.

    Args:
        conn_id: Airflow Connection ID containing the credentials.
        site_host: SharePoint hostname, e.g.: colaboragov.sharepoint.com
        site_name: Site name after /sites/, e.g.: CDATASEGES
        local_path: Full path of the local file, e.g.: /tmp/file.csv
        folder: Destination folder in SharePoint, e.g.: legado or General/2024.
                Do not include Shared Documents.
        delete_after_upload: If True, removes the local file after a successful
                             upload. Useful for cleaning up temporary files.
                             Default: False.

    Returns:
        str: webUrl of the uploaded file.

    Raises:
        HTTPError: if the upload fails.
        FileNotFoundError: if the local file does not exist.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"File not found: {local_path}")

    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    file_name = os.path.basename(local_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    with open(local_path, "rb") as f:
        r = requests.put(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
            f"/root:/{folder}/{file_name}:/content",
            headers=headers,
            data=f
        )
    r.raise_for_status()
    web_url = r.json().get("webUrl")
    logging.info("Upload successful -> %s", web_url)

    if delete_after_upload:
        os.remove(local_path)
        logging.info("Local file removed -> %s", local_path)

    return web_url


def download_from_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    file_name: str,
    folder: str,
    destination: str = "/tmp",
) -> str:
    """
    Downloads a file from SharePoint to a local folder.

    Args:
        conn_id: Airflow Connection ID containing the credentials.
        site_host: SharePoint hostname, e.g.: colaboragov.sharepoint.com
        site_name: Site name after /sites/, e.g.: CDATASEGES
        file_name: Name of the file in SharePoint, e.g.: Base_Indicadores.xlsx
        folder: Source folder in SharePoint, e.g.: legado or General/2024.
                Do not include Shared Documents.
        destination: Local folder where the file will be saved. Default: /tmp

    Returns:
        str: Full path of the downloaded file.

    Raises:
        HTTPError: if the download fails.
    """
    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
        f"/root:/{folder}/{file_name}:/content",
        headers=headers,
        allow_redirects=True
    )
    r.raise_for_status()

    os.makedirs(destination, exist_ok=True)
    local_path = os.path.join(destination, file_name)
    with open(local_path, "wb") as f:
        f.write(r.content)

    logging.info(
        "Download successful -> %s (%d bytes)",
        local_path,
        os.path.getsize(local_path)
    )
    return local_path