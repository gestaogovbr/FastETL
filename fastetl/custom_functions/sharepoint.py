"""
Funções utilitárias para acesso ao SharePoint via Microsoft Graph API.

Substitui o método ACS (descontinuado em abril de 2026) pelo fluxo moderno:
Entra ID + MSAL + Microsoft Graph com permissão Sites.Selected.

Uso:
    from fastetl.custom_functions.sharepoint import (
        get_sharepoint_client,
        upload_to_sharepoint,
        upload_tmp_to_sharepoint,
        download_from_sharepoint,
        listar_arquivos_sharepoint,
    )
"""

import os
import json
import logging

import msal
import requests
from airflow.hooks.base import BaseHook
from airflow.models import Variable


def get_sharepoint_client(conn_id: str, site_host: str, site_name: str):
    """
    Autentica no SharePoint via Microsoft Graph API e retorna
    token, site_id e drive_id prontos para uso.

    A Airflow Connection informada deve conter no campo extra:
        CLIENT_ID, CLIENT_SECRET e TENANT_ID do app registration
        criado no Entra ID do tenant colaboragov.

    Args:
        conn_id: ID da Airflow Connection com as credenciais.
        site_host: hostname do SharePoint, ex: colaboragov.sharepoint.com
        site_name: nome do site, ex: CDATASEGES

    Returns:
        tuple: (token, site_id, drive_id)

    Raises:
        ValueError: se o token não for obtido com sucesso.
        HTTPError: se o site ou drive não forem encontrados.
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
        raise ValueError(f"Erro ao obter token: {result.get('error_description')}")

    token = result["access_token"]
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    # Descobre SITE_ID
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_host}:/sites/{site_name}",
        headers=headers
    )
    r.raise_for_status()
    site_id = r.json()["id"]

    # Descobre DRIVE_ID (biblioteca Documents)
    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives",
        headers=headers
    )
    r.raise_for_status()
    drives = r.json()["value"]
    drive_id = next(d["id"] for d in drives if d["name"] == "Documents")

    return token, site_id, drive_id


def listar_arquivos_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    pasta: str,
) -> list:
    """
    Lista os arquivos dentro de uma pasta no SharePoint.

    Args:
        conn_id: ID da Airflow Connection com as credenciais.
        site_host: hostname do SharePoint, ex: colaboragov.sharepoint.com
        site_name: nome do site, ex: CDATASEGES
        pasta: caminho da pasta relativo à raiz do drive,
               ex: legado ou General/2024. Nao incluir Shared Documents.

    Returns:
        list: lista de dicts com os metadados dos arquivos.
    """
    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    headers = {"Authorization": f"Bearer {token}", "Accept": "application/json"}

    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}/root:/{pasta}:/children",
        headers=headers
    )
    r.raise_for_status()
    arquivos = r.json().get("value", [])

    for item in arquivos:
        logging.info("  %s (%s bytes)", item["name"], item.get("size", 0))

    return arquivos


def upload_to_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    local_path: str,
    pasta: str,
    delete_after_upload: bool = False,
) -> str:
    """
    Faz upload de um arquivo local para uma pasta no SharePoint.

    Args:
        conn_id: ID da Airflow Connection com as credenciais.
        site_host: hostname do SharePoint, ex: colaboragov.sharepoint.com
        site_name: nome do site, ex: CDATASEGES
        local_path: caminho completo do arquivo local, ex: /tmp/arquivo.csv
        pasta: pasta de destino no SharePoint, ex: legado ou General/2024.
               Nao incluir Shared Documents.
        delete_after_upload: se True, remove o arquivo local apos o upload
                             com sucesso. Util para limpeza de pastas
                             temporarias do Airflow. Default: False.

    Returns:
        str: webUrl do arquivo enviado.

    Raises:
        HTTPError: se o upload falhar.
        FileNotFoundError: se o arquivo local nao existir.
    """
    if not os.path.exists(local_path):
        raise FileNotFoundError(f"Arquivo nao encontrado: {local_path}")

    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    file_name = os.path.basename(local_path)
    headers = {
        "Authorization": f"Bearer {token}",
        "Content-Type": "application/octet-stream"
    }

    with open(local_path, "rb") as f:
        r = requests.put(
            f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
            f"/root:/{pasta}/{file_name}:/content",
            headers=headers,
            data=f
        )
    r.raise_for_status()
    web_url = r.json().get("webUrl")
    logging.info("Upload OK -> %s", web_url)

    if delete_after_upload:
        os.remove(local_path)
        logging.info("Arquivo local removido -> %s", local_path)

    return web_url


def upload_tmp_to_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    file_name: str,
    pasta: str,
) -> str:
    """
    Faz upload de um arquivo localizado na pasta temporária do Airflow
    (variável path_tmp) para uma pasta no SharePoint, removendo o arquivo
    local após o upload com sucesso.

    Ideal para pipelines onde o arquivo é gerado em path_tmp e deve ser
    enviado ao SharePoint sem deixar resíduos no worker do Airflow.

    Args:
        conn_id: ID da Airflow Connection com as credenciais.
        site_host: hostname do SharePoint, ex: colaboragov.sharepoint.com
        site_name: nome do site, ex: CDATASEGES
        file_name: nome do arquivo dentro de path_tmp, ex: arquivo.csv
        pasta: pasta de destino no SharePoint, ex: legado ou General/2024.
               Nao incluir Shared Documents.

    Returns:
        str: webUrl do arquivo enviado.

    Raises:
        HTTPError: se o upload falhar.
        FileNotFoundError: se o arquivo nao existir em path_tmp.
    """
    local_path = os.path.join(Variable.get("path_tmp"), file_name)
    return upload_to_sharepoint(
        conn_id=conn_id,
        site_host=site_host,
        site_name=site_name,
        local_path=local_path,
        pasta=pasta,
        delete_after_upload=True
    )


def download_from_sharepoint(
    conn_id: str,
    site_host: str,
    site_name: str,
    file_name: str,
    pasta: str,
    destino: str = "/tmp",
) -> str:
    """
    Faz download de um arquivo do SharePoint para uma pasta local.

    Args:
        conn_id: ID da Airflow Connection com as credenciais.
        site_host: hostname do SharePoint, ex: colaboragov.sharepoint.com
        site_name: nome do site, ex: CDATASEGES
        file_name: nome do arquivo no SharePoint, ex: Base_Indicadores.xlsx
        pasta: pasta de origem no SharePoint, ex: legado ou General/2024.
               Nao incluir Shared Documents.
        destino: pasta local onde o arquivo sera salvo. Default: /tmp

    Returns:
        str: caminho completo do arquivo baixado.

    Raises:
        HTTPError: se o download falhar.
    """
    token, site_id, drive_id = get_sharepoint_client(conn_id, site_host, site_name)
    headers = {
        "Authorization": f"Bearer {token}",
        "Accept": "application/json"
    }

    r = requests.get(
        f"https://graph.microsoft.com/v1.0/sites/{site_id}/drives/{drive_id}"
        f"/root:/{pasta}/{file_name}:/content",
        headers=headers,
        allow_redirects=True
    )
    r.raise_for_status()

    os.makedirs(destino, exist_ok=True)
    local_path = os.path.join(destino, file_name)
    with open(local_path, "wb") as f:
        f.write(r.content)

    logging.info(
        "Download OK -> %s (%d bytes)",
        local_path,
        os.path.getsize(local_path)
    )
    return local_path