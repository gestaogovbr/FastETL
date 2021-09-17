"""
Funções para manipulação e geração de strings para usos comuns em várias
DAGs
"""

from datetime import datetime
import inspect
import textwrap

import slugify as sl
from markdown import markdown

def slugify_column_names(column_name: str):
    """
    Format column name to canonic style.
    Ex.: column_name_without_accent_lower_case_and_without_space

    Args:
        - column_name (str): Column name.

    Return:
        - results (str): Formatted column name.
    """

    if column_name:
        return sl.slugify(column_name).replace("-","_")
    else:
        return ""

def convert_gsheets_str_to_datetime(datetime_str):
    """
    Transforma string data para datetime.datetime

    Args:
        - datetime_str (str): String de data no formato %Y-%m-%dT%H:%M:%S.%fZ.
        Exemplo: '2020-06-10T15:45:18.525Z'

    Return:
        - datetime.datetime da string
    """

    return datetime.strptime(datetime_str, '%Y-%m-%dT%H:%M:%S.%fZ')

def construct_vocative_names_from_emails(emails_list: list):
    """
    Constrói uma string com os primeiros nomes de cada email para ser
    utilizado no vocativo no corpo de um email. Segue o formato: "Fulano,
    Sicrano, Beltrano e Zezinho".

    Args:
        - emails_list (list): Uma lista de strings contendo emails no
        formato nome.ultimonome@tantofaz.como.tanto.fez

    Return:
        - string para ser utilizada no vocativo do corpo do email
    """
    captalized_names_list = [m.split(".")[0].capitalize() for m in emails_list]
    if len(captalized_names_list) > 1:
        vocative_names = ", ".join(captalized_names_list[:-1])
        vocative_names += " e " + captalized_names_list[-1]
    else:
        vocative_names = captalized_names_list[0]

    return vocative_names

def imarkdown_to_html(text: str) -> str:
    """Transforms indented markdown text to html.
    """
    return markdown(inspect.cleandoc(textwrap.dedent(text)))
