import html
import unicodedata
import logging

def replace_to_html_encode(text: str) -> str:
    "Replace accented characters with html entity equivalents."

    SYMBOLS = '§ª°º˚"'

    logging.warn(
        "If you are using Airflow's send_email, using replace_to_html_encode is"
        ' discouraged. Use the mime_charset="utf-8" parameter instead.'
    )

    for entity, char in html.entities.html5.items():
        if unicodedata.category(char[0]) in ('Ll', 'Lu') or char[0] in SYMBOLS:

            text = text.replace(char, f'&{entity};')

    return text
