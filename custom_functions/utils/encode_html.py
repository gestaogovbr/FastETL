import html
import unicodedata

def replace_to_html_encode(text: str) -> str:
    "Replace accented characters with html entity equivalents."

    SYMBOLS = '§ª°º˚"'

    for entity, char in html.entities.html5.items():
        if unicodedata.category(char[0]) == 'Ll' or char[0] in SYMBOLS:

            text = text.replace(char, f'&{entity};')

    return text

