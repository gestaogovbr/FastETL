def replace_to_html_encode(text: str) -> str:
    "Replace accented characters with html entity equivalents."
    html_map = (
        ('À', '&Agrave;'),
        ('Á', '&Aacute;'),
        ('Â', '&Acirc;'),
        ('Ã', '&Atilde;'),
        ('Ç', '&Ccedil;'),
        ('È', '&Egrave;'),
        ('É', '&Eacute;'),
        ('Ê', '&Ecirc;'),
        ('Ì', '&Igrave;'),
        ('Í', '&Iacute;'),
        ('Î', '&Icirc;'),
        ('Ñ', '&Ntilde;'),
        ('Ò', '&Ograve;'),
        ('Ó', '&Oacute;'),
        ('Ô', '&Ocirc;'),
        ('Õ', '&Otilde;'),
        ('Ù', '&Ugrave;'),
        ('Ú', '&Uacute;'),
        ('Û', '&Ucirc;'),
        ('à', '&agrave;'),
        ('á', '&aacute;'),
        ('â', '&acirc;'),
        ('ã', '&atilde;'),
        ('ç', '&ccedil;'),
        ('è', '&egrave;'),
        ('é', '&eacute;'),
        ('ê', '&ecirc;'),
        ('ì', '&igrave;'),
        ('í', '&iacute;'),
        ('î', '&icirc;'),
        ('ñ', '&ntilde;'),
        ('ò', '&ograve;'),
        ('ó', '&oacute;'),
        ('ô', '&ocirc;'),
        ('õ', '&otilde;'),
        ('ù', '&ugrave;'),
        ('ú', '&uacute;'),
        ('û', '&ucirc;'),
        # (' ', '&#160;'),
        ('§', '&#167;'),
        ('ª', '&#170;'),
        ('°', '&#176;'),
        ('º', '&#186;'),
        ('˚', '&#730;'),
        ('"', '&#8220;')
    )

    for r in html_map:
        text = text.replace(*r)

    return text
