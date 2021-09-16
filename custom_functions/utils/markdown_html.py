import inspect
import textwrap

from markdown import markdown

def imarkdown_to_html(text: str) -> str:
    """Transforms indented markdown text to html.
    """
    return markdown(inspect.cleandoc(textwrap.dedent(text)))
