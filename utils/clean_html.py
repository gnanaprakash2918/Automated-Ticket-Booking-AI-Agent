from bs4 import BeautifulSoup, Comment
import re

def minify_html(html: str) -> str:
    """
    Minifies HTML by removing non-essential tags and attributes,
    but preserves key identifiers like class, id, and data-*.
    """
    soup = BeautifulSoup(html, "html.parser")

    tags_to_remove = [
        "head", "script", "style", "noscript", "iframe", "img", "link", 
        "meta", "header", "footer", "nav", "button", "input"
    ]
    for tag in soup.find_all(tags_to_remove):
        tag.decompose()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    attributes_to_remove = [
        "style", "onclick", "href", "width", "height", "cellpadding", 
        "cellspacing", "border", "bgcolor", "background", "align", 
        "valign", "target", "rel", "type", "role", "aria-labelledby",
        "aria-hidden", "method", "action", "autocomplete"
    ]
    
    for tag in soup.find_all(True):
        attrs = tag.attrs.copy()
        for attr in attrs:
            if attr not in ["id", "class"] and not attr.startswith("data-"):
                del tag.attrs[attr]

    for tag in soup.find_all():
        if not tag.get_text(strip=True) and not tag.find_all(recursive=False):
             if tag.name not in ["table", "tr", "td", "th"]:
                tag.decompose()

    compact = str(soup)
    compact = re.sub(r"\s+", " ", compact)
    compact = re.sub(r">\s+<", "><", compact)
    return compact.strip()