from bs4 import BeautifulSoup, Comment
import re

def minify_html(html: str, keep_tags=("table", "tr", "td", "th", "strong", "b")) -> str:
    soup = BeautifulSoup(html, "html.parser")

    if soup.head:
        soup.head.decompose()

    for tag in soup.find_all(["script", "style", "noscript", "iframe", "img", "link", "meta", "input", "button"]):
        tag.decompose()

    for tag in soup.find_all("form"):
        tag.unwrap()

    for c in soup.find_all(string=lambda text: isinstance(text, Comment)):
        c.extract()

    for tag in soup.find_all(True):
        tag.attrs = {}

    for tag in soup.find_all():
        if not tag.get_text(strip=True) and tag.name not in keep_tags:
            tag.decompose()

    compact = str(soup)
    compact = re.sub(r"\s+", " ", compact)
    compact = re.sub(r">\s+<", "><", compact)
    return compact.strip()