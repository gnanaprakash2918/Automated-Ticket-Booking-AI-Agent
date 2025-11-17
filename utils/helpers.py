from bs4 import BeautifulSoup, Comment
import re
from typing import List

import re
from bs4 import BeautifulSoup, Comment

def minify_html(
    html: str,
    decompose_tags=None,
    unwrap_tags=None,
    preserve_whitespace_tags=None,
    preserve_attrs=None,
    parser="lxml",
) -> str:
    """
    Minifies HTML:
      - decompose_tags: tags to fully remove (tag + children)
      - unwrap_tags: tags to remove but keep their inner text/children
      - preserve_whitespace_tags: tags whose inner whitespace must be preserved (pre, code, textarea)
      - preserve_attrs: attributes to keep (besides id/class and data-*)
    """

    if decompose_tags is None:
        decompose_tags = [
            "head", "script", "style", "noscript", "iframe", "img", "link", "meta",
            "header", "footer", "nav", "button", "input"
        ]
    if unwrap_tags is None:
        unwrap_tags = ["b", "strong", "i", "em", "font", "u", "s", "strike"]
    if preserve_whitespace_tags is None:
        preserve_whitespace_tags = ["pre", "code", "textarea"]
    if preserve_attrs is None:
        preserve_attrs = ["role"]

    soup = BeautifulSoup(html, parser)

    for t in decompose_tags:
        for tag in soup.find_all(t):
            tag.decompose()

    for t in unwrap_tags:
        for tag in soup.find_all(t):
            tag.unwrap()

    for comment in soup.find_all(string=lambda text: isinstance(text, Comment)):
        comment.extract()

    def should_keep_attr(attr):
        if attr in ("id", "class"):
            return True
        if attr.startswith("data-") or attr.startswith("aria-"):
            return True
        if attr in preserve_attrs:
            return True
        return False

    for tag in soup.find_all(True):
        for attr in list(tag.attrs.keys()):
            if not should_keep_attr(attr):
                del tag.attrs[attr]

    removable_empty = set([
        "div", "span", "section", "article", "aside", "p", "li"
    ])
    for tag in soup.find_all():
        text = tag.get_text(strip=True)
        has_non_tag_children = any(not isinstance(c, (str,)) and getattr(c, "name", None) for c in tag.contents)
        if not text and not has_non_tag_children and tag.name in removable_empty:
            tag.decompose()

    placeholders = {}
    for i, tagname in enumerate(preserve_whitespace_tags):
        for idx, tag in enumerate(soup.find_all(tagname)):
            key = f"__PRESERVE_{i}_{idx}__"
            placeholders[key] = str(tag)
            tag.replace_with(key)

    compact = str(soup)

    compact = re.sub(r"\s+", " ", compact)
    compact = re.sub(r">\s+<", "><", compact)

    for key, val in placeholders.items():
        compact = compact.replace(key, val)

    return compact.strip()


def calculate_message_size(messages: List[dict]) -> int:
    """Calculate total characters in all messages."""
    total = 0
    for msg in messages:
        total += len(msg.get('content', ''))
    return total