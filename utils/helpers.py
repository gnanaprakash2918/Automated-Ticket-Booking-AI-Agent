import inspect
import json
import re
from typing import Annotated, Any, Dict, List, Type, Union, get_args, get_origin
from bs4 import BeautifulSoup, Comment, Tag
from pydantic import BaseModel


BOOTSTRAP_UTIL_RE = re.compile(
    r'^(?:'
    r'col(?:-\d+|-(?:sm|md|lg|xl)(?:-\d+)?)|row|container(?:-fluid)?|'
    r'd-(?:none|block|inline|flex|table|inline-block|sm-block|md-block|lg-block)|'
    r'text-(?:left|right|center|[0-9]+|dark|muted|sm-center|md-center|lg-center)|'
    r'align-(?:items|self)(?:-(?:sm|md|lg|xl))?-(?:start|center|end)|'
    r'(?:m|p)(?:-(?:sm|md|lg|xl))?-\d+|(?:mt|mb|ml|mr|pt|pb|pl|pr)(?:-(?:sm|md|lg|xl))?-\d+|'
    r'btn|table|lead|small|flex-row|d-(?:none|inline|block|flex)|'
    r'listHeading|bgColor|bodytextWith.+|opensans|size\d+|blue\d+'
    r')$',
    re.I
)

def minify_html(html: str,
                decompose_tags=None,
                unwrap_tags=None,
                preserve_whitespace_tags=None,
                preserve_attrs=None,
                parser="lxml") -> str:

    if decompose_tags is None:
        decompose_tags = ["head","script","style","noscript","iframe","img","link","meta",
                          "header","footer","nav","button","input"]
    if unwrap_tags is None:
        unwrap_tags = ["b","strong","i","em","font","u","s","strike"]
    if preserve_whitespace_tags is None:
        preserve_whitespace_tags = ["pre","code","textarea"]
    if preserve_attrs is None:
        preserve_attrs = ["role"]

    soup = BeautifulSoup(html, parser)

    for t in decompose_tags:
        for tag in soup.find_all(t):
            tag.decompose()

    for t in unwrap_tags + ["span","small"]:
        for tag in list(soup.find_all(t)):
            if tag.attrs and any(k in ("id","class") or k.startswith(("data-","aria-")) or k in preserve_attrs for k in tag.attrs.keys()):
                continue
            tag.unwrap()

    for c in soup.find_all(string=lambda x: isinstance(x, Comment)):
        c.extract()

    for form in list(soup.find_all("form")):
        form.unwrap()

    def keep_attr(a: str) -> bool:
        return a in ("id","class") or a.startswith(("data-","aria-")) or a in preserve_attrs

    for tag in soup.find_all(True):
        for a in list(tag.attrs.keys()):
            al = a.lower()
            if al == "style" or al.startswith("on"):
                tag.attrs.pop(a, None)

        if "class" in tag.attrs:
            cls = tag.get("class") or []
            cleaned = [c for c in cls if c and not BOOTSTRAP_UTIL_RE.match(c)]
            if cleaned:
                tag["class"] = " ".join(cleaned)
            else:
                tag.attrs.pop("class", None)

        for a in list(tag.attrs.keys()):
            if not keep_attr(a):
                tag.attrs.pop(a, None)

    def convert_simple_tables(soup_obj):
        for table in list(soup_obj.find_all("table")):
            rows = table.find_all("tr", recursive=False)
            if not rows:
                continue
            kvs, ok = [], True
            for r in rows:
                cells = r.find_all(["td","th"], recursive=False)
                if len(cells) < 2:
                    ok = False; break
                k = cells[0].get_text(strip=True)
                v = cells[1].get_text(strip=True)
                if not k or not v:
                    ok = False; break
                kvs.append((k.rstrip(":"), v))
            if not (ok and kvs):
                continue
            frag = BeautifulSoup("", parser)
            root = frag.new_tag("div"); root["class"] = "kv-list"
            for k,v in kvs:
                kv = frag.new_tag("div"); kv["class"] = "kv"
                ks = frag.new_tag("span"); ks["class"] = "k"; ks.string = k
                vs = frag.new_tag("span"); vs["class"] = "v"; vs.string = v
                kv.append(ks); kv.append(vs); root.append(kv)
            table.replace_with(root)

    convert_simple_tables(soup)

    for b in list(soup.find_all("b", class_="boxheader")):
        h = soup.new_tag("h2"); h["class"] = "boxheader"; h.string = b.get_text(strip=True); b.replace_with(h)


    removable = {"div","span","section","article","aside","p","li"}
    for tag in list(soup.find_all(True)):
        if not tag.get_text(strip=True) and not any(isinstance(c, Tag) for c in tag.contents) and tag.name in removable:
            tag.decompose()

    placeholders = {}
    for i, tn in enumerate(preserve_whitespace_tags):
        for idx, tag in enumerate(soup.find_all(tn)):
            key = f"__PRESERVE_{i}_{idx}__"
            placeholders[key] = str(tag)
            tag.replace_with(key)

    compact = re.sub(r"\s+", " ", str(soup))
    compact = re.sub(r">\s+<", "><", compact)
    for k,v in placeholders.items():
        compact = compact.replace(k, v)
    return compact.strip()


def calculate_message_size(messages: List[Dict]) -> int:
    """Calculate total characters in all messages."""
    total = 0
    for msg in messages:
        total += len(msg.get('content', ''))
    return total


def _get_base_type(type_hint: Any) -> Any:
    origin = get_origin(type_hint)
    args = get_args(type_hint)
    if origin is Union:
        non_none = [a for a in args if a is not type(None)]
        if len(non_none) == 1:
            return _get_base_type(non_none[0])
        return type_hint
    if origin is Annotated:
        return _get_base_type(args[0])
    if origin in (list, List, tuple):
        return _get_base_type(args[0]) if args else type_hint
    if args:
        return _get_base_type(args[0])
    return type_hint

def extract_examples_from_pydantic_schema(model: Type[BaseModel], visited: set = None) -> str:
    if visited is None:
        visited = set()
    if model in visited:
        return ""
    visited.add(model)
    example_str = ""
    model_config = getattr(model, "model_config", {})
    if isinstance(model_config, dict) and "json_schema_extra" in model_config:
        extra = model_config["json_schema_extra"]
        if isinstance(extra, dict) and "examples" in extra:
            examples = extra["examples"]
            if examples:
                example_str += f"- **{model.__name__} Examples:**\n"
                for i, example in enumerate(examples):
                    example_str += f"  - **Example {i + 1}:**\n"
                    example_str += "```json\n" + json.dumps(example, indent=2) + "\n```\n"
    for name, field in model.model_fields.items():
        field_type = _get_base_type(field.annotation)
        if inspect.isclass(field_type) and issubclass(field_type, BaseModel) and field_type != model:
            nested = extract_examples_from_pydantic_schema(field_type, visited)
            if nested:
                if example_str:
                    example_str += "\n\n"
                example_str += nested
    return example_str.strip()