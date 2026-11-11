"""Skill alias table and pure normalization helpers (no I/O)."""

import re
import unicodedata
from collections.abc import Iterable, Mapping
from types import MappingProxyType

MAX_SKILLS = 50
MAX_SKILL_LEN = 50

_ALIAS_GROUPS: dict[str, tuple[str, ...]] = {
    "Go": ("go", "golang"),
    "Kubernetes": ("k8s", "kubernetes"),
    "PostgreSQL": ("postgres", "postgresql", "psql"),
    "JavaScript": ("js", "javascript"),
    "TypeScript": ("ts", "typescript"),
    "Python": ("python",),
    "Docker": ("docker",),
    "Kafka": ("kafka", "apache kafka"),
    "Redis": ("redis",),
    "React": ("react", "reactjs", "react.js"),
    "Node.js": ("node", "nodejs", "node.js"),
    "Java": ("java",),
    "C++": ("c++", "cpp"),
    "C#": ("c#", "csharp"),
    "AWS": ("aws", "amazon web services"),
    "GCP": ("gcp", "google cloud"),
    "SQL": ("sql",),
    "gRPC": ("grpc",),
    "Linux": ("linux",),
    "Git": ("git",),
    "MySQL": ("mysql",),
    "MongoDB": ("mongodb", "mongo"),
    "REST": ("rest", "restful"),
    "Microservices": ("microservices", "microservice"),
}

ALIASES: Mapping[str, str] = MappingProxyType(
    {alias: canonical for canonical, aliases in _ALIAS_GROUPS.items() for alias in aliases}
)

_WS = re.compile(r"\s+")


def _collapse(text: str) -> str:
    return _WS.sub(" ", text).strip()


def normalize(skills: Iterable[str]) -> list[str]:
    """Trims, maps aliases to canonical names and de-duplicates case-insensitively.

    Empty items and items longer than MAX_SKILL_LEN are dropped; the first
    spelling of a duplicate wins and at most MAX_SKILLS items are kept.
    """
    out: list[str] = []
    seen: set[str] = set()
    for raw in skills:
        skill = _collapse(raw)
        skill = ALIASES.get(skill.lower(), skill)
        if not skill or len(skill) > MAX_SKILL_LEN:
            continue
        key = skill.casefold()
        if key in seen:
            continue
        seen.add(key)
        out.append(skill)
        if len(out) == MAX_SKILLS:
            break
    return out


def unaccent(text: str) -> str:
    """Removes diacritics (NFD, drops combining marks) and maps đ/Đ to d/D."""
    text = text.replace("đ", "d").replace("Đ", "D")
    decomposed = unicodedata.normalize("NFD", text)
    return "".join(ch for ch in decomposed if not unicodedata.combining(ch))


def _searchable(alias: str) -> bool:
    # Short all-letter aliases ("go", "js", "ts") match ordinary words, so only
    # aliases of 3+ chars or containing a non-letter ("c#", "c++") are searched.
    return len(alias) >= 3 or not alias.isalpha()


_SEARCH_ALIASES = sorted((a for a in ALIASES if _searchable(a)), key=len, reverse=True)
_SKILL_RE = re.compile(
    r"(?<![a-z0-9+#.])(" + "|".join(re.escape(a) for a in _SEARCH_ALIASES) + r")(?![a-z0-9+#])"
)


def find_skills(text: str) -> list[str]:
    """Finds known skills in free text, in order of first appearance, normalized."""
    # Longest aliases come first in the alternation, so "apache kafka" wins over "kafka".
    haystack = _collapse(unaccent(text).lower())
    return normalize(ALIASES[m.group(1)] for m in _SKILL_RE.finditer(haystack))
