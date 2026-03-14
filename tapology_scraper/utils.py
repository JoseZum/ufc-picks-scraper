import re
from typing import Optional


def extract_tapology_fighter_id(url_or_path: Optional[str]) -> Optional[str]:
    """
    Extract the canonical Tapology fighter key from a fighter URL/path.

    Tapology fighter URLs come in two common shapes:
    - /fightcenter/fighters/41705-alexander-volkanovski-the-great
    - /fightcenter/fighters/aljamain-sterling

    We use the numeric prefix when present, otherwise the full slug.
    """
    if not url_or_path:
        return None

    match = re.search(r"/fighters/([^/?#]+)", url_or_path)
    if not match:
        return None

    fighter_segment = match.group(1).strip().strip("/")
    numeric_match = re.match(r"(\d+)(?:-|$)", fighter_segment)
    if numeric_match:
        return numeric_match.group(1)

    return fighter_segment or None
