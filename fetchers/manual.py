"""
Fetcher: manual
Used for centers that have no public pricing page or require login.
Instead of fetching data, this sends an alert reminding you to check manually.
Used by: AKÜ DAL
"""


def fetch(center: dict) -> dict:
    """
    Does not fetch any data. Returns a flag so the tracker
    knows to send a manual-review notification.
    """
    return {
        "center_id": center["id"],
        "url": center.get("url", ""),
        "manual": True,
        "notes": center.get("notes", "No public pricing page. Manual review required."),
        "tables": [],
        "raw_text": "",
    }
