"""
Notifier
Handles alerts when a center's pricing page changes.
Currently prints to console. Extend with email or GitHub Issues as needed.
"""

from datetime import datetime


def notify(center: dict, diff_report: dict) -> None:
    status = diff_report["status"]

    if status == "unchanged":
        _log(center, "✅ No changes detected.")
        return

    if status == "new":
        _log(center, "🆕 First snapshot saved.")
        return

    # status == "changed"
    lines = [f"⚠️  CHANGE DETECTED — {center['name']} ({center['city']})"]

    if diff_report["pdf_url_changed"]:
        lines.append("  📄 PDF URL changed — new price list file uploaded")

    added = diff_report["added_rows"]
    removed = diff_report["removed_rows"]

    if added:
        lines.append(f"  ➕ {len(added)} new row(s):")
        for row in added[:10]:   # cap output at 10 rows
            lines.append(f"     {' | '.join(str(c) for c in row)}")
        if len(added) > 10:
            lines.append(f"     ... and {len(added) - 10} more")

    if removed:
        lines.append(f"  ➖ {len(removed)} removed row(s):")
        for row in removed[:10]:
            lines.append(f"     {' | '.join(str(c) for c in row)}")
        if len(removed) > 10:
            lines.append(f"     ... and {len(removed) - 10} more")

    lines.append(f"  🔗 {center['pricing_url']}")
    lines.append(f"  🕐 Previous snapshot: {diff_report['previous_snapshot_time']}")

    print("\n".join(lines))
    print()


def notify_manual(center: dict) -> None:
    """Reminder to manually check a center with no public pricing page."""
    month = datetime.now().strftime("%B %Y")
    print(
        f"📋 MANUAL CHECK REMINDER [{month}]\n"
        f"   {center['name']} — {center['notes']}\n"
        f"   🔗 {center['url']}\n"
    )


def notify_error(center: dict, error: Exception) -> None:
    print(
        f"❌ ERROR — {center['name']}\n"
        f"   {type(error).__name__}: {error}\n"
        f"   🔗 {center.get('pricing_url', center['url'])}\n"
    )


def _log(center: dict, message: str) -> None:
    print(f"[{center['name']}] {message}")
