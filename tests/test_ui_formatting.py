import sys
from pathlib import Path

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

from formatting import format_search_date  # noqa: E402


def test_publication_date_hides_midnight_when_requested() -> None:
    assert (
        format_search_date(
            "2019-12-13T00:00:00+00:00",
            hide_midnight=True,
        )
        == "13.12.2019"
    )


def test_publication_date_keeps_real_time() -> None:
    assert (
        format_search_date(
            "2019-12-13T12:34:00+00:00",
            hide_midnight=True,
        )
        == "13.12.2019 12:34"
    )


def test_other_timestamps_keep_midnight_by_default() -> None:
    assert format_search_date("2019-12-13T00:00:00+00:00") == "13.12.2019 00:00"
