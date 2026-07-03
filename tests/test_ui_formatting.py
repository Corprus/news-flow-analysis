import sys
from pathlib import Path

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

from formatting import display_news_title, display_news_url, format_search_date  # noqa: E402


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


def test_display_news_title_hides_demo_duplicate_marker() -> None:
    assert (
        display_news_title("Новость про крейсер (повтор kuznetsov_casualties)")
        == "Новость про крейсер"
    )
    assert display_news_title("Новость про крейсер (повтор)") == "Новость про крейсер"


def test_display_news_url_hides_demo_duplicate_query_param() -> None:
    assert (
        display_news_url(
            "https://lenta.ru/news/2019/12/12/postradalii?demo_duplicate=kuznetsov&x=1"
        )
        == "https://lenta.ru/news/2019/12/12/postradalii?x=1"
    )
    assert (
        display_news_url("https://lenta.ru/news/2019/12/12/postradalii?demo_duplicate=x")
        == "https://lenta.ru/news/2019/12/12/postradalii"
    )
