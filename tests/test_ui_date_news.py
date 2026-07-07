import sys
from pathlib import Path
from types import SimpleNamespace

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

sys.modules.setdefault(
    "streamlit",
    SimpleNamespace(
        fragment=lambda **_kwargs: (lambda function: function),
        session_state={},
    ),
)

from views.date_news import PAGE_SIZE, _should_render_date_pager  # noqa: E402


def test_date_news_pager_is_hidden_for_single_page() -> None:
    """Навигация по страницам даты скрыта, если все сюжеты помещаются на экран."""
    assert not _should_render_date_pager(0)
    assert not _should_render_date_pager(PAGE_SIZE)
    assert _should_render_date_pager(PAGE_SIZE + 1)
