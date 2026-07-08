import sys
from pathlib import Path
from types import SimpleNamespace

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

sys.modules.setdefault(
    "streamlit",
    SimpleNamespace(
        session_state={},
        column_config=SimpleNamespace(
            TextColumn=lambda *args, **kwargs: None,
            LinkColumn=lambda *args, **kwargs: None,
        ),
    ),
)

from views.transactions import _transaction_object_label  # noqa: E402


def test_transaction_object_label_describes_batch_publication() -> None:
    """Пакетная публикация в операциях отображается как объект операции."""
    label = _transaction_object_label(
        {
            "reason": "news_add",
            "batch_id": "55555555-5555-5555-5555-555555555555",
            "item_count": 22,
        }
    )

    assert label == "Пакетная публикация: 22 новостей"


def test_transaction_object_label_falls_back_to_reference_title() -> None:
    """Одиночная операция по-прежнему показывает заголовок новости."""
    assert (
        _transaction_object_label({"reference_title": "Заголовок новости"})
        == "Заголовок новости"
    )
