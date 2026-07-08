import sys
from pathlib import Path
from types import ModuleType

UI_DIR = Path(__file__).resolve().parents[1] / "ui"
sys.path.insert(0, str(UI_DIR))

streamlit_stub = ModuleType("streamlit")
streamlit_stub.dialog = lambda *_args, **_kwargs: (lambda function: function)
streamlit_stub.fragment = lambda *_args, **_kwargs: (lambda function: function)
streamlit_stub.session_state = {}

sys.modules["streamlit"] = streamlit_stub

from api_client import ApiError  # noqa: E402
from views.news import _active_import_job_id, _is_import_job_not_found  # noqa: E402


def test_missing_import_job_is_treated_as_stale_ui_state() -> None:
    """UI считает 404 по import job устаревшим session state, а не ошибкой."""
    assert _is_import_job_not_found(ApiError("Import job not found", status_code=404))


def test_non_404_import_job_error_stays_visible() -> None:
    """UI продолжает показывать реальные ошибки получения import job."""
    assert not _is_import_job_not_found(ApiError("API unavailable", status_code=503))


def test_import_status_is_restored_only_from_current_ui_session() -> None:
    """UI не подхватывает старые import jobs без явного запуска в текущей сессии."""
    streamlit_stub.session_state.clear()

    assert _active_import_job_id() is None

    streamlit_stub.session_state["news_import_job_id"] = "job-from-current-session"

    assert _active_import_job_id() == "job-from-current-session"
