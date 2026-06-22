from pathlib import Path

import pandas as pd
from scripts.build_demo_fixture import build_fixture


def test_build_demo_fixture_selects_latest_valid_rows(tmp_path: Path) -> None:
    source = tmp_path / "source.csv"
    output = tmp_path / "demo.csv"
    pd.DataFrame(
        [
            {
                "url": "https://example.com/old",
                "title": "Старая",
                "text": "Старый текст",
                "topic": "Архив",
                "tags": "Все",
                "date": "2018-01-01",
            },
            {
                "url": "https://example.com/middle",
                "title": "Средняя",
                "text": "Средний текст",
                "topic": None,
                "tags": "Россия",
                "date": "2019-01-01",
            },
            {
                "url": "https://example.com/new",
                "title": "Новая",
                "text": "Новый текст  \nВторая строка ",
                "topic": "Мир",
                "tags": "Все",
                "date": "2019-01-02",
            },
            {
                "url": "",
                "title": "Некорректная",
                "text": "Без ссылки",
                "topic": "Мир",
                "tags": "Все",
                "date": "2020-01-01",
            },
        ]
    ).to_csv(source, index=False)

    selected = build_fixture(source, output, rows=2)

    assert selected["published_at"].tolist() == ["2019-01-01", "2019-01-02"]
    assert selected["topic"].tolist() == ["Россия", "Мир"]
    assert selected["news_id"].str.len().tolist() == [24, 24]
    assert selected["text"].iloc[-1] == "Новый текст\nВторая строка"
    assert output.is_file()
