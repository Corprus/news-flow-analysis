from __future__ import annotations

import argparse
from pathlib import Path

import joblib


def main() -> None:
    parser = argparse.ArgumentParser(description="Показать типы сохранённых model artifacts")
    parser.add_argument("root", nargs="?", default="data/artifacts/models")
    args = parser.parse_args()

    root = Path(args.root)
    for path in sorted(root.rglob("*.joblib")):
        print("\n", path)
        try:
            obj = joblib.load(path)
            print("type:", type(obj))
            for attr in ["model", "feature_columns", "config"]:
                if hasattr(obj, attr):
                    print(attr, "=>", type(getattr(obj, attr)))
        except Exception as exc:
            print("load error:", exc)


if __name__ == "__main__":
    main()
