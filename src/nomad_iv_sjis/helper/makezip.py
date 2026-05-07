#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
make_iv_upload_zip.py

指定フォルダ内の CSV ファイルごとに NOMAD archive.yaml を自動生成し、
1 つの upload ZIP にまとめるスクリプト。

想定:
- 1 CSV = 1 NOMAD entry
- 各 CSV に対応する <stem>.archive.yaml を生成
- archive.yaml 内の data_file には CSV ファイル名を記載
"""

from __future__ import annotations

import argparse
import json
import shutil
import zipfile
from pathlib import Path


M_DEF = "nomad_iv_sjis.schema_packages.ivdata.IVData"


def yaml_quote(value: str) -> str:
    """
    YAML の double-quoted scalar として安全に書き出す。

    json.dumps の出力は YAML の double quoted string としても概ね安全に使える。
    日本語ファイル名も読めるよう ensure_ascii=False にする。
    """
    return json.dumps(value, ensure_ascii=False)


def create_archive_yaml(csv_name: str) -> str:
    """
    1 つの CSV に対応する archive.yaml 文字列を作る。
    """
    return (
        "data:\n"
        f"  m_def: {M_DEF}\n"
        f"  data_file: {yaml_quote(csv_name)}\n"
    )


def make_upload_zip(input_dir: Path, output_zip: Path, pattern: str = "*.csv") -> None:
    """
    input_dir 内の CSV を対象に、CSV と archive.yaml を ZIP に格納する。

    Args:
        input_dir:
            CSV ファイルが入っているフォルダ。
        output_zip:
            作成する ZIP ファイルのパス。
        pattern:
            対象 CSV の glob pattern。既定は *.csv。
    """
    input_dir = input_dir.resolve()
    output_zip = output_zip.resolve()

    csv_files = sorted(input_dir.glob(pattern))

    if not csv_files:
        raise FileNotFoundError(f"No CSV files found: {input_dir / pattern}")

    output_zip.parent.mkdir(parents=True, exist_ok=True)

    # 既存 ZIP があれば上書き
    if output_zip.exists():
        output_zip.unlink()

    with zipfile.ZipFile(output_zip, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        for csv_path in csv_files:
            csv_name = csv_path.name

            # CSV 本体を ZIP 直下に入れる
            zf.write(csv_path, arcname=csv_name)

            # 対応する archive.yaml を ZIP 直下に入れる
            archive_name = f"{csv_path.stem}.archive.yaml"
            archive_text = create_archive_yaml(csv_name)
            zf.writestr(archive_name, archive_text)

    print(f"Created: {output_zip}")
    print(f"CSV files: {len(csv_files)}")
    for csv_path in csv_files:
        print(f"  - {csv_path.name}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Create NOMAD IV CSV upload ZIP from multiple CSV files."
    )
    parser.add_argument(
        "input_dir",
        type=Path,
        help="Directory containing IV CSV files.",
    )
    parser.add_argument(
        "-o",
        "--output",
        type=Path,
        default=Path("iv_batch_upload.zip"),
        help="Output ZIP file path.",
    )
    parser.add_argument(
        "--pattern",
        default="*.csv",
        help="CSV glob pattern. Default: *.csv",
    )

    args = parser.parse_args()
    make_upload_zip(args.input_dir, args.output, args.pattern)


if __name__ == "__main__":
    main()
