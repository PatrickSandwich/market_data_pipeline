from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import List, Optional


def _bootstrap_import_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

def _ensure_utf8_console() -> None:
    try:
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
    except Exception:
        pass


def _parse_symbols(value: Optional[str]) -> Optional[List[str]]:
    if value is None:
        return None
    value = value.strip()
    if not value or value.lower() == 'all':
        return None
    symbols = [s.strip().upper() for s in value.split(',') if s.strip()]
    if not symbols:
        return None
    return symbols


def _parse_date(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    try:
        datetime.strptime(value, '%Y-%m-%d')
        return value
    except ValueError as exc:
        raise argparse.ArgumentTypeError('Ngày phải theo format YYYY-MM-DD') from exc


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Market Data Pipeline runner',
    )
    parser.add_argument(
        '--mode',
        required=True,
        choices=['daily', 'full', 'analysis', 'validate'],
        help='Chế độ chạy pipeline: daily, full, analysis, validate',
    )
    parser.add_argument(
        '--symbols',
        default='all',
        help="Danh sách mã comma-separated hoặc 'all' để lấy từ config",
    )
    parser.add_argument(
        '--date',
        type=_parse_date,
        default=None,
        help='Ngày cụ thể YYYY-MM-DD (nếu set sẽ override start/end date)',
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=None,
        help='Số workers cho parallel execution (>= 1)',
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Bỏ qua cache (đặc biệt cache Market Scanner tickers) và gọi API mới',
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Đường dẫn config YAML',
    )
    return parser


def main() -> int:
    _bootstrap_import_path()
    _ensure_utf8_console()
    args = build_parser().parse_args()

    if args.parallel is not None and args.parallel < 1:
        raise SystemExit('--parallel phải >= 1')

    if args.date:
        os.environ['MDP_START_DATE'] = args.date
        os.environ['MDP_END_DATE'] = args.date
    if args.force_refresh:
        os.environ['MDP_FORCE_REFRESH'] = '1'

    from src.pipeline import Pipeline

    pipeline = Pipeline(config_path=args.config)
    symbols = _parse_symbols(args.symbols)

    if args.mode == 'daily':
        result = pipeline.run_daily_update(symbols=symbols, parallel_workers=args.parallel)
    elif args.mode == 'full':
        result = pipeline.run_full_pipeline(symbols=symbols, parallel_workers=args.parallel)
    elif args.mode == 'analysis':
        if symbols is None:
            symbols = pipeline._resolve_symbols(None)
        result = pipeline.run_batch_analysis(
            symbols=symbols, analysis_types=['technical', 'fundamental', 'breadth']
        )
    elif args.mode == 'validate':
        if symbols is None:
            symbols = pipeline._resolve_symbols(None)
        validations = {}
        for sym in symbols:
            try:
                validations[sym] = pipeline.validate_data_quality(sym)
            except Exception as exc:
                sys.stderr.write(f'[WARN] Validate failed {sym}: {exc}\n')
                validations[sym] = {'error': str(exc)}
        result = {'validations': validations}
    else:
        raise SystemExit(f'Unsupported mode: {args.mode}')

    print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
