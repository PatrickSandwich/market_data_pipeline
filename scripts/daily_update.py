from __future__ import annotations

import argparse
import os
import sys
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional


def _bootstrap_import_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

def _ensure_utf8_console() -> None:
    try:
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
    except Exception:
        pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description='Chạy daily update tự động lúc 18:00',
    )
    parser.add_argument(
        '--config',
        default='config/pipeline_config.yaml',
        help='Đường dẫn config YAML',
    )
    parser.add_argument(
        '--symbols',
        default='all',
        help="Danh sách mã comma-separated hoặc 'all'",
    )
    parser.add_argument(
        '--parallel',
        type=int,
        default=None,
        help='Số workers parallel (>= 1)',
    )
    parser.add_argument(
        '--once',
        action='store_true',
        help='Chạy 1 lần rồi thoát (dùng để test)',
    )
    parser.add_argument(
        '--force-refresh',
        action='store_true',
        help='Bỏ qua cache (đặc biệt cache Market Scanner tickers) và gọi API mới',
    )
    return parser


def _seconds_until(hour: int = 18, minute: int = 0) -> float:
    now = datetime.now()
    target = now.replace(hour=hour, minute=minute, second=0, microsecond=0)
    if now >= target:
        target = target + timedelta(days=1)
    return max(0.0, (target - now).total_seconds())


def main() -> int:
    _bootstrap_import_path()
    _ensure_utf8_console()
    args = build_parser().parse_args()

    if args.parallel is not None and args.parallel < 1:
        raise SystemExit('--parallel phải >= 1')
    if args.force_refresh:
        os.environ['MDP_FORCE_REFRESH'] = '1'

    from src.utils.logger import configure_logging, get_logger
    from src.pipeline import Pipeline

    configure_logging(log_dir='logs', log_filename='daily_update.log', level=os.getenv('MDP_LOG_LEVEL', 'INFO'))
    logger = get_logger('daily_update')

    while True:
        wait_seconds = 0.0 if args.once else _seconds_until(18, 0)
        if wait_seconds > 0:
            logger.info('Chờ %.0f giây đến 18:00 để chạy daily update...', wait_seconds)
            time.sleep(wait_seconds)

        try:
            pipeline = Pipeline(config_path=args.config)
            symbols = None if args.symbols.strip().lower() == 'all' else [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
            result = pipeline.run_daily_update(symbols=symbols, parallel_workers=args.parallel)
            pipeline._notify(f'Daily update completed: failed={result.get("failed")}', severity='info')
            logger.info('Daily update hoàn tất: %s', result.get('failed'))
        except Exception as exc:
            logger.exception('Daily update thất bại: %s', exc)
            try:
                pipeline._notify(f'Daily update failed: {exc}', severity='error')  # type: ignore[name-defined]
            except Exception:
                logger.warning('Không gửi được notification')

        if args.once:
            return 0

        logger.info('Đợi đến ngày tiếp theo...')
        time.sleep(1)


if __name__ == '__main__':
    raise SystemExit(main())
