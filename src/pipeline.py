from __future__ import annotations

import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd

from src.utils.config_loader import ConfigLoader
from src.utils.file_utils import ensure_dir
from src.utils.logger import configure_logging, get_logger
from src.utils.market_scanner import MarketScanner
from src.analyzers.breadth_analyzer import BreadthAnalyzer
from src.analyzers.fundamental_analyzer import FundamentalAnalyzer
from src.analyzers.technical_screener import TechnicalScreener
from src.scanners.market_scope_filter import MarketScopeConfig, MarketScopeFilter
from src.extractors.fundamental_extractor import FundamentalExtractor
from src.extractors.price_extractor import PriceExtractor
from src.extractors.breadth_extractor import BreadthExtractor
from src.extractors.data_validator import validate_and_filter_symbols
from src.extractors.models import ExtractionTask
from src.transformers.data_cleaner import DataCleaner
from src.transformers.technical_indicators import TechnicalIndicators


class Pipeline:
    """Orchestrator tổng hợp các extractor, transformer và analyzer trong pipeline."""

    def __init__(self, config_path: str = 'config/settings.yaml') -> None:
        self.config = ConfigLoader(config_path).load()
        run_id = datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')
        log_dir = Path(self.config['logging'].get('dir', 'logs')) / f'pipeline_{run_id}'
        ensure_dir(str(log_dir))
        configure_logging(
            log_dir=str(log_dir),
            log_filename='pipeline.log',
            level=self.config['logging'].get('level', 'INFO'),
        )
        self.logger = get_logger(self.__class__.__name__)
        self.price_extractor = PriceExtractor()
        self.fundamental_extractor = FundamentalExtractor()
        self.breadth_extractor = BreadthExtractor()
        self.data_cleaner = DataCleaner()
        self.indicators = TechnicalIndicators()
        self.screener = TechnicalScreener()
        self.breadth_analyzer = BreadthAnalyzer()
        self.fundamental_analyzer = FundamentalAnalyzer()
        self.processed_dir = Path(self.config['data_paths']['processed'])
        ensure_dir(str(self.processed_dir))
        self.raw_dir = Path(self.config['data_paths']['raw'])
        ensure_dir(str(self.raw_dir))

    def _resolve_symbols(self, symbols: Optional[List[str]] = None) -> List[str]:
        """
        Xác định danh sách mã chứng khoán cần xử lý theo cấu hình market_scope.

        Logic:
        - Nếu `symbols` được truyền vào (CLI/API) -> ưu tiên sử dụng.
        - Nếu `market_scope.mode == "dynamic"` -> quét toàn thị trường qua MarketScanner.
        - Nếu `market_scope.mode == "manual"` -> dùng `market_scope.symbols`.
        - Fallback -> dùng `config.symbols` (tương thích ngược).

        Returns:
            Danh sách symbol đã normalize (uppercase, unique).

        Raises:
            RuntimeError: nếu mode dynamic lỗi và không có danh sách fallback hợp lệ.
        """

        if symbols:
            valid_symbols, removed = validate_and_filter_symbols(symbols)
            for item in removed:
                self.logger.warning(
                    'Bỏ qua symbol %s: %s', item.get('symbol'), item.get('reason')
                )
            if not valid_symbols:
                raise ValueError('Không có symbol hợp lệ từ tham số đầu vào.')
            return valid_symbols

        raw_market_scope = self.config.get('market_scope')
        market_scope = raw_market_scope if isinstance(raw_market_scope, dict) else {}
        default_mode = 'dynamic' if isinstance(raw_market_scope, str) else 'manual'
        mode = str(market_scope.get('mode', default_mode)).strip().lower()

        if mode == 'dynamic':
            try:
                force_refresh = bool(
                    str(os.getenv('MDP_FORCE_REFRESH', '')).strip().lower() in {'1', 'true', 'yes'}
                ) or bool(market_scope.get('force_refresh', False))
                filters = market_scope.get('filters') if isinstance(market_scope, dict) else None
                exchanges = market_scope.get('exchanges') if isinstance(market_scope, dict) else None

                cache_root = self.raw_dir.parent
                if self.raw_dir.name.lower() == 'raw':
                    cache_root = self.raw_dir.parent
                elif self.raw_dir.parent.name.lower() == 'raw':
                    cache_root = self.raw_dir.parent.parent
                scanner = MarketScanner(cache_dir=str(cache_root / 'cache'))
                tickers = scanner.get_all_tickers(
                    force_refresh=force_refresh,
                    exchanges=exchanges if isinstance(exchanges, list) else None,
                    filters=filters if isinstance(filters, dict) else None,
                )
                self.logger.info(
                    'Khởi chạy chế độ DYNAMIC - Quét toàn thị trường. Tổng số mã phát hiện: %s',
                    len(tickers),
                )

                # Market Scope Filter: giảm số lượng mã cần xử lý (đặc biệt UPCOM) để tối ưu hiệu năng.
                try:
                    scope_config = MarketScopeConfig.from_config(self.config)
                    listing_df = MarketScopeFilter.load_listing_dataframe(source='vci')
                    tickers = MarketScopeFilter(scope_config).filter_symbols(
                        listing_df,
                        universe_symbols=tickers,
                    )
                except Exception as exc:
                    self.logger.warning(
                        'Không áp dụng được Market Scope Filter, dùng danh sách gốc: %s',
                        exc,
                    )

                valid_symbols, removed = validate_and_filter_symbols(tickers)
                removed_log_limit = int(market_scope.get('removed_symbols_log_limit', 200))
                for item in removed[:removed_log_limit]:
                    self.logger.warning(
                        'Bỏ qua symbol %s: %s', item.get('symbol'), item.get('reason')
                    )
                if len(removed) > removed_log_limit:
                    self.logger.warning(
                        '... và còn %s symbols bị loại bỏ khác (giới hạn log=%s).',
                        len(removed) - removed_log_limit,
                        removed_log_limit,
                    )
                self.logger.info('✓ Còn %s symbols hợp lệ sau khi lọc', len(valid_symbols))
                if not valid_symbols:
                    self.logger.error(
                        '🚨 Tất cả symbols đều không hợp lệ! Fallback về MANUAL mode'
                    )
                    fallback = market_scope.get('symbols') or self.config.get('symbols') or [
                        'VNM',
                        'MWG',
                    ]
                    valid_fallback, removed_fallback = validate_and_filter_symbols(fallback)
                    for item in removed_fallback[:removed_log_limit]:
                        self.logger.warning(
                            'Bỏ qua symbol %s (fallback MANUAL): %s',
                            item.get('symbol'),
                            item.get('reason'),
                        )
                    if not valid_fallback:
                        raise RuntimeError(
                            'Không có symbol hợp lệ ở fallback MANUAL sau khi lọc.'
                        )
                    self.logger.info(
                        'Khởi chạy chế độ MANUAL - Danh sách tùy chỉnh. Số lượng mã: %s',
                        len(valid_fallback),
                    )
                    return valid_fallback
                return valid_symbols
            except Exception as exc:
                self.logger.error('Lỗi MarketScanner ở chế độ DYNAMIC: %s', exc)
                fallback = market_scope.get('symbols') or self.config.get('symbols') or []
                if fallback:
                    valid_fallback, removed_fallback = validate_and_filter_symbols(fallback)
                    for item in removed_fallback:
                        self.logger.warning(
                            'Bỏ qua symbol %s (fallback MANUAL): %s',
                            item.get('symbol'),
                            item.get('reason'),
                        )
                    if not valid_fallback:
                        raise RuntimeError(
                            'Không có symbol hợp lệ ở fallback MANUAL sau khi lọc.'
                        ) from exc
                    self.logger.warning(
                        'Fallback sang MANUAL list do lỗi DYNAMIC. Số lượng mã: %s',
                        len(valid_fallback),
                    )
                    return valid_fallback
                raise RuntimeError(
                    f'Không thể chạy DYNAMIC và không có danh sách symbols fallback: {exc}'
                ) from exc

        manual_symbols = market_scope.get('symbols') or self.config.get('symbols') or []
        valid_symbols, removed = validate_and_filter_symbols(manual_symbols)
        for item in removed:
            self.logger.warning(
                'Bỏ qua symbol %s (MANUAL): %s', item.get('symbol'), item.get('reason')
            )
        if not valid_symbols:
            self.logger.error('MANUAL mode không có symbol hợp lệ. Dùng fallback: VNM, MWG')
            valid_symbols = ['VNM', 'MWG']
        self.logger.info(
            'Khởi chạy chế độ MANUAL - Danh sách tùy chỉnh. Số lượng mã: %s',
            len(valid_symbols),
        )
        return valid_symbols

    def run_daily_update(
        self,
        symbols: Optional[List[str]] = None,
        parallel_workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Chạy cập nhật OHLCV hàng ngày cho các mã được cấu hình."""

        target_symbols = self._resolve_symbols(symbols)
        summary: List[Dict[str, Any]] = []
        failed_symbols: List[str] = []
        start_date = self.config['start_date']
        end_date = self.config['end_date']
        resolution = self.config.get('resolution', '1D')
        performance = self.config.get('performance') or {}
        if parallel_workers is None:
            try:
                parallel_workers = int(performance.get('max_concurrent_requests'))
            except Exception:
                parallel_workers = None
        retry_count = int(performance.get('max_retries', self.config.get('retry', 3)))
        if parallel_workers and parallel_workers > 1:
            with ThreadPoolExecutor(max_workers=parallel_workers) as executor:
                futures = {
                    executor.submit(
                        self._run_with_retries,
                        lambda s=symbol: self._process_symbol(s, start_date, end_date, resolution),
                        retry_count,
                    ): symbol
                    for symbol in target_symbols
                }
                for future in as_completed(futures):
                    symbol = futures[future]
                    try:
                        df = future.result()
                        self._persist_symbol(symbol, df)
                        summary.append({'symbol': symbol, 'status': 'success', 'rows': len(df)})
                    except Exception as exc:  # pragma: no cover
                        self.logger.exception('Daily update failed cho %s', symbol)
                        self._notify(f'Daily update failed {symbol}: {exc}', severity='error')
                        failed_symbols.append(symbol)
                        summary.append({'symbol': symbol, 'status': 'failed', 'error': str(exc)})
        else:
            for symbol in target_symbols:
                try:
                    df = self._run_with_retries(
                        lambda: self._process_symbol(symbol, start_date, end_date, resolution),
                        max_attempts=retry_count,
                    )
                    self._persist_symbol(symbol, df)
                    summary.append({'symbol': symbol, 'status': 'success', 'rows': len(df)})
                except Exception as exc:  # pragma: no cover - orchestrator must continue
                    self.logger.exception('Daily update failed cho %s', symbol)
                    self._notify(f'Daily update failed {symbol}: {exc}', severity='error')
                    failed_symbols.append(symbol)
                    summary.append({'symbol': symbol, 'status': 'failed', 'error': str(exc)})

        total_symbols = len(target_symbols)
        error_count = len(failed_symbols)
        success_count = total_symbols - error_count
        self.logger.info('=== PIPELINE SUMMARY ===')
        self.logger.info('Tổng symbols: %s', total_symbols)
        self.logger.info('Thành công: %s', success_count)
        self.logger.info('Thất bại: %s', error_count)
        self.logger.info('Symbols lỗi: %s', failed_symbols)

        return {
            'run': datetime.utcnow().isoformat(),
            'total_symbols': total_symbols,
            'successful': success_count,
            'failed': error_count,
            'failed_symbols': failed_symbols,
            'details': summary,
        }

    def _process_symbol(
        self,
        symbol: str,
        start_date: str,
        end_date: str,
        resolution: str,
    ) -> pd.DataFrame:
        task = ExtractionTask(
            task_id=f'daily-{symbol}-{int(time.time())}',
            symbol=symbol,
            data_type='ohlcv',
            start_date=start_date,
            end_date=end_date,
            resolution=resolution,
            config={},
        )
        result = self.price_extractor.extract(task)
        if not result.success or result.data is None:
            raise RuntimeError(f'Extractor trả về lỗi cho {symbol}')
        cleaned = self.data_cleaner.clean_ohlcv(result.data)
        augmented = cleaned.copy()
        augmented = self.indicators.add_moving_averages(augmented)
        augmented = self.indicators.add_ema(augmented)
        augmented = self.indicators.add_rsi(augmented)
        augmented = self.indicators.add_macd(augmented)
        augmented = self.indicators.add_bollinger_bands(augmented)
        augmented = self.indicators.add_volatility(augmented)
        augmented = self.indicators.add_volume_metrics(augmented)
        augmented = self.indicators.add_price_changes(augmented)
        return augmented

    def _persist_symbol(self, symbol: str, df: pd.DataFrame) -> None:
        path = self.processed_dir / f'{symbol}.parquet'
        ensure_dir(str(path.parent))
        try:
            df.to_parquet(path, index=False)
            self.logger.info('Lưu %s -> %s (%s rows)', symbol, path, len(df))
        except Exception as exc:
            fallback_path = self.processed_dir / f'{symbol}.csv'
            df.to_csv(fallback_path, index=False)
            self.logger.warning(
                'Không thể ghi parquet (%s). Fallback CSV: %s', exc, fallback_path
            )

    def run_batch_analysis(
        self,
        symbols: List[str],
        analysis_types: List[str],
    ) -> Dict[str, Any]:
        """Chạy nhiều phân tích theo các loại yêu cầu."""

        records: List[Dict[str, Any]] = []
        for symbol in symbols:
            path = self.processed_dir / f'{symbol}.parquet'
            if not path.exists():
                self.logger.warning('Không tìm thấy processed data cho %s', symbol)
                continue
            df = pd.read_parquet(path)
            row: Dict[str, Any] = {'symbol': symbol}
            if 'technical' in analysis_types:
                row['technical'] = self.screener.calculate_signal(df)
            if 'fundamental' in analysis_types:
                try:
                    financials = self.fundamental_extractor.get_financial_report(
                        symbol, report_type='income_statement'
                    )
                    row['fundamental_score'] = self.fundamental_analyzer.score_fundamentals(
                        financials, self.config.get('sector_averages', {})
                    )
                except Exception as exc:
                    self.logger.warning('Fundamental analysis failed %s: %s', symbol, exc)
                    row['fundamental_score'] = {'error': str(exc)}
            if 'breadth' in analysis_types:
                breadth = self.breadth_extractor.get_market_breadth()
                row['breadth'] = self.breadth_analyzer.calculate_market_health(breadth)
            records.append(row)
        return {'analysis': records}

    def run_full_pipeline(
        self,
        symbols: Optional[List[str]] = None,
        parallel_workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        """Chạy toàn bộ flows: daily update, breadth, fundamental, screener."""

        report: Dict[str, Any] = {}
        target_symbols = self._resolve_symbols(symbols)
        report['daily'] = self.run_daily_update(symbols=target_symbols, parallel_workers=parallel_workers)
        try:
            breadth = self.breadth_extractor.get_market_breadth()
            report['breadth_health'] = self.breadth_analyzer.calculate_market_health(
                breadth
            )
            report['breadth_regime'] = self.breadth_analyzer.detect_market_regime(breadth)
        except Exception as exc:
            self.logger.warning('Không cập nhật breadth: %s', exc)
        report['technical_screen'] = self.run_batch_analysis(target_symbols, ['technical'])
        report['fundamental_screen'] = self.run_batch_analysis(target_symbols, ['fundamental'])
        return report

    def validate_data_quality(self, symbol: str) -> Dict[str, Any]:
        """Kiểm tra chất lượng dữ liệu đã được xử lý."""

        path = self.processed_dir / f'{symbol}.parquet'
        if not path.exists():
            raise FileNotFoundError(f'Processed file missing: {path}')
        df = pd.read_parquet(path)
        issues: List[str] = []
        expected_days = pd.date_range(
            self.config['start_date'], self.config['end_date'], freq='B'
        )
        actual_days = df['date'].nunique() if 'date' in df.columns else 0
        missing = len(expected_days) - actual_days
        if missing > 0:
            issues.append(f'Missing {missing} trading days')
        duplicates = df.duplicated(subset=['time']).sum() if 'time' in df.columns else 0
        if duplicates > 0:
            issues.append(f'{duplicates} duplicate dates')
        if (df['close'] < 0).any():
            issues.append('Close < 0 detected')
        if (df['volume'] == 0).any():
            issues.append('Zero volume bar(s)')
        freshness = pd.to_datetime(df['time']).max() if 'time' in df.columns else None
        quality_score = max(0, 100 - len(issues) * 10)
        issues.append(f'Freshness: {freshness}')
        return {
            'quality_score': quality_score,
            'issues_found': issues,
            'recommendations': ['Increase data cadence' if missing > 0 else 'OK'],
        }

    def _notify(self, message: str, severity: str = 'info') -> None:
        """Gửi thông báo khi pipeline có lỗi/cảnh báo (Telegram/Email nếu cấu hình)."""

        import json
        import os
        import urllib.request

        self.logger.info('Notify [%s]: %s', severity, message)
        token = os.getenv('TELEGRAM_BOT_TOKEN')
        chat_id = os.getenv('TELEGRAM_CHAT_ID')
        if token and chat_id:
            try:
                url = f'https://api.telegram.org/bot{token}/sendMessage'
                payload = json.dumps({'chat_id': chat_id, 'text': message}).encode('utf-8')
                req = urllib.request.Request(
                    url=url,
                    data=payload,
                    headers={'Content-Type': 'application/json'},
                    method='POST',
                )
                with urllib.request.urlopen(req, timeout=10) as resp:
                    resp.read()
            except Exception as exc:
                self.logger.warning('Gửi Telegram thất bại: %s', exc)

    def _run_with_retries(
        self,
        func: Any,
        max_attempts: int = 3,
        delay: float = 1.0,
        backoff: float = 2.0,
    ) -> Any:
        """Chạy hàm với retry exponential backoff."""

        attempt = 0
        current_delay = delay
        while True:
            try:
                return func()
            except Exception:
                attempt += 1
                if attempt >= max_attempts:
                    raise
                self.logger.warning('Retry %s/%s sau %.1fs', attempt, max_attempts, current_delay)
                time.sleep(current_delay)
                current_delay *= backoff
