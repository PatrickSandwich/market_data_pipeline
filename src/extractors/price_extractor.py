from __future__ import annotations

from datetime import datetime, timedelta
import time
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from pytz import timezone
from vnstock import Quote

from src.utils.decorators import retry, safe_execute, timer
from .base_extractor import BaseExtractor
from .models import ExtractionTask, TaskResult

ASIA_SG_TZ = timezone('Asia/Ho_Chi_Minh')
REALTIME_CACHE: Dict[Tuple[str, ...], Tuple[datetime, pd.DataFrame]] = {}
MAX_REALTIME_SYMBOLS = 50


class PriceExtractor(BaseExtractor):
    """Extractor chuyên biệt để truy vấn dữ liệu giá OHLCV và realtime."""

    name = 'price_extractor'
    supported_data_types = ['ohlcv', 'realtime', 'historical']
    default_config: Dict[str, Any] = {'config': {}, 'data_type': 'ohlcv'}

    def extract(self, task: ExtractionTask) -> TaskResult:
        """Thực thi task trích xuất OHLCV cho symbol được chỉ định."""

        self.logger.info('Bắt đầu trích xuất %s (%s)', task.symbol, task.data_type)
        start_time = time.monotonic()
        try:
            if not task.start_date or not task.end_date:
                raise ValueError('Missing start_date hoặc end_date trong task.')
            df = self._fetch_ohlcv(
                symbol=task.symbol,
                start=task.start_date,
                end=task.end_date,
                interval=task.resolution,
            )
            cleaned = self._validate_and_clean(df)
            execution_time = time.monotonic() - start_time
            self.logger.info(
                'Hoàn tất %s rows cho %s trong %.2fs',
                len(cleaned),
                task.symbol,
                execution_time,
            )

            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=True,
                data=cleaned,
                row_count=len(cleaned),
                execution_time=execution_time,
            )
        except Exception as exc:
            self.logger.error('Lỗi khi trích xuất %s: %s', task.symbol, exc)
            execution_time = time.monotonic() - start_time
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=False,
                error=str(exc),
                row_count=0,
                execution_time=execution_time,
            )

    @retry(max_attempts=3, delay=1.0, backoff=2.0)
    @timer
    def _fetch_ohlcv(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str,
    ) -> pd.DataFrame:
        """Gọi API vnstock để lấy dữ liệu lịch sử OHLCV."""

        quote = Quote(symbol=symbol, source='vci')
        df = quote.history(start=start, end=end, interval=interval)
        if df.empty:
            raise ValueError('API trả về dữ liệu trống.')
        return df

    def _validate_and_clean(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa tên cột, timezone và kiểm tra dữ liệu."""

        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        required = {'time', 'open', 'high', 'low', 'close', 'volume'}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            raise ValueError(f'Thiếu cột bắt buộc: {missing}')
        df = df.dropna(subset=required)
        df = df.drop_duplicates(subset=['time'], keep='last')
        df['time'] = (
            pd.to_datetime(df['time'])
            .dt.tz_localize('UTC', ambiguous='infer')
            .dt.tz_convert(ASIA_SG_TZ)
        )
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        if df[numeric_cols].isna().any().any():
            raise ValueError('Dữ liệu chứa giá trị NaN sau khi ép kiểu.')
        return df

    @safe_execute(default=pd.DataFrame())
    def get_realtime(self, symbols: List[str]) -> pd.DataFrame:
        """Lấy bảng giá realtime cho tối đa 50 symbols và cache 60s."""

        if len(symbols) > MAX_REALTIME_SYMBOLS:
            raise ValueError(f'Maximum {MAX_REALTIME_SYMBOLS} symbols cho mỗi lần gọi realtime.')
        cache_key = tuple(sorted(symbols))
        now = datetime.utcnow()
        cached = REALTIME_CACHE.get(cache_key)
        if cached:
            expires_at, cached_df = cached
            if now < expires_at:
                self.logger.debug('Realtime cache hit cho %s', cache_key)
                return cached_df.copy()
        records: List[Dict[str, Any]] = []
        for symbol in symbols:
            record = self._fetch_realtime_record(symbol)
            if record:
                records.append(record)
        df = pd.DataFrame(records)
        if not df.empty:
            df.columns = [col.strip().lower() for col in df.columns]
            df['time'] = (
                pd.to_datetime(df['time'])
                .dt.tz_localize(ASIA_SG_TZ)
            )
        REALTIME_CACHE[cache_key] = (now + timedelta(seconds=60), df.copy())
        self.logger.info('Realtime fetched %s symbols', len(df))
        return df

    @safe_execute(default=None)
    def _fetch_realtime_record(self, symbol: str) -> Optional[Dict[str, Any]]:
        """Lấy dữ liệu realtime cho một mã."""

        quote = Quote(symbol=symbol, source='vci')
        data = quote.realtime()
        if not data:
            self.logger.warning('Không có dữ liệu realtime cho %s', symbol)
            return None
        return {
            'symbol': symbol,
            'price': data.get('price'),
            'change': data.get('change'),
            'pct_change': data.get('pct_change'),
            'volume': data.get('volume'),
            'time': data.get('time'),
        }

    @safe_execute(default=pd.DataFrame())
    def get_intraday(
        self,
        symbol: str,
        interval: str = '15m',
        limit: int = 100,
    ) -> pd.DataFrame:
        """Lấy dữ liệu intraday theo interval và giới hạn số bản ghi."""

        if limit > 5000:
            raise ValueError('limit tối đa là 5000.')
        quote = Quote(symbol=symbol, source='vci')
        df = quote.history(interval=interval, limit=limit)
        if df.empty:
            return df
        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        df['time'] = (
            pd.to_datetime(df['time'])
            .dt.tz_localize('UTC', ambiguous='infer')
            .dt.tz_convert(ASIA_SG_TZ)
        )
        numeric_cols = ['open', 'high', 'low', 'close', 'volume']
        df[numeric_cols] = df[numeric_cols].apply(pd.to_numeric, errors='coerce')
        if df[numeric_cols].isna().any().any():
            self.logger.warning('Dữ liệu intraday chứa NaN cho %s', symbol)
        df = df.drop_duplicates(subset=['time'], keep='last')
        return df
