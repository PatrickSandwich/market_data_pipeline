from __future__ import annotations

import re
from typing import Any, Iterable, List

import pandas as pd
from pytz import timezone

from src.utils.logger import get_logger


class DataCleaner:
    """Tập hợp hàm xử lý sạch và chuẩn hóa nguồn dữ liệu đầu vào."""

    REQUIRED_OHLCV = ['open', 'high', 'low', 'close', 'volume']
    TZ = timezone('Asia/Ho_Chi_Minh')

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def _log_counts(self, label: str, before: int, after: int) -> None:
        """Ghi log số dòng trước / sau khi clean."""

        self.logger.info('%s: %s -> %s records', label, before, after)

    def clean_ohlcv(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Dọn dữ liệu OHLCV: timezone, sort, dedupe, forward-fill và validate.

        Args:
            df: Dữ liệu thô có cột `time` và các cột OHLCV.
        Returns:
            DataFrame mới đã chuẩn hóa.
        Raises:
            ValueError: nếu cột bắt buộc thiếu hoặc chứa giá trị không mong muốn.
        """

        if 'time' not in df.columns:
            raise ValueError('OHLCV dataset cần cột "time".')
        before = len(df)
        clean_df = df.copy()
        clean_df['time'] = pd.to_datetime(clean_df['time'], errors='coerce', utc=True)
        clean_df['time'] = clean_df['time'].dt.tz_convert(self.TZ)
        clean_df.sort_values('time', inplace=True, ignore_index=False)
        clean_df.drop_duplicates(subset=['time'], keep='last', inplace=True)
        clean_df[self.REQUIRED_OHLCV] = clean_df[self.REQUIRED_OHLCV].apply(
            pd.to_numeric, errors='coerce'
        )
        clean_df[self.REQUIRED_OHLCV] = clean_df[self.REQUIRED_OHLCV].ffill()
        if (clean_df[self.REQUIRED_OHLCV] <= 0).any().any():
            raise ValueError('Giá trị OHLCV phải dương sau khi clean.')
        clean_df.dropna(subset=['time'], inplace=True)
        clean_df.reset_index(drop=True, inplace=True)
        clean_df['date'] = clean_df['time'].dt.date
        after = len(clean_df)
        self._log_counts('clean_ohlcv', before, after)
        return clean_df

    def clean_financial(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Chuẩn hóa financial report: snake_case, parse units, xử lý % và sắp xếp.

        Args:
            df: Bảng financial thô.
        Returns:
            DataFrame đã chuẩn hóa.
        Raises:
            ValueError: khi không có dữ liệu hợp lệ để sắp xếp.
        """

        before = len(df)
        clean_df = df.copy()
        clean_df.columns = [self._snake_case(col) for col in clean_df.columns]
        for column in clean_df.columns:
            clean_df[column] = clean_df[column].apply(self._parse_financial_value)
        clean_df.replace({'': pd.NA, None: pd.NA}, inplace=True)
        clean_df.dropna(how='all', inplace=True)
        if clean_df.empty:
            raise ValueError('Financial dataframe rỗng sau khi xử lý.')
        if 'year' in clean_df.columns:
            clean_df.sort_values('year', ascending=False, inplace=True)
        elif 'period' in clean_df.columns:
            clean_df.sort_values('period', ascending=False, inplace=True)
        clean_df.reset_index(drop=True, inplace=True)
        after = len(clean_df)
        self._log_counts('clean_financial', before, after)
        return clean_df

    def clean_breadth(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Validate và hoàn thiện các chỉ số breadth.

        Args:
            df: Dữ liệu breadth thô.
        Returns:
            DataFrame đã chuẩn với derived columns cập nhật.
        Raises:
            ValueError: nếu breadth_percent vượt ngưỡng 100% hoặc thiếu date.
        """

        before = len(df)
        clean_df = df.copy()
        clean_df.columns = [self._snake_case(col) for col in clean_df.columns]
        if 'date' not in clean_df.columns:
            raise ValueError('Breadth dataset cần cột date.')
        clean_df['date'] = pd.to_datetime(clean_df['date'], errors='coerce')
        clean_df.dropna(subset=['date'], inplace=True)
        clean_df.sort_values('date', inplace=True)
        for col in ['advancers', 'decliners', 'unchanged']:
            if col not in clean_df.columns:
                clean_df[col] = 0
        clean_df[['advancers', 'decliners', 'unchanged']] = clean_df[
            ['advancers', 'decliners', 'unchanged']
        ].apply(pd.to_numeric, errors='coerce').fillna(0)
        clean_df['total_issues'] = (
            clean_df['advancers'] + clean_df['decliners'] + clean_df['unchanged']
        )
        if (clean_df['total_issues'] == 0).all():
            raise ValueError('Breadth không có tổng mã giao dịch hợp lệ.')
        date_index = pd.date_range(clean_df['date'].min(), clean_df['date'].max(), freq='D')
        clean_df = clean_df.set_index('date').reindex(date_index).fillna(
            {'advancers': 0, 'decliners': 0, 'unchanged': 0}
        )
        clean_df.index.name = 'date'
        clean_df.reset_index(inplace=True)
        clean_df['total_issues'] = (
            clean_df['advancers'] + clean_df['decliners'] + clean_df['unchanged']
        )
        if clean_df['total_issues'].eq(0).all():
            raise ValueError('Breadth toàn bộ giá trị 0 sau khi fill.')
        clean_df['breadth_percent'] = clean_df.get('breadth_percent')
        if clean_df['breadth_percent'].isnull().all():
            clean_df['breadth_percent'] = (
                (clean_df['advancers'] - clean_df['decliners']) / clean_df['total_issues']
            ) * 100
        clean_df['breadth_percent'] = clean_df['breadth_percent'].clip(-100.0, 100.0)
        if clean_df['breadth_percent'].abs().gt(100).any():
            self.logger.warning('Breadth_percent bất thường: %s', clean_df['breadth_percent'])
        clean_df['adv_dec_ratio'] = (
            clean_df['advancers'] / clean_df['decliners'].replace({0: pd.NA})
        ).fillna(0)
        clean_df.reset_index(drop=True, inplace=True)
        after = len(clean_df)
        self._log_counts('clean_breadth', before, after)
        return clean_df

    def normalize_symbols(self, symbols: Iterable[str]) -> List[str]:
        """
        Chuẩn hóa danh sách symbol: trim, uppercase, validate định dạng và loại bỏ trùng.

        Args:
            symbols: Danh sách symbol thô.
        Returns:
            Danh sách symbol đã normalize.
        Raises:
            ValueError: nếu symbol không đúng định dạng VN (3-4 chữ cái).
        """

        cleaned: List[str] = []
        for raw in symbols:
            if not raw:
                continue
            sym = raw.strip().upper()
            # VN tickers có thể chứa số (ví dụ: A32), nên cho phép A-Z và 0-9.
            if not re.match(r'^[A-Z0-9]{3,5}$', sym):
                raise ValueError(f'Symbol không đúng chuẩn VN: {raw}')
            if sym not in cleaned:
                cleaned.append(sym)
        self.logger.debug('normalize_symbols: %s -> %s', list(symbols), cleaned)
        return cleaned

    @staticmethod
    def _snake_case(value: str) -> str:
        return re.sub(r'[^a-z0-9]+', '_', value.lower()).strip('_')

    def _parse_financial_value(self, value: Any) -> Any:
        if pd.isna(value):
            return pd.NA
        if isinstance(value, (int, float)):
            return value
        text = str(value).strip().lower()
        if text.endswith('%'):
            try:
                return float(text.replace('%', '').strip()) / 100
            except ValueError:
                return pd.NA
        multiplier = 1.0
        if 'tỷ' in text:
            multiplier = 1_000_000_000
        elif 'triệu' in text or 'tr' in text:
            multiplier = 1_000_000
        cleaned = re.sub(r'[^\d\-,\.]+', '', text)
        cleaned = cleaned.replace(',', '')
        try:
            return float(cleaned) * multiplier
        except ValueError:
            return pd.NA
