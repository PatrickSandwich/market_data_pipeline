from __future__ import annotations

from datetime import datetime
import time
from typing import Any, Dict, List, Optional

import pandas as pd
from vnstock import Quote

from src.utils.decorators import safe_execute
from .base_extractor import BaseExtractor
from .models import ExtractionTask, TaskResult


class BreadthExtractor(BaseExtractor):
    """Extractor dành cho các số đo breadth, thị trường, và giao dịch nước ngoài."""

    name = 'breadth_extractor'
    supported_data_types = ['breadth', 'market_index', 'foreign_trading']
    default_config: Dict[str, Any] = {'config': {}, 'data_type': 'breadth'}

    def extract(self, task: ExtractionTask) -> TaskResult:
        """Chạy task breadth tùy theo `data_type`."""

        self.logger.info('Breadth extract %s (%s)', task.symbol, task.data_type)
        start_time = time.monotonic()
        loader: Optional[pd.DataFrame] = None
        try:
            data_type = task.data_type.lower()
            if data_type == 'breadth':
                loader = self.get_market_breadth()
            elif data_type == 'market_index':
                loader = self.get_sector_performance()
            elif data_type == 'foreign_trading':
                loader = self.get_foreign_trading()
            else:
                raise ValueError(f'Unsupported breadth data_type: {task.data_type}')

            if loader is None:
                loader = pd.DataFrame()

            execution_time = time.monotonic() - start_time
            self.logger.info(
                'Breadth %s cho %s trả về %s dòng',
                data_type,
                task.symbol,
                len(loader),
            )
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=True,
                data=loader,
                row_count=len(loader),
                execution_time=execution_time,
            )
        except Exception as exc:
            self.logger.error('Breadth extractor thất bại %s: %s', task.symbol, exc)
            execution_time = time.monotonic() - start_time
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=False,
                error=str(exc),
                row_count=0,
                execution_time=execution_time,
            )

    @safe_execute(default=pd.DataFrame())
    def get_market_breadth(self) -> pd.DataFrame:
        """Lấy các chỉ số breadth hàng ngày cho thị trường."""

        quote = Quote(source='vci')
        raw = self._call_quote_method(quote, ['market_breadth', 'breadth', 'advance_decline'])
        df = self._normalize_dataframe(raw)
        if df.empty:
            self.logger.warning('Không có dữ liệu market breadth hiện tại.')
            return df
        df = self._normalize_market_breadth(df)
        return df

    @safe_execute(default=pd.DataFrame())
    def get_foreign_trading(self) -> pd.DataFrame:
        """Lấy dữ liệu giao dịch của nhà đầu tư nước ngoài."""

        quote = Quote(source='vci')
        raw = self._call_quote_method(
            quote,
            ['foreign_trading', 'foreign_transactions', 'foreign_flow'],
        )
        df = self._normalize_dataframe(raw)
        if df.empty:
            self.logger.warning('Không có dữ liệu foreign trading.')
            return df
        df = self._normalize_foreign_trading(df)
        return df

    @safe_execute(default=pd.DataFrame())
    def get_sector_performance(self) -> pd.DataFrame:
        """Lấy hiệu suất ngành/chỉ số thị trường."""

        quote = Quote(source='vci')
        raw = self._call_quote_method(
            quote,
            ['sector_performance', 'industry_performance', 'market_index_performance'],
        )
        df = self._normalize_dataframe(raw)
        if df.empty:
            self.logger.warning('Không có dữ liệu sector performance.')
            return df
        df = self._normalize_sector_performance(df)
        return df

    def _call_quote_method(self, quote: Quote, names: List[str]) -> Any:
        """Thử tuần tự nhiều method để tương thích với phiên bản vnstock hiện tại."""

        for name in names:
            attr = getattr(quote, name, None)
            if callable(attr):
                try:
                    return attr()
                except Exception as exc:  # pragma: no cover - ngoại lệ runtime
                    self.logger.debug('Method %s thất bại: %s', name, exc)
            elif attr is not None:
                return attr
        self.logger.warning('Không tìm thấy method breadth trong vnstock: %s', names)
        return None

    def _normalize_dataframe(self, raw: Any) -> pd.DataFrame:
        """Chuẩn hóa kết quả đầu vào thành DataFrame."""

        if isinstance(raw, pd.DataFrame):
            return raw.copy()
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            return pd.DataFrame([raw])
        return pd.DataFrame()

    def _normalize_market_breadth(self, df: pd.DataFrame) -> pd.DataFrame:
        """Đảm bảo các cột breadth và tính toán các chỉ số bổ sung."""

        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        df['date'] = pd.to_datetime(df.get('date') or df.get('day'), errors='coerce')
        if df['date'].isna().all():
            self.logger.warning('Dữ liệu breadth không có ngày hợp lệ.')
        numeric_cols = ['advancers', 'decliners', 'unchanged', 'new_highs', 'new_lows']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                df[col] = 0
        df['advancers'] = df['advancers'].fillna(0)
        df['decliners'] = df['decliners'].fillna(0)
        df['unchanged'] = df['unchanged'].fillna(0)
        df['total_issues'] = df['advancers'] + df['decliners'] + df['unchanged']
        if 'breadth_percent' not in df.columns:
            df['breadth_percent'] = (
                (df['advancers'] - df['decliners']) / df['total_issues'].replace(0, pd.NA)
            ) * 100
        df['adv_dec_ratio'] = (
            df['advancers'] / df['decliners'].replace({0: pd.NA})
        ).fillna(0)
        df['breadth_percent'] = df['breadth_percent'].clip(-100.0, 100.0)
        if (df['breadth_percent'].abs() > 100.0).any():
            self.logger.warning('breadth_percent vượt ngưỡng 100%%: %s', df['breadth_percent'])
        if 'percent_above_ma20' not in df.columns:
            df['percent_above_ma20'] = pd.NA
        if 'percent_above_ma50' not in df.columns:
            df['percent_above_ma50'] = pd.NA
        df = df.dropna(subset=['date'])
        df = df.sort_values(by='date', ascending=False)
        return df

    def _normalize_foreign_trading(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa bảng dữ liệu giao dịch nước ngoài."""

        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        df['date'] = pd.to_datetime(df.get('date') or df.get('day'), errors='coerce')
        numeric_cols = ['net_buy', 'net_sell', 'value_buy', 'value_sell', 'volume']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=['date'])
        df = df.sort_values(by='date', ascending=False)
        return df

    def _normalize_sector_performance(self, df: pd.DataFrame) -> pd.DataFrame:
        """Chuẩn hóa dữ liệu hiệu suất ngành."""

        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        df['sector'] = df.get('sector') or df.get('industry') or df.get('index')
        df['sector'] = df['sector'].astype(str)
        df['change_pct'] = pd.to_numeric(df.get('change_pct') or df.get('change') or df.get('percent'), errors='coerce')
        df['volume'] = pd.to_numeric(df.get('volume'), errors='coerce')
        df['market_cap'] = pd.to_numeric(df.get('market_cap') or df.get('capitalization'), errors='coerce')
        df['date'] = pd.to_datetime(df.get('date'), errors='coerce')
        df = df.dropna(subset=['sector'])
        df = df.sort_values(by='change_pct', ascending=False)
        return df
