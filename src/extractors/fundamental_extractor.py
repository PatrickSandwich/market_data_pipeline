from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from vnstock import Quote

from src.utils.decorators import safe_execute
from .base_extractor import BaseExtractor
from .models import ExtractionTask, TaskResult

FUNDAMENTAL_TTL = timedelta(hours=24)
FUNDAMENTAL_CACHE: Dict[str, Tuple[datetime, Any]] = {}


class FundamentalExtractor(BaseExtractor):
    """Extractor xử lý dữ liệu fundamental và thông tin cổ phiếu."""

    name = 'fundamental_extractor'
    supported_data_types = ['financial', 'company_info', 'dividend', 'events']
    default_config: Dict[str, Any] = {'config': {}, 'data_type': 'financial'}

    def extract(self, task: ExtractionTask) -> TaskResult:
        """Chạy task fundamental dựa trên data_type và report_type."""

        data_type = task.data_type.lower()
        report_type = task.config.get('report_type', 'income_statement')
        self.logger.info('Fundamental extract %s (%s, %s)', task.symbol, data_type, report_type)
        start_time = datetime.utcnow()
        try:
            if data_type == 'financial':
                data = self._cache_fetch(
                    f'financial:{task.symbol}:{report_type}',
                    lambda: self.get_financial_report(task.symbol, report_type),
                )
            elif data_type == 'company_info':
                data = self._cache_fetch(
                    f'company:{task.symbol}',
                    lambda: self.get_company_overview(task.symbol),
                )
            elif data_type == 'dividend':
                data = self._cache_fetch(
                    f'dividends:{task.symbol}',
                    lambda: self.get_dividends(task.symbol),
                )
            elif data_type == 'events':
                data = self._cache_fetch(
                    f'events:{task.symbol}',
                    lambda: self.get_events(task.symbol),
                )
            else:
                raise ValueError(f'Unsupported fundamental data_type: {data_type}')

            execution_time = (datetime.utcnow() - start_time).total_seconds()
            if isinstance(data, pd.DataFrame):
                row_count = len(data)
            elif isinstance(data, dict):
                row_count = 1
                data = pd.DataFrame([data])
            else:
                row_count = 0
                data = pd.DataFrame()

            self.logger.info(
                'Fundamental %s cho %s thành công (%s rows)',
                data_type,
                task.symbol,
                row_count,
            )
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=True,
                data=data,
                row_count=row_count,
                execution_time=execution_time,
            )
        except Exception as exc:
            self.logger.error('Fundamental extractor lỗi %s: %s', task.symbol, exc)
            execution_time = (datetime.utcnow() - start_time).total_seconds()
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=False,
                error=str(exc),
                row_count=0,
                execution_time=execution_time,
            )

    def _cache_fetch(self, key: str, fetcher: Any) -> Any:
        """Lấy dữ liệu từ cache nếu còn thời hạn, ngược lại gọi fetcher."""

        now = datetime.utcnow()
        cached = FUNDAMENTAL_CACHE.get(key)
        if cached and now - cached[0] < FUNDAMENTAL_TTL:
            return cached[1]
        payload = fetcher()
        FUNDAMENTAL_CACHE[key] = (now, payload)
        return payload

    def get_financial_report(self, symbol: str, report_type: str = 'income_statement') -> pd.DataFrame:
        """Lấy báo cáo tài chính: income_statement, balance_sheet hoặc cash_flow."""

        quote = Quote(symbol=symbol, source='vci')
        report_type = report_type.lower()
        method_map = {
            'income_statement': ['income_statement', 'income_statement_quarterly', 'income_statement_annual', 'financials'],
            'balance_sheet': ['balance_sheet', 'balance_sheet_quarterly', 'balance_sheet_annual'],
            'cash_flow': ['cash_flow', 'cash_flow_quarterly', 'cash_flow_annual'],
        }
        candidates = method_map.get(report_type, method_map['income_statement'])
        result = self._attempt_methods(quote, candidates)
        if result is None:
            raise ValueError(f'Không lấy được báo cáo {report_type} cho {symbol}')
        df = self._to_dataframe(result)
        df = self._validate_financial_report(df)
        return df

    def get_financial_ratios(self, symbol: str, period: str = 'quarterly') -> pd.DataFrame:
        """Lấy các chỉ số tài chính (PE, PB, ROE, ROA, ROIC, EPS, BVPS)."""

        quote = Quote(symbol=symbol, source='vci')
        ratios = self._attempt_methods(quote, ['financial_ratios', 'ratios', 'fundamental_ratios'])
        if ratios is None:
            raise ValueError(f'Không có ratios cho {symbol}')
        df = self._to_dataframe(ratios)
        numeric_cols = ['pe', 'pb', 'ps', 'roe', 'roa', 'roic', 'eps', 'bvps']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
        df = df.dropna(subset=[col for col in numeric_cols if col in df.columns])
        if df.empty:
            raise ValueError(f'Dữ liệu ratios không đủ cho {symbol}')
        return df

    def get_company_overview(self, symbol: str) -> Dict[str, Any]:
        """Lấy thông tin công ty: tên, ngành, sàn, vốn hóa, cổ phiếu lưu hành."""

        quote = Quote(symbol=symbol, source='vci')
        overview = self._attempt_methods(quote, ['overview', 'company', 'profile'])
        if not overview:
            raise ValueError(f'Không có overview cho {symbol}')
        return self._normalize_dict(overview)

    def get_dividends(self, symbol: str, limit: int = 10) -> pd.DataFrame:
        """Lấy lịch sử cổ tức, sắp xếp theo năm gần nhất."""

        quote = Quote(symbol=symbol, source='vci')
        dividends = self._attempt_methods(quote, ['dividends', 'dividend_history'])
        if dividends is None:
            raise ValueError(f'Không có data cổ tức cho {symbol}')
        df = self._to_dataframe(dividends)
        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        if 'ex_date' in df.columns:
            df['ex_date'] = pd.to_datetime(df['ex_date'], errors='coerce')
        df = df.dropna(subset=['ex_date'])
        if 'dividend' in df.columns:
            df['dividend'] = pd.to_numeric(df['dividend'], errors='coerce')
        df = df.sort_values(by='ex_date', ascending=False)
        return df.head(limit)

    def get_events(self, symbol: str) -> pd.DataFrame:
        """Lấy sự kiện của công ty."""

        quote = Quote(symbol=symbol, source='vci')
        events = self._attempt_methods(quote, ['events', 'corporate_actions'])
        if events is None:
            raise ValueError(f'Không tìm thấy events cho {symbol}')
        df = self._to_dataframe(events)
        if 'date' in df.columns:
            df['date'] = pd.to_datetime(df['date'], errors='coerce')
        return df

    def _attempt_methods(self, quote: Quote, method_names: List[str]) -> Optional[Any]:
        """Cố gắng gọi tuần tự các method để tương thích nhiều phiên bản vnstock."""

        for name in method_names:
            target = getattr(quote, name, None)
            if callable(target):
                try:
                    return target()
                except Exception as exc:
                    self.logger.debug('Method %s bốc lỗi: %s', name, exc)
            elif target is not None:
                return target
        return None

    def _to_dataframe(self, raw: Any) -> pd.DataFrame:
        """Chuẩn hóa input thành DataFrame."""

        if isinstance(raw, pd.DataFrame):
            return raw.copy()
        if isinstance(raw, list):
            return pd.DataFrame(raw)
        if isinstance(raw, dict):
            return pd.DataFrame([raw])
        raise ValueError('Không thể chuyển đổi dữ liệu sang DataFrame')

    def _validate_financial_report(self, df: pd.DataFrame) -> pd.DataFrame:
        """Đảm bảo dữ liệu báo cáo tài chính đầy đủ và sạch."""

        df = df.rename(columns={col: col.strip().lower() for col in df.columns})
        numeric_columns = [col for col in df.columns if col not in {'report_type', 'period'}]
        df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        if df.empty:
            raise ValueError('Báo cáo tài chính không có dữ liệu hợp lệ')
        return df

    def _normalize_dict(self, payload: Any) -> Dict[str, Any]:
        """Chuẩn hóa dict bằng cách hạ cột và convert số."""

        if isinstance(payload, dict):
            normalized: Dict[str, Any] = {}
            for key, value in payload.items():
                normalized[key.strip().lower()] = value
            return normalized
        raise ValueError('Overview trả về không phải dict')

    @safe_execute(default=pd.DataFrame())
    def get_intraday(self, symbol: str, interval: str = '15m', limit: int = 100) -> pd.DataFrame:
        """Placeholder intraday dữ liệu fundamental, chỉ phục vụ interface."""

        return pd.DataFrame()
