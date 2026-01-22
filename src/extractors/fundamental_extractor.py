from __future__ import annotations

from datetime import datetime, timedelta
import inspect
import sys
from typing import Any, Dict, List, Optional, Tuple

try:
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
    sys.stderr.reconfigure(encoding='utf-8', errors='replace')  # type: ignore[attr-defined]
except Exception:
    pass

import pandas as pd
import vnstock
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
    default_config: Dict[str, Any] = {
        'config': {
            'period': 'quarterly',
            'get_all': True,
            'report_type_code': None,
        },
        'data_type': 'financial',
    }

    def extract(self, task: ExtractionTask) -> TaskResult:
        """Chạy task fundamental dựa trên data_type và report_type."""

        data_type = task.data_type.lower()
        statement = task.config.get('report_type', 'income_statement')
        period = task.config.get('period', 'quarterly')
        get_all = task.config.get('get_all', True)
        report_type_code = task.config.get('report_type_code')
        self.logger.info(
            'Fundamental extract %s (%s, %s, %s)',
            task.symbol,
            data_type,
            statement,
            period,
        )
        start_time = datetime.utcnow()
        try:
            if data_type == 'financial':
                data = self._cache_fetch(
                    f'financial:{task.symbol}:{statement}:{period}:{get_all}:{report_type_code}',
                    lambda: self.get_financial_report(
                        task.symbol,
                        report_type=statement,
                        period=period,
                        get_all=get_all,
                        report_type_code=report_type_code,
                    ),
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

    def get_financial_report(
        self,
        symbol: str,
        report_type: str = 'income_statement',
        period: str = 'quarterly',
        get_all: bool = True,
        report_type_code: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lấy báo cáo tài chính từ vnstock v3: income_statement, balance_sheet, cash_flow.

        Args:
            symbol: Mã chứng khoán (VD: VNM)
            report_type: Loại báo cáo tài chính (income_statement/balance_sheet/cash_flow)
            period: Kỳ báo cáo (quarterly/annual)
            get_all: True = lấy tất cả kỳ, False = chỉ kỳ gần nhất
            report_type_code: Mã loại báo cáo (nếu API hỗ trợ), VD: Q1/Q2/Q3/Q4/Y

        Returns:
            DataFrame báo cáo tài chính; DataFrame rỗng nếu không có dữ liệu hoặc lỗi.
        """

        statement = (report_type or 'income_statement').strip().lower()
        if statement in {'income', 'income_statement', 'is'}:
            return self.get_income_statement(
                symbol=symbol,
                period=period,
                get_all=get_all,
                report_type=report_type_code,
            )
        if statement in {'balance', 'balance_sheet', 'bs'}:
            return self.get_balance_sheet(
                symbol=symbol,
                period=period,
                get_all=get_all,
                report_type=report_type_code,
            )
        if statement in {'cash_flow', 'cashflow', 'cf'}:
            return self.get_cash_flow(
                symbol=symbol,
                period=period,
                get_all=get_all,
                report_type=report_type_code,
            )

        self.logger.warning(
            'report_type=%s không hợp lệ, fallback sang income_statement (%s)',
            report_type,
            symbol,
        )
        return self.get_income_statement(
            symbol=symbol,
            period=period,
            get_all=get_all,
            report_type=report_type_code,
        )

    def get_income_statement(
        self,
        symbol: str,
        period: str = 'quarterly',
        get_all: bool = True,
        report_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lấy Income Statement theo cú pháp vnstock v3 (module vnstock.financial).

        Args:
            symbol: Mã chứng khoán
            period: quarterly/annual
            get_all: True = lấy tất cả kỳ
            report_type: Loại báo cáo (nếu API hỗ trợ), VD: Q1/Q2/Q3/Q4/Y

        Returns:
            DataFrame Income Statement hoặc DataFrame rỗng nếu lỗi/không có dữ liệu.
        """

        return self._fetch_financial_statement(
            method_name='income_statement',
            symbol=symbol,
            period=period,
            get_all=get_all,
            report_type=report_type,
        )

    def get_balance_sheet(
        self,
        symbol: str,
        period: str = 'quarterly',
        get_all: bool = True,
        report_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lấy Balance Sheet theo cú pháp vnstock v3.

        Args:
            symbol: Mã chứng khoán
            period: quarterly/annual
            get_all: True = lấy tất cả kỳ
            report_type: Loại báo cáo (nếu API hỗ trợ), VD: Q1/Q2/Q3/Q4/Y

        Returns:
            DataFrame Balance Sheet hoặc DataFrame rỗng nếu lỗi/không có dữ liệu.
        """

        return self._fetch_financial_statement(
            method_name='balance_sheet',
            symbol=symbol,
            period=period,
            get_all=get_all,
            report_type=report_type,
        )

    def get_cash_flow(
        self,
        symbol: str,
        period: str = 'quarterly',
        get_all: bool = True,
        report_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Lấy Cash Flow theo cú pháp vnstock v3.

        Args:
            symbol: Mã chứng khoán
            period: quarterly/annual
            get_all: True = lấy tất cả kỳ
            report_type: Loại báo cáo (nếu API hỗ trợ), VD: Q1/Q2/Q3/Q4/Y

        Returns:
            DataFrame Cash Flow hoặc DataFrame rỗng nếu lỗi/không có dữ liệu.
        """

        return self._fetch_financial_statement(
            method_name='cash_flow',
            symbol=symbol,
            period=period,
            get_all=get_all,
            report_type=report_type,
        )

    def get_financial_ratios(self, symbol: str, period: str = 'quarterly') -> pd.DataFrame:
        """
        Lấy các chỉ số tài chính theo cú pháp vnstock v3 (vnstock.financial.ratio).

        Args:
            symbol: Mã chứng khoán
            period: quarterly/annual

        Returns:
            DataFrame ratios hoặc DataFrame rỗng nếu lỗi/không có dữ liệu.
        """

        symbol_normalized = self._normalize_symbol(symbol)
        try:
            financial = getattr(vnstock, 'financial', None)
            func = getattr(financial, 'ratio', None) if financial is not None else None
            if callable(func):
                raw = self._call_with_supported_kwargs(
                    func,
                    {
                        'symbol': symbol_normalized,
                        'period': period,
                    },
                )
            else:
                self.logger.debug(
                    'vnstock.financial.ratio không khả dụng, fallback qua Quote (%s)',
                    symbol_normalized,
                )
                quote = Quote(symbol=symbol_normalized, source='vci')
                raw = self._attempt_methods(quote, ['financial_ratios', 'ratios', 'fundamental_ratios'])
            df = self._to_dataframe(raw)
            df = df.rename(columns={col: col.strip().lower() for col in df.columns})
            numeric_cols = ['pe', 'pb', 'ps', 'roe', 'roa', 'roic', 'eps', 'bvps']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            if df.empty:
                self.logger.warning('Không có dữ liệu ratios cho %s', symbol_normalized)
                return pd.DataFrame()
            return df
        except Exception as exc:
            self.logger.error('Lỗi khi lấy ratios %s: %s', symbol_normalized, exc)
            return pd.DataFrame()

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

    def _fetch_financial_statement(
        self,
        method_name: str,
        symbol: str,
        period: str = 'quarterly',
        get_all: bool = True,
        report_type: Optional[str] = None,
    ) -> pd.DataFrame:
        """
        Wrapper an toàn để gọi vnstock.financial.* và chuẩn hóa kết quả.

        Xử lý các tình huống:
        - Mã mới niêm yết chưa có báo cáo (DataFrame rỗng/None)
        - Mã không tồn tại / API lỗi
        - Lỗi mạng (bị raise exception)
        """

        symbol_normalized = self._normalize_symbol(symbol)
        period_normalized = (period or 'quarterly').strip().lower()

        if period_normalized not in {'quarterly', 'annual'}:
            self.logger.warning(
                'period=%s không hợp lệ, fallback sang quarterly (%s)',
                period,
                symbol_normalized,
            )
            period_normalized = 'quarterly'

        try:
            raw: Any
            financial = getattr(vnstock, 'financial', None)
            func = getattr(financial, method_name, None) if financial is not None else None
            if callable(func):
                raw = self._call_with_supported_kwargs(
                    func,
                    {
                        'symbol': symbol_normalized,
                        'period': period_normalized,
                        'get_all': bool(get_all),
                        'report_type': report_type,
                    },
                )
            else:
                self.logger.debug(
                    'vnstock.financial.%s không khả dụng, fallback qua Quote cho %s',
                    method_name,
                    symbol_normalized,
                )
                raw = self._fetch_statement_via_quote(
                    method_name=method_name,
                    symbol=symbol_normalized,
                    period=period_normalized,
                )

            if raw is None:
                self.logger.warning(
                    'API trả về None cho %s (%s, %s)',
                    symbol_normalized,
                    method_name,
                    period_normalized,
                )
                return pd.DataFrame()

            df = self._to_dataframe(raw)
            if df.empty:
                self.logger.warning(
                    'Không có dữ liệu %s cho %s (có thể mã mới niêm yết hoặc thiếu báo cáo)',
                    method_name,
                    symbol_normalized,
                )
                return df

            df = self._validate_financial_report(df)
            missing = self._check_income_statement_columns_if_needed(method_name, df)
            if missing:
                self.logger.warning(
                    'Dữ liệu %s của %s thiếu cột quan trọng: %s',
                    method_name,
                    symbol_normalized,
                    ', '.join(sorted(missing)),
                )

            self.logger.info(
                'Đã lấy %s cho %s: %s rows (%s)',
                method_name,
                symbol_normalized,
                len(df),
                period_normalized,
            )
            return df
        except Exception as exc:
            self.logger.error(
                'Lỗi khi gọi vnstock.financial.%s cho %s: %s',
                method_name,
                symbol_normalized,
                exc,
            )
            return pd.DataFrame()

    def _fetch_statement_via_quote(self, method_name: str, symbol: str, period: str) -> Any:
        """Fallback lấy báo cáo qua Quote cho các phiên bản vnstock không có vnstock.financial."""

        quote = Quote(symbol=symbol, source='vci')
        candidates_map = {
            'income_statement': [
                'income_statement',
                'income_statement_quarterly',
                'income_statement_annual',
                'financials',
            ],
            'balance_sheet': [
                'balance_sheet',
                'balance_sheet_quarterly',
                'balance_sheet_annual',
            ],
            'cash_flow': [
                'cash_flow',
                'cash_flow_quarterly',
                'cash_flow_annual',
            ],
        }
        candidates = candidates_map.get(method_name, candidates_map['income_statement'])
        if period == 'annual':
            candidates = [name for name in candidates if 'annual' in name] + candidates
        return self._attempt_methods(quote, candidates)

    def _call_with_supported_kwargs(self, func: Any, kwargs: Dict[str, Any]) -> Any:
        """Chỉ truyền các kwargs mà function signature hỗ trợ để tránh crash do lệch phiên bản."""

        signature = inspect.signature(func)
        supported = set(signature.parameters.keys())
        filtered = {key: value for key, value in kwargs.items() if key in supported and value is not None}
        return func(**filtered)

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
        non_numeric = {
            'ticker',
            'symbol',
            'time',
            'date',
            'quarter',
            'year',
            'report_type',
            'period',
        }
        numeric_columns = [col for col in df.columns if col not in non_numeric]
        if numeric_columns:
            df[numeric_columns] = df[numeric_columns].apply(pd.to_numeric, errors='coerce')
        if df.empty:
            raise ValueError('Báo cáo tài chính không có dữ liệu hợp lệ')
        return df

    def _check_income_statement_columns_if_needed(self, method_name: str, df: pd.DataFrame) -> List[str]:
        """Kiểm tra nhẹ các cột thường dùng; chỉ warning để không làm gãy pipeline."""

        if method_name != 'income_statement':
            return []
        required_columns = {'time', 'revenue', 'profit', 'eps'}
        missing = [col for col in required_columns if col not in df.columns]
        return missing

    def _normalize_symbol(self, symbol: str) -> str:
        """Chuẩn hóa mã chứng khoán (strip + upper)."""

        return (symbol or '').strip().upper()

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
