from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import pandas as pd
from vnstock import Listing

from src.utils.logger import get_logger


@dataclass(frozen=True)
class MarketScopeConfig:
    """Cấu hình cho việc lọc phạm vi thị trường.

    Attributes:
        scope: Phạm vi lọc: "all", "core", "hsx_only", "hsx_hnx".
        upcom_max_symbols: Số lượng mã UPCOM tối đa khi scope="core".
        upcom_sort_by: Tên cột dùng để xếp hạng thanh khoản (nếu có).
        include_exchanges: Danh sách sàn sẽ được giữ lại sau khi lọc.
    """

    scope: str
    upcom_max_symbols: int = 50
    upcom_sort_by: str = 'avg_value'
    include_exchanges: Optional[List[str]] = None

    def normalized_scope(self) -> str:
        """Chuẩn hóa tên scope về lowercase."""

        return str(self.scope or '').strip().lower() or 'all'

    def normalized_exchanges(self) -> List[str]:
        """Chuẩn hóa danh sách sàn về uppercase."""

        exchanges = self.include_exchanges or []
        return [str(x).strip().upper() for x in exchanges if str(x).strip()]

    @classmethod
    def from_config(cls, config: Dict[str, Any]) -> 'MarketScopeConfig':
        """Khởi tạo MarketScopeConfig từ cấu hình YAML đã load.

        Args:
            config: Cấu hình hợp nhất từ ConfigLoader.

        Returns:
            MarketScopeConfig đã được map theo các key phổ biến trong dự án.
        """

        scope = 'all'
        market_scope = config.get('market_scope')
        if isinstance(market_scope, dict) and isinstance(market_scope.get('scope'), str):
            scope = market_scope.get('scope', scope)
        elif isinstance(config.get('market_scope_filter'), str):
            scope = config.get('market_scope_filter', scope)
        elif isinstance(market_scope, str):
            scope = market_scope

        settings = config.get('market_scope_settings') or {}
        try:
            upcom_max = int(settings.get('upcom_max_symbols', 50))
        except Exception:
            upcom_max = 50
        upcom_sort_by = str(settings.get('upcom_sort_by', 'avg_value'))

        include_map = settings.get('include_exchanges') or {}
        included = None
        if isinstance(include_map, dict):
            included = include_map.get(str(scope).strip().lower())
        if included is not None and not isinstance(included, list):
            included = None

        return cls(
            scope=str(scope),
            upcom_max_symbols=upcom_max,
            upcom_sort_by=upcom_sort_by,
            include_exchanges=included,
        )


class MarketScopeFilter:
    """Lọc danh sách mã theo phạm vi thị trường để tối ưu hiệu năng pipeline."""

    SCOPE_EXCHANGE_MAP: Dict[str, List[str]] = {
        'all': ['HSX', 'HNX', 'UPCOM'],
        'core': ['HSX', 'HNX', 'UPCOM'],
        'hsx_only': ['HSX'],
        'hsx_hnx': ['HSX', 'HNX'],
    }

    def __init__(self, config: MarketScopeConfig) -> None:
        self.config = config
        self.logger = get_logger(self.__class__.__name__)

    def filter_symbols(
        self,
        all_symbols_df: pd.DataFrame,
        universe_symbols: Optional[List[str]] = None,
    ) -> List[str]:
        """Lọc danh sách symbols theo cấu hình scope.

        Args:
            all_symbols_df: DataFrame chứa tất cả symbols từ vnstock listing.
                Cột bắt buộc: `symbol`, `exchange`.
                Cột tuỳ chọn: `avg_volume`, `avg_value`, `market_cap`.
            universe_symbols: Nếu truyền vào, chỉ lọc trên tập symbols này (giúp
                kết hợp với MarketScanner/cache).

        Returns:
            Danh sách symbols sau khi lọc (uppercase, unique, giữ thứ tự ổn định).

        Raises:
            ValueError: nếu thiếu các cột bắt buộc.
        """

        if all_symbols_df is None or all_symbols_df.empty:
            raise ValueError('all_symbols_df rỗng, không thể lọc market scope.')

        df = all_symbols_df.copy()
        df.columns = [str(c).strip().lower() for c in df.columns]
        if 'symbol' not in df.columns or 'exchange' not in df.columns:
            raise ValueError('all_symbols_df phải có cột symbol và exchange.')

        df['symbol'] = df['symbol'].astype(str).str.strip().str.upper()
        df['exchange'] = df['exchange'].astype(str).str.strip().str.upper()

        if universe_symbols:
            universe_set = {str(s).strip().upper() for s in universe_symbols if str(s).strip()}
            df = df[df['symbol'].isin(universe_set)]

        scope = self.config.normalized_scope()
        self.logger.info('Bắt đầu lọc symbols với scope=%s', scope)

        df = self._filter_by_exchange(df, scope)
        if scope == 'core':
            df = self._filter_upcom_by_liquidity(df)

        symbols = df['symbol'].dropna().astype(str).tolist()
        unique_ordered = list(dict.fromkeys(symbols))

        try:
            exchange_counts = df['exchange'].value_counts().to_dict()
        except Exception:
            exchange_counts = {}

        self.logger.info('Đã lọc scope=%s. Tổng symbols: %s. Chi tiết: %s', scope, len(unique_ordered), exchange_counts)
        return unique_ordered

    def _filter_by_exchange(self, df: pd.DataFrame, scope: str) -> pd.DataFrame:
        """Lọc theo danh sách sàn được bao gồm trong scope."""

        included_exchanges = self.config.normalized_exchanges()
        if not included_exchanges:
            included_exchanges = self.SCOPE_EXCHANGE_MAP.get(scope, ['HSX', 'HNX'])
        return df[df['exchange'].isin(included_exchanges)]

    def _filter_upcom_by_liquidity(self, df: pd.DataFrame) -> pd.DataFrame:
        """Lọc UPCOM theo thanh khoản: chỉ giữ top N mã.

        Nếu không có cột thanh khoản để sort, fallback về lấy tối đa N mã đầu tiên
        (theo thứ tự đang có trong DataFrame).
        """

        hsx_hnx = df[df['exchange'].isin(['HSX', 'HNX'])]
        upcom = df[df['exchange'] == 'UPCOM']
        if upcom.empty:
            return df

        limit = max(1, int(self.config.upcom_max_symbols))
        sort_column = str(self.config.upcom_sort_by or '').strip().lower()

        if sort_column and sort_column in upcom.columns:
            upcom_sorted = upcom.copy()
            upcom_sorted[sort_column] = pd.to_numeric(upcom_sorted[sort_column], errors='coerce')
            upcom_sorted = upcom_sorted.dropna(subset=[sort_column])
            if upcom_sorted.empty:
                self.logger.warning(
                    'Cột %s không có dữ liệu số hợp lệ; dùng top %s mã UPCOM đầu tiên.',
                    sort_column,
                    limit,
                )
                upcom_filtered = upcom.head(limit)
            else:
                upcom_filtered = upcom_sorted.nlargest(limit, sort_column)
        else:
            self.logger.warning(
                'Không có cột %s để sort; dùng top %s mã UPCOM đầu tiên.',
                sort_column or '(none)',
                limit,
            )
            upcom_filtered = upcom.head(limit)

        removed = len(upcom) - len(upcom_filtered)
        if removed > 0:
            self.logger.info(
                'UPCOM: Giữ %s/%s mã (top %s). Bỏ %s mã để tối ưu hiệu năng.',
                len(upcom_filtered),
                len(upcom),
                limit,
                removed,
            )

        return pd.concat([hsx_hnx, upcom_filtered], ignore_index=True)

    @staticmethod
    def load_listing_dataframe(source: str = 'vci') -> pd.DataFrame:
        """Tải DataFrame danh sách symbols từ vnstock Listing.

        Args:
            source: Nguồn dữ liệu vnstock (VD: "vci").

        Returns:
            DataFrame chứa ít nhất các cột symbol và exchange.
        """

        listing = Listing(source=source)
        df = listing.symbols_by_exchange()
        return df if isinstance(df, pd.DataFrame) else pd.DataFrame(df)

