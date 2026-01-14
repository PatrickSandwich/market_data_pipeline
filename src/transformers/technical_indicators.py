from __future__ import annotations

from typing import Iterable, List, Optional

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


class TechnicalIndicators:
    """Tính toán các chỉ số kỹ thuật dựa trên dữ liệu OHLCV."""

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def add_moving_averages(
        self,
        df: pd.DataFrame,
        periods: List[int] = [10, 20, 50, 200],
    ) -> pd.DataFrame:
        """
        Thêm SMA cho các khoảng thời gian nhất định.

        Công thức: ma = close.rolling(period).mean().
        Args:
            df: OHLCV có cột `close`.
            periods: Danh sách khoảng thời gian.
        Returns:
            DataFrame kèm cột `ma_{period}`.
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_moving_averages')
        for period in periods:
            column = f'ma_{period}'
            if len(result) < period:
                self.logger.warning(
                    'Không đủ %s dòng để tính %s', period, column
                )
            result[column] = result['close'].rolling(window=period, min_periods=period).mean()
        return result

    def add_ema(
        self,
        df: pd.DataFrame,
        periods: List[int] = [12, 26],
    ) -> pd.DataFrame:
        """
        Tính Exponential Moving Average.

        EMA công thức: EMA_t = alpha * close_t + (1-alpha) * EMA_{t-1} với alpha=2/(period+1).
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_ema')
        for period in periods:
            column = f'ema_{period}'
            result[column] = result['close'].ewm(span=period, adjust=False, min_periods=period).mean()
        return result

    def add_rsi(self, df: pd.DataFrame, period: int = 14) -> pd.DataFrame:
        """
        Tính RSI: RSI = 100 - 100 / (1 + RS) với RS = avg_gain/avg_loss.
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_rsi')
        delta = result['close'].diff()
        gain = delta.clip(lower=0)
        loss = -delta.clip(upper=0)
        avg_gain = gain.rolling(window=period, min_periods=period).mean()
        avg_loss = loss.rolling(window=period, min_periods=period).mean()
        rs = avg_gain / avg_loss.replace({0: np.nan})
        result['rsi'] = 100 - (100 / (1 + rs))
        result['rsi_signal'] = np.select(
            [result['rsi'] > 70, result['rsi'] < 30],
            ['overbought', 'oversold'],
            default='neutral',
        )
        return result

    def add_macd(
        self,
        df: pd.DataFrame,
        fast: int = 12,
        slow: int = 26,
        signal: int = 9,
    ) -> pd.DataFrame:
        """
        Tính MACD line và histogram.
        MACD = EMA_fast - EMA_slow, signal = EMA(macd, signal), hist = macd - signal.
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_macd')
        ema_fast = result['close'].ewm(span=fast, adjust=False, min_periods=fast).mean()
        ema_slow = result['close'].ewm(span=slow, adjust=False, min_periods=slow).mean()
        result['macd'] = ema_fast - ema_slow
        result['macd_signal'] = result['macd'].ewm(span=signal, adjust=False, min_periods=signal).mean()
        result['macd_hist'] = result['macd'] - result['macd_signal']
        return result

    def add_bollinger_bands(
        self,
        df: pd.DataFrame,
        period: int = 20,
        std: int = 2,
    ) -> pd.DataFrame:
        """
        Tính Bollinger Bands và các chỉ số bổ trợ.
        upper = SMA + std * duha, lower = SMA - std * duha.
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_bollinger_bands')
        if len(result) < period:
            self.logger.warning(
                'Không đủ dữ liệu để tính Bollinger bands period=%s', period
            )
        middle = result['close'].rolling(window=period, min_periods=period).mean()
        deviation = result['close'].rolling(window=period, min_periods=period).std()
        result['bb_middle'] = middle
        result['bb_upper'] = middle + std * deviation
        result['bb_lower'] = middle - std * deviation
        result['bb_width'] = (
            (result['bb_upper'] - result['bb_lower'])
            / result['bb_middle'].replace({0: np.nan})
        )
        result['bb_position'] = (
            (result['close'] - result['bb_lower'])
            / (result['bb_upper'] - result['bb_lower']).replace({0: np.nan})
        ).clip(0, 1)
        return result

    def add_volatility(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
        """
        Tính ATR và volatility ratio.
        ATR = rolling mean of True Range.
        """

        result = df.copy()
        self._ensure_columns(result, ['high', 'low', 'close'], 'add_volatility')
        high = result['high']
        low = result['low']
        prev_close = result['close'].shift(1)
        tr = pd.concat(
            [
                high - low,
                (high - prev_close).abs(),
                (low - prev_close).abs(),
            ],
            axis=1,
        ).max(axis=1)
        result['atr'] = tr.rolling(window=period, min_periods=period).mean()
        result['close_std'] = result['close'].rolling(window=period, min_periods=period).std()
        result['volatility_ratio'] = result['atr'] / result['close'].replace({0: np.nan})
        return result

    def add_volume_metrics(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Thêm các chỉ số volume: SMA, OBV, volume-price trend.
        """

        result = df.copy()
        self._ensure_columns(result, ['close', 'volume'], 'add_volume_metrics')
        result['vol_sma_10'] = result['volume'].rolling(window=10, min_periods=10).mean()
        result['vol_sma_20'] = result['volume'].rolling(window=20, min_periods=20).mean()
        result['volume_ratio'] = result['volume'] / result['volume'].rolling(window=20, min_periods=20).mean()
        direction = np.sign(result['close'].diff()).fillna(0)
        result['obv'] = (direction * result['volume']).cumsum()
        result['volume_price_trend'] = result['volume'] * result['close'].diff()
        return result

    def add_price_changes(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        Tính các tỷ lệ thay đổi giá hàng ngày và momentum theo chu kỳ.
        """

        result = df.copy()
        self._ensure_columns(result, ['close'], 'add_price_changes')
        result['daily_return_pct'] = result['close'].pct_change()
        result['daily_return_abs'] = result['close'].diff()
        result['cumulative_return'] = (1 + result['daily_return_pct']).cumprod() - 1
        horizons = {
            'momentum_1m': 21,
            'momentum_3m': 63,
            'momentum_6m': 126,
        }
        for label, lookback in horizons.items():
            if len(result) < lookback:
                self.logger.warning(
                    'Không đủ dữ liệu để tính %s (yêu cầu %s bản ghi)', label, lookback
                )
                result[label] = np.nan
                continue
            result[label] = result['close'] / result['close'].shift(lookback) - 1
        result['momentum_ytd'] = self._compute_ytd_momentum_series(result)
        ma_columns = [col for col in result.columns if col.startswith('ma_')]
        for ma_col in ma_columns:
            dist_col = f'dist_{ma_col}'
            result[dist_col] = (result['close'] - result[ma_col]) / result[ma_col].replace({0: np.nan})
        return result

    def _ensure_columns(self, df: pd.DataFrame, columns: Iterable[str], context: str) -> None:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            message = f'{context} yêu cầu các cột: {missing}'
            self.logger.warning(message)
            raise ValueError(message)

    def _compute_ytd_momentum_series(self, df: pd.DataFrame) -> pd.Series:
        time_col = self._infer_time_column(df)
        metrics = pd.Series(np.nan, index=df.index)
        if time_col is None:
            self.logger.warning('Không tìm thấy cột thời gian để tính YTD momentum.')
            return metrics
        df_time = pd.to_datetime(df[time_col], errors='coerce')
        if df_time.isna().all():
            return metrics
        current_year = pd.Timestamp.now().year
        mask = df_time.dt.year == current_year
        if not mask.any():
            return metrics
        first_close = df.loc[mask, 'close'].iloc[0]
        metrics.loc[mask] = df.loc[mask, 'close'] / first_close - 1
        return metrics

    def _infer_time_column(self, df: pd.DataFrame) -> Optional[str]:
        for candidate in ['time', 'date', 'datetime', 'timestamp']:
            if candidate in df.columns:
                return candidate
        return None
