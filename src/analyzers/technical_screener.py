from __future__ import annotations

from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


class TechnicalScreener:
    """Bộ lọc kỹ thuật cao cấp hỗ trợ breakout, trend, divergence và screening."""

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def find_breakout(
        self,
        df: pd.DataFrame,
        ma_period: int = 20,
        vol_multiplier: float = 1.5,
    ) -> pd.DataFrame:
        """
        Tìm breakout khi giá cắt lên MA và volume tăng đột biến.
        """

        self._ensure_columns(df, ['close', 'volume'], 'find_breakout')
        data = df.copy()
        data['ma'] = data['close'].rolling(ma_period, min_periods=ma_period).mean()
        data['vol_sma'] = data['volume'].rolling(20, min_periods=20).mean()
        latest = data.iloc[-1]
        prev = data.iloc[-2] if len(data) > 1 else latest
        cond_close = latest['close'] > latest['ma']
        cond_prev = prev['close'] <= prev['ma']
        cond_volume = latest['volume'] > vol_multiplier * latest['vol_sma']
        if not (cond_close and cond_prev and cond_volume):
            return pd.DataFrame()
        breakout = {
            'symbol': data.get('symbol', pd.Series([''], index=data.index)).iloc[-1],
            'date': latest.get('time', latest.get('date', datetime.utcnow())),
            'close': latest['close'],
            'ma': latest['ma'],
            'volume': latest['volume'],
            'vol_sma': latest['vol_sma'],
            'vol_multiplier': latest['volume'] / latest['vol_sma'],
        }
        return pd.DataFrame([breakout])

    def find_support_resistance(
        self,
        df: pd.DataFrame,
        window: int = 10,
    ) -> Dict[str, List[float]]:
        """
        Xác định các mức hỗ trợ/kháng cự theo price channels.
        """

        self._ensure_columns(df, ['high', 'low'], 'find_support_resistance')
        highs = df['high'].rolling(window, min_periods=window).max().dropna()
        lows = df['low'].rolling(window, min_periods=window).min().dropna()
        resistances = highs.tail(3).tolist()
        supports = lows.tail(3).tolist()
        channel = {
            'resistances': sorted(set(resistances), reverse=True),
            'supports': sorted(set(supports)),
            'latest_high': highs.iloc[-1] if not highs.empty else np.nan,
            'latest_low': lows.iloc[-1] if not lows.empty else np.nan,
        }
        return channel

    def check_trend(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Đánh giá xu hướng hiện tại dựa trên vị trí giá với MA và sequence highs/lows.
        """

        self._ensure_columns(df, ['close', 'high', 'low'], 'check_trend')
        data = df.copy()
        data['ma_50'] = data['close'].rolling(50, min_periods=50).mean()
        data['ma_200'] = data['close'].rolling(200, min_periods=200).mean()
        latest = data.iloc[-1]
        prev = data.iloc[-2] if len(data) > 1 else latest
        close = latest['close']
        slope_50 = latest['ma_50'] - prev['ma_50'] if len(data) > 1 else 0
        slope_200 = latest['ma_200'] - prev['ma_200'] if len(data) > 1 else 0
        trend_strength = slope_50 + slope_200
        highs = data['high']
        lows = data['low']
        higher_highs = highs.iloc[-3:].is_monotonic_increasing
        lower_lows = lows.iloc[-3:].is_monotonic_decreasing
        if close > latest['ma_50'] > latest['ma_200'] and slope_50 > 0 and slope_200 > 0:
            trend_type = 'uptrend'
        elif close < latest['ma_50'] < latest['ma_200'] and slope_50 < 0:
            trend_type = 'downtrend'
        else:
            trend_type = 'sideways'
        confidence = np.clip(abs(trend_strength), 0, 1)
        return {
            'trend_type': trend_type,
            'trend_strength': trend_strength,
            'confidence': confidence,
            'higher_highs': bool(higher_highs),
            'lower_lows': bool(lower_lows),
        }

    def find_divergence(
        self,
        df: pd.DataFrame,
        indicator: str = 'rsi',
        lookback: int = 14,
    ) -> List[Dict[str, Any]]:
        """
        Tìm phân kỳ giữa giá và indicator (RSI hoặc MACD).
        """

        if indicator not in df.columns:
            self.logger.warning('Indicator %s không có trong df', indicator)
            return []
        result: List[Dict[str, Any]] = []
        prices = df['close']
        values = df[indicator]
        for idx in range(lookback, len(df)):
            window_prices = prices.iloc[idx - lookback:idx]
            window_values = values.iloc[idx - lookback:idx]
            price_trend = window_prices.iloc[-1] - window_prices.iloc[0]
            indicator_trend = window_values.iloc[-1] - window_values.iloc[0]
            if price_trend > 0 and indicator_trend < 0:
                result.append({
                    'type': 'regular',
                    'index': df.index[idx - 1],
                    'indicator': indicator,
                    'description': 'Price lên mà indicator giảm (regular divergence)',
                })
            if price_trend < 0 and indicator_trend > 0:
                result.append({
                    'type': 'hidden',
                    'index': df.index[idx - 1],
                    'indicator': indicator,
                    'description': 'Price giảm mà indicator tăng (hidden divergence)',
                })
        return result

    def calculate_signal(self, df: pd.DataFrame) -> Dict[str, Any]:
        """
        Tổng hợp tín hiệu từ RSI, MA, MACD và volume để trả về tín hiệu chung.
        """

        self._ensure_columns(df, ['close', 'volume'], 'calculate_signal')
        data = df.copy()
        signal = 0
        breakdown: Dict[str, int] = {}
        if 'rsi' in data.columns:
            rsi = data['rsi'].iloc[-1]
            if rsi < 30:
                signal += 2
                breakdown['rsi'] = 2
            elif rsi > 70:
                signal -= 2
                breakdown['rsi'] = -2
            else:
                breakdown['rsi'] = 0
        if 'ma_20' in data.columns:
            close = data['close'].iloc[-1]
            prev_close = data['close'].iloc[-2] if len(data) > 1 else close
            ma = data['ma_20'].iloc[-1]
            prev_ma = data['ma_20'].iloc[-2] if len(data) > 1 else ma
            if prev_close <= prev_ma < close:
                signal += 2
                breakdown['ma_breakout'] = 2
            elif prev_close >= prev_ma > close:
                signal -= 2
                breakdown['ma_breakout'] = -2
            else:
                breakdown['ma_breakout'] = 0
        if 'macd' in data.columns and 'macd_signal' in data.columns:
            macd = data['macd'].iloc[-1]
            macd_signal = data['macd_signal'].iloc[-1]
            prev_macd = data['macd'].iloc[-2] if len(data) > 1 else macd
            prev_signal = data['macd_signal'].iloc[-2] if len(data) > 1 else macd_signal
            if prev_macd <= prev_signal < macd:
                signal += 2
                breakdown['macd'] = 2
            elif prev_macd >= prev_signal > macd:
                signal -= 2
                breakdown['macd'] = -2
            else:
                breakdown['macd'] = 0
        vol_avg = data['volume'].rolling(20, min_periods=20).mean().iloc[-1]
        if vol_avg and data['volume'].iloc[-1] > vol_avg:
            signal += 1
            breakdown['volume'] = 1
        overall = 'neutral'
        if signal > 1:
            overall = 'buy'
        elif signal < -1:
            overall = 'sell'
        confidence = min(1.0, abs(signal) / 6)
        return {
            'overall_signal': overall,
            'confidence': round(confidence, 2),
            'score': signal,
            'breakdown': breakdown,
        }

    def screen_multiple_stocks(
        self,
        stocks_data: Dict[str, pd.DataFrame],
        criteria: Dict[str, Dict[str, Any]],
    ) -> pd.DataFrame:
        """
        Lọc nhiều mã theo tiêu chí đơn giản (lt, gt, gte, lte, eq, gt_col).
        """

        matched: List[Dict[str, Any]] = []
        for symbol, data in stocks_data.items():
            try:
                self._ensure_columns(
                    data,
                    ['close', 'volume'],
                    'screen_multiple_stocks',
                )
            except ValueError:
                continue
            latest = data.iloc[-1]
            meets = True
            snapshot: Dict[str, Any] = {'symbol': symbol}
            for key, rule in criteria.items():
                val = latest.get(key)
                if val is None and isinstance(rule, dict) and 'col' in rule:
                    target_col = rule['col']
                    val = latest.get(target_col)
                if val is None:
                    meets = False
                    break
                for op, threshold in rule.items():
                    if op == 'lt' and not (val < threshold):
                        meets = False
                    elif op == 'gt' and not (val > threshold):
                        meets = False
                    elif op == 'lte' and not (val <= threshold):
                        meets = False
                    elif op == 'gte' and not (val >= threshold):
                        meets = False
                    elif op == 'eq' and not (val == threshold):
                        meets = False
                snapshot[key] = val
                if not meets:
                    break
            if meets:
                matched.append(snapshot)
        return pd.DataFrame(matched)

    def _ensure_columns(self, df: pd.DataFrame, columns: List[str], context: str) -> None:
        missing = [col for col in columns if col not in df.columns]
        if missing:
            raise ValueError(f'{context} yêu cầu cột {missing}')
