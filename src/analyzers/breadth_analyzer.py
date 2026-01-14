from __future__ import annotations

from datetime import timedelta
from typing import Any, Dict, Optional

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


class BreadthAnalyzer:
    """Phân tích sức khỏe thị trường dựa trên dữ liệu breadth tổng hợp."""

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def calculate_market_health(
        self,
        breadth_df: pd.DataFrame,
        lookback: int = 20,
    ) -> Dict[str, Optional[float]]:
        """
        Tính toán các chỉ số health dựa trên breadth trong `lookback` ngày gần nhất.
        """

        if breadth_df.empty:
            raise ValueError('breadth_df không được rỗng.')
        df = breadth_df.copy().tail(lookback)
        df['breadth_percent'] = df['breadth_percent'].astype(float)
        health_score = df['breadth_percent'].mean()
        adv_dec_ratio = (
            df['advancers'].sum() / df['decliners'].replace({0: np.nan}).sum()
        )
        new_highs = df['new_highs'].sum() if 'new_highs' in df else 0
        new_lows = df['new_lows'].sum() if 'new_lows' in df else 0
        ratio_new = (new_highs / new_lows) if new_lows > 0 else np.nan
        trend = 'bullish' if health_score >= 50 else 'bearish'
        confidence = min(1.0, abs(health_score - 50) / 50)
        return {
            'health_score': round(health_score, 2),
            'trend': trend,
            'confidence': round(confidence, 2),
            'adv_dec_ratio': round(adv_dec_ratio, 2) if pd.notna(adv_dec_ratio) else None,
            'new_high_low_ratio': round(ratio_new, 2) if pd.notna(ratio_new) else None,
        }

    def detect_market_regime(self, breadth_df: pd.DataFrame) -> Dict[str, str]:
        """
        Phát hiện market regime dựa trên phần trăm mã trên MA20.
        """

        if breadth_df.empty:
            return {'regime': 'unknown', 'note': 'Không có dữ liệu breadth'}
        latest = breadth_df.dropna(subset=['breadth_percent']).iloc[-1]
        score = latest['breadth_percent']
        if score >= 70:
            regime = 'strong bullish'
        elif 50 <= score < 70:
            regime = 'bullish'
        elif 40 <= score < 50:
            regime = 'neutral'
        elif 20 <= score < 40:
            regime = 'bearish'
        else:
            regime = 'strong bearish'
        return {
            'regime': regime,
            'breadth_percent': round(score, 2),
            'timestamp': latest.get('date', latest.get('time')),
        }

    def find_leading_sectors(
        self,
        breadth_df: pd.DataFrame,
        sector_data: pd.DataFrame,
    ) -> pd.DataFrame:
        """
        Xếp hạng các ngành dựa trên thay đổi % gần nhất.
        """

        if sector_data.empty:
            return pd.DataFrame()
        sector_data = sector_data.copy()
        if 'sector' not in sector_data.columns:
            raise ValueError('sector_data cần cột sector.')
        if 'change_pct' not in sector_data.columns:
            raise ValueError('sector_data cần cột change_pct.')
        ranking = sector_data.sort_values('change_pct', ascending=False)
        return ranking[['sector', 'change_pct', 'volume', 'market_cap']].head(10)

    def calculate_correlation(
        self,
        df1: pd.DataFrame,
        df2: pd.DataFrame,
        window: Optional[int] = None,
    ) -> float:
        """
        Tính correlation giữa hai chuỗi giá hoặc index.
        """

        if df1.empty or df2.empty:
            raise ValueError('Cả hai DataFrame phải có dữ liệu.')
        common = pd.merge(
            df1[['time', 'close']],
            df2[['time', 'close']],
            on='time',
            suffixes=('_1', '_2'),
        )
        if common.empty:
            raise ValueError('Không có time chung để tính correlation.')
        if window:
            return common['close_1'].rolling(window).corr(common['close_2']).iloc[-1]
        return common['close_1'].corr(common['close_2'])

    def generate_market_summary(
        self,
        breadth_df: pd.DataFrame,
        price_df: pd.DataFrame,
    ) -> Dict[str, Any]:
        """
        Tổng hợp số liệu thị trường như số mã tăng/giảm, volume so với trung bình, sentiment.
        """

        summary: Dict[str, Any] = {}
        latest = breadth_df.dropna(subset=['date']).iloc[-1]
        summary['advancers'] = int(latest.get('advancers', 0))
        summary['decliners'] = int(latest.get('decliners', 0))
        summary['breadth_percent'] = round(latest.get('breadth_percent', 0), 2)
        if 'ma_20' in price_df.columns and 'ma_50' in price_df.columns:
            latest_price = price_df.iloc[-1]
            summary['above_ma20'] = int(latest_price['close'] >= latest_price['ma_20'])
            summary['above_ma50'] = int(latest_price['close'] >= latest_price['ma_50'])
        volume_mean = price_df['volume'].rolling(20, min_periods=5).mean().iloc[-1]
        summary['volume_vs_avg'] = round(
            price_df['volume'].iloc[-1] / volume_mean,
            2,
        ) if volume_mean > 0 else None
        gainers = price_df.sort_values('close', ascending=False).head(5)[['close']]
        losers = price_df.sort_values('close').head(5)[['close']]
        summary['top_gainers'] = gainers.to_dict('records')
        summary['top_losers'] = losers.to_dict('records')
        sentiment = 50 + (summary['breadth_percent'] - 50) / 2
        summary['market_sentiment'] = round(max(0, min(100, sentiment)), 2)
        return summary
