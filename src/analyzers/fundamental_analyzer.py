from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd

from src.utils.logger import get_logger


class FundamentalAnalyzer:
    """Phân tích cơ bản xếp hạng fair value, scoring and red flags."""

    def __init__(self) -> None:
        self.logger = get_logger(__name__)

    def calculate_fair_value(
        self,
        df: pd.DataFrame,
        financials: pd.DataFrame,
        method: str = 'dcf',
    ) -> Dict[str, float]:

        result: Dict[str, float] = {'fair_value': np.nan, 'delta': np.nan}
        price = df['close'].iloc[-1]
        if method == 'dcf':
            cashflows = financials['free_cash_flow'].dropna()
            if len(cashflows) < 3:
                raise ValueError('Financials cần ít nhất 3 period FCF để tính DCF.')
            discount_rate = 0.1
            pv = [(cf / ((1 + discount_rate) ** (i + 1))) for i, cf in enumerate(cashflows)]
            result['fair_value'] = float(np.sum(pv))
        elif method == 'pe_relative':
            pe = df['pe_ratio'].dropna().iloc[-1]
            sector_pe = financials.get('sector_pe', np.nan)
            result['fair_value'] = float(pe / sector_pe * price) if sector_pe else float(pe * 1)
        elif method == 'pb_relative':
            pb = df['pb_ratio'].dropna().iloc[-1]
            sector_pb = financials.get('sector_pb', np.nan)
            result['fair_value'] = float(pb / sector_pb * price) if sector_pb else float(pb * 1)
        else:
            raise ValueError('Unknown fair value method')
        result['delta'] = (result['fair_value'] - price) / price
        return result

    def score_fundamentals(
        self,
        financials: pd.DataFrame,
        sector_averages: Dict[str, float],
    ) -> Dict[str, Any]:

        latest = financials.sort_values('period').iloc[-1]
        score = 0
        breakdown: Dict[str, int] = {}
        if latest['roe'] > sector_averages.get('roe', 0):
            score += 1
            breakdown['roe'] = 1
        if latest['debt_to_equity'] < 1:
            score += 1
            breakdown['de_ratio'] = 1
        if latest['current_ratio'] > 1.5:
            score += 1
            breakdown['current_ratio'] = 1
        if latest['revenue_growth'] > 0.1:
            score += 1
            breakdown['revenue_growth'] = 1
        if latest['gross_margin'] > sector_averages.get('gross_margin', 0):
            score += 1
            breakdown['gross_margin'] = 1
        recommendation = 'buy' if score >= 4 else 'hold'
        return {
            'total_score': score,
            'breakdown': breakdown,
            'recommendation': recommendation,
        }

    def check_red_flags(self, financials: pd.DataFrame) -> List[str]:
        flags: List[str] = []
        recent = financials.sort_values('period', ascending=False).head(5)
        if recent['revenue'].is_monotonic_decreasing:
            flags.append('Revenue giảm liên tiếp')
        if recent['debt'].iloc[0] > recent['assets'].iloc[0]:
            flags.append('Debt vượt assets')
        if recent['cash_flow'].lt(0).sum() >= 3:
            flags.append('Cash flow âm nhiều kỳ')
        if recent.get('auditor_note', '').str.contains('qualified', case=False).any():
            flags.append('Auditor có ý kiến')
        return flags

    def compare_with_sector(
        self,
        symbol: str,
        financials: pd.DataFrame,
        sector_data: pd.DataFrame,
    ) -> Dict[str, Any]:

        latest = financials.sort_values('period').iloc[-1]
        sector_summary = sector_data.describe().astype(float)
        comparison = {k: latest.get(k, np.nan) for k in ['pe', 'pb', 'roe', 'ps', 'debt_to_equity']}
        relative_position = {
            metric: float(
                (latest.get(metric, 0) - sector_summary.loc['50%', metric])
                / sector_summary.loc['50%', metric]
                * 100
            )
            if sector_summary.loc['50%', metric] != 0
            else 0
            for metric in comparison
        }
        return {
            'symbol': symbol,
            'comparison': comparison,
            'relative_position': relative_position,
        }
