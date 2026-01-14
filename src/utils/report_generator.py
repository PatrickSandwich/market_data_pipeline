from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

from src.utils.config_loader import ConfigLoader
from src.utils.file_utils import ensure_dir
from src.utils.logger import get_logger


@dataclass(frozen=True)
class ReportPaths:
    """Quy ước đường dẫn lưu báo cáo."""

    base_dir: Path = Path('reports')

    def daily_dir(self) -> Path:
        return self.base_dir / 'daily'

    def weekly_dir(self) -> Path:
        return self.base_dir / 'weekly'

    def daily_report_path(self, report_date: str) -> Path:
        return self.daily_dir() / f'{report_date}.md'

    def weekly_report_path(self, year_week: str) -> Path:
        return self.weekly_dir() / f'{year_week}.md'


class ReportGenerator:
    """Tạo báo cáo Markdown/HTML từ kết quả pipeline và dữ liệu processed."""

    def __init__(
        self,
        config_path: str = 'config/settings.yaml',
        report_paths: Optional[ReportPaths] = None,
    ) -> None:
        self.logger = get_logger(__name__)
        self.config_path = config_path
        self.report_paths = report_paths or ReportPaths()

    def generate_daily_report(self, pipeline_result: Dict[str, Any], output_path: str) -> str:
        """
        Tạo báo cáo hàng ngày dạng Markdown và lưu ra file.

        Args:
            pipeline_result: Kết quả trả về từ Pipeline (khuyến nghị dùng `run_full_pipeline()`).
            output_path: Đường dẫn file output (nếu là thư mục sẽ tự đặt tên theo ngày).

        Returns:
            Nội dung Markdown của báo cáo.

        Raises:
            ValueError: nếu pipeline_result không có cấu trúc tối thiểu.
        """

        report_date = datetime.utcnow().strftime('%Y-%m-%d')
        daily = pipeline_result.get('daily', {})
        technical_screen = pipeline_result.get('technical_screen', {})
        breadth_health = pipeline_result.get('breadth_health', {})
        breadth_regime = pipeline_result.get('breadth_regime', {})

        output = Path(output_path)
        if output.suffix.lower() != '.md':
            output = output / f'{report_date}.md'
        ensure_dir(str(output.parent))

        technical_rows = self._extract_technical_rows(technical_screen)
        breakout_rows = [
            row for row in technical_rows
            if row.get('breakdown', {}).get('ma_breakout') == 2
            and row.get('breakdown', {}).get('volume', 0) >= 1
        ]
        oversold_rows = [row for row in technical_rows if row.get('breakdown', {}).get('rsi') == 2]
        overbought_rows = [row for row in technical_rows if row.get('breakdown', {}).get('rsi') == -2]

        failed = int(daily.get('failed', 0) or 0)
        total_symbols = int(daily.get('total_symbols', 0) or len(technical_rows))
        summary_signal = self._summarize_recommendations(technical_rows)

        content = '\n'.join(
            [
                f'# 📊 Daily Market Report ({report_date})',
                '',
                '## 🧾 Pipeline Summary',
                f'- Symbols processed: **{total_symbols}**',
                f'- Failed: **{failed}**',
                '',
                '## 🌐 Market Overview (Breadth)',
                self._md_kv(
                    [
                        ('Health score', breadth_health.get('health_score')),
                        ('Trend', breadth_health.get('trend')),
                        ('Confidence', breadth_health.get('confidence')),
                        ('Regime', breadth_regime.get('regime')),
                        ('Breadth %', breadth_regime.get('breadth_percent')),
                    ]
                ),
                '',
                '## 🚀 Breakouts',
                self._md_table(
                    ['symbol', 'overall_signal', 'confidence', 'score'],
                    breakout_rows,
                    empty_message='Không phát hiện breakout theo tiêu chí MA + Volume.',
                ),
                '',
                '## 🧊 Oversold (RSI)',
                self._md_table(
                    ['symbol', 'overall_signal', 'confidence', 'score'],
                    oversold_rows,
                    empty_message='Không có mã oversold theo RSI.',
                ),
                '',
                '## 🔥 Overbought (RSI)',
                self._md_table(
                    ['symbol', 'overall_signal', 'confidence', 'score'],
                    overbought_rows,
                    empty_message='Không có mã overbought theo RSI.',
                ),
                '',
                '## 🏆 Top Gainers / Losers',
                '_Chưa đủ dữ liệu tổng hợp top gainers/losers từ pipeline_result. Có thể bổ sung khi lưu bảng giá index hoặc tính từ processed data._',
                '',
                '## ✅ Recommendations',
                self._md_kv(
                    [
                        ('Buy candidates', summary_signal.get('buy', 0)),
                        ('Sell warnings', summary_signal.get('sell', 0)),
                        ('Neutral', summary_signal.get('neutral', 0)),
                    ]
                ),
                '',
                '---',
                f'_Generated at {datetime.utcnow().isoformat()}Z_',
                '',
            ]
        )

        output.write_text(content, encoding='utf-8')
        self.logger.info('Daily report written: %s', output)
        return content

    def generate_technical_report(self, symbol: str, df: pd.DataFrame, analysis: Dict[str, Any]) -> str:
        """
        Tạo báo cáo kỹ thuật chi tiết cho một mã.

        Args:
            symbol: Mã chứng khoán.
            df: DataFrame OHLCV/indicators (đã clean).
            analysis: Kết quả phân tích từ TechnicalScreener (trend/support/signal,...).

        Returns:
            Nội dung Markdown.
        """

        if df.empty:
            raise ValueError('df rỗng, không thể tạo technical report.')
        latest = df.iloc[-1]

        indicators = self._collect_indicator_snapshot(df)
        support_resistance = analysis.get('support_resistance') or {}
        signal = analysis.get('signal') or analysis
        trend = analysis.get('trend') or {}

        lines: List[str] = [
            f'# 📈 Technical Report: {symbol}',
            '',
            '## Snapshot',
            self._md_kv(
                [
                    ('Close', self._fmt_float(latest.get('close'))),
                    ('Volume', self._fmt_float(latest.get('volume'))),
                    ('RSI', self._fmt_float(latest.get('rsi'))),
                    ('MACD', self._fmt_float(latest.get('macd'))),
                ]
            ),
            '',
            '## Indicators',
            self._md_kv([(k, v) for k, v in indicators]),
            '',
            '## Support / Resistance',
            self._md_kv(
                [
                    ('Resistances', support_resistance.get('resistances')),
                    ('Supports', support_resistance.get('supports')),
                ]
            ),
            '',
            '## Trend',
            self._md_kv(
                [
                    ('Trend', trend.get('trend_type')),
                    ('Strength', trend.get('trend_strength')),
                    ('Confidence', trend.get('confidence')),
                ]
            ),
            '',
            '## Signal Summary',
            self._md_kv(
                [
                    ('Overall', signal.get('overall_signal')),
                    ('Score', signal.get('score')),
                    ('Confidence', signal.get('confidence')),
                ]
            ),
            '',
            '## Risk Assessment',
            self._risk_assessment(df),
            '',
        ]
        return '\n'.join(lines)

    def generate_weekly_summary(self) -> str:
        """
        Tóm tắt tuần giao dịch dựa trên dữ liệu processed hiện có.

        Returns:
            Nội dung Markdown của weekly summary.

        Notes:
            - Hàm sẽ đọc `config/settings.yaml` để lấy symbols và đường dẫn processed.
            - Nếu không có dữ liệu, hàm vẫn trả về báo cáo với cảnh báo.
        """

        config = ConfigLoader(self.config_path).load()
        processed_dir = Path(config['data_paths']['processed'])
        symbols: List[str] = config.get('symbols', [])
        if not symbols:
            raise ValueError('Config thiếu symbols để tạo weekly summary.')

        rows: List[Dict[str, Any]] = []
        for symbol in symbols:
            df = self._load_processed(processed_dir, symbol)
            if df is None or df.empty or 'close' not in df.columns:
                continue
            df = df.copy()
            time_col = 'time' if 'time' in df.columns else ('date' if 'date' in df.columns else None)
            if time_col:
                df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
                df = df.dropna(subset=[time_col])
                df = df.sort_values(time_col)
            if len(df) < 2:
                continue
            week_df = df.tail(5)
            start_close = float(week_df['close'].iloc[0])
            end_close = float(week_df['close'].iloc[-1])
            weekly_return = (end_close / start_close) - 1 if start_close else None
            rows.append(
                {
                    'symbol': symbol,
                    'weekly_return_pct': round((weekly_return or 0) * 100, 2) if weekly_return is not None else None,
                    'last_close': round(end_close, 2),
                }
            )

        year_week = datetime.utcnow().strftime('%Y-W%U')
        output = self.report_paths.weekly_report_path(year_week)
        ensure_dir(str(output.parent))

        sorted_rows = sorted(
            rows,
            key=lambda r: (r.get('weekly_return_pct') is None, -(r.get('weekly_return_pct') or 0)),
        )
        top = sorted_rows[:5]
        bottom = list(reversed(sorted_rows[-5:])) if sorted_rows else []

        content = '\n'.join(
            [
                f'# 🗓️ Weekly Summary ({year_week})',
                '',
                '## ✅ Weekly Performance',
                self._md_table(['symbol', 'weekly_return_pct', 'last_close'], sorted_rows, empty_message='Không có dữ liệu processed để tổng hợp tuần.'),
                '',
                '## 🏆 Top Performers',
                self._md_table(['symbol', 'weekly_return_pct', 'last_close'], top, empty_message='N/A'),
                '',
                '## 🥶 Underperformers',
                self._md_table(['symbol', 'weekly_return_pct', 'last_close'], bottom, empty_message='N/A'),
                '',
                '---',
                f'_Generated at {datetime.utcnow().isoformat()}Z_',
                '',
            ]
        )

        output.write_text(content, encoding='utf-8')
        self.logger.info('Weekly summary written: %s', output)
        return content

    def _extract_technical_rows(self, technical_screen: Dict[str, Any]) -> List[Dict[str, Any]]:
        analysis = technical_screen.get('analysis')
        if isinstance(analysis, list):
            rows: List[Dict[str, Any]] = []
            for item in analysis:
                if not isinstance(item, dict):
                    continue
                technical = item.get('technical', {})
                if isinstance(technical, dict):
                    rows.append({'symbol': item.get('symbol'), **technical})
            return rows
        return []

    def _summarize_recommendations(self, technical_rows: List[Dict[str, Any]]) -> Dict[str, int]:
        counts = {'buy': 0, 'sell': 0, 'neutral': 0}
        for row in technical_rows:
            signal = row.get('overall_signal')
            if signal in counts:
                counts[signal] += 1
        return counts

    def _md_table(self, columns: List[str], rows: List[Dict[str, Any]], empty_message: str) -> str:
        if not rows:
            return f'_{empty_message}_'
        header = '| ' + ' | '.join(columns) + ' |'
        sep = '| ' + ' | '.join(['---'] * len(columns)) + ' |'
        body = []
        for row in rows:
            body.append('| ' + ' | '.join(self._fmt_cell(row.get(c)) for c in columns) + ' |')
        return '\n'.join([header, sep, *body])

    def _md_kv(self, items: List[Tuple[str, Any]]) -> str:
        lines = []
        for key, value in items:
            lines.append(f'- **{key}**: {self._fmt_cell(value)}')
        return '\n'.join(lines)

    def _fmt_cell(self, value: Any) -> str:
        if value is None or (isinstance(value, float) and pd.isna(value)):
            return 'N/A'
        if isinstance(value, (int, float)):
            return str(value)
        if isinstance(value, list):
            return ', '.join(str(x) for x in value) if value else 'N/A'
        return str(value)

    def _fmt_float(self, value: Any) -> Optional[float]:
        try:
            if value is None or (isinstance(value, float) and pd.isna(value)):
                return None
            return float(value)
        except Exception:
            return None

    def _collect_indicator_snapshot(self, df: pd.DataFrame) -> List[Tuple[str, Any]]:
        latest = df.iloc[-1]
        keys = ['ma_20', 'ma_50', 'ma_200', 'ema_12', 'ema_26', 'bb_upper', 'bb_middle', 'bb_lower', 'atr']
        items: List[Tuple[str, Any]] = []
        for key in keys:
            if key in df.columns:
                items.append((key, self._fmt_float(latest.get(key))))
        return items

    def _risk_assessment(self, df: pd.DataFrame) -> str:
        if 'atr' in df.columns and 'close' in df.columns:
            latest = df.iloc[-1]
            atr = self._fmt_float(latest.get('atr'))
            close = self._fmt_float(latest.get('close'))
            if atr is not None and close:
                ratio = atr / close
                if ratio > 0.05:
                    return '⚠️ Volatility cao (ATR/Close > 5%). Cân nhắc giảm vị thế hoặc đặt stop-loss chặt.'
                if ratio > 0.02:
                    return '✅ Volatility trung bình. Có thể quản trị rủi ro bằng stop-loss theo ATR.'
        return 'N/A'

    def _load_processed(self, processed_dir: Path, symbol: str) -> Optional[pd.DataFrame]:
        parquet_path = processed_dir / f'{symbol}.parquet'
        csv_path = processed_dir / f'{symbol}.csv'
        try:
            if parquet_path.exists():
                return pd.read_parquet(parquet_path)
            if csv_path.exists():
                return pd.read_csv(csv_path)
        except Exception as exc:
            self.logger.warning('Không đọc được processed data %s: %s', symbol, exc)
            return None
        return None
