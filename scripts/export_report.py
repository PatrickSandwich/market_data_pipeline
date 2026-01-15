from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd


def _bootstrap_import_path() -> None:
    repo_root = Path(__file__).resolve().parents[1]
    sys.path.insert(0, str(repo_root))

def _ensure_utf8_console() -> None:
    try:
        sys.stdout.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
        sys.stderr.reconfigure(encoding='utf-8')  # type: ignore[attr-defined]
    except Exception:
        pass


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description='Export báo cáo từ dữ liệu processed')
    parser.add_argument('--config', default='config/pipeline_config.yaml', help='Đường dẫn config YAML')
    parser.add_argument('--symbols', default='all', help="Danh sách mã comma-separated hoặc 'all'")
    parser.add_argument('--format', default='markdown', choices=['markdown', 'html', 'pdf'], help='Định dạng output')
    parser.add_argument('--output', default=None, help='Đường dẫn file output (tự động đặt tên nếu bỏ trống)')
    parser.add_argument('--template', default=None, help='Đường dẫn template (tùy chọn)')
    return parser


def _load_processed(processed_dir: Path, symbol: str) -> Optional[pd.DataFrame]:
    parquet_path = processed_dir / f'{symbol}.parquet'
    csv_path = processed_dir / f'{symbol}.csv'
    try:
        if parquet_path.exists():
            return pd.read_parquet(parquet_path)
        if csv_path.exists():
            return pd.read_csv(csv_path)
    except Exception:
        return None
    return None


def _render_markdown(summary: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append(f"# Market Data Pipeline Report ({summary['generated_at']})")
    lines.append('')
    lines.append('## Technical Signals')
    lines.append('| Symbol | Signal | Confidence | Score |')
    lines.append('|---|---:|---:|---:|')
    for row in summary['technical']:
        lines.append(
            f"| {row['symbol']} | {row.get('overall_signal')} | {row.get('confidence')} | {row.get('score')} |"
        )
    lines.append('')
    return '\\n'.join(lines)


def _render_html(summary: Dict[str, Any]) -> str:
    rows = []
    for row in summary['technical']:
        rows.append(
            f"<tr><td>{row['symbol']}</td><td>{row.get('overall_signal')}</td><td>{row.get('confidence')}</td><td>{row.get('score')}</td></tr>"
        )
    rows_html = '\\n'.join(rows)
    return f"""<!doctype html>
<html>
  <head>
    <meta charset="utf-8">
    <title>Market Data Pipeline Report</title>
    <style>
      body {{ font-family: Arial, sans-serif; margin: 24px; }}
      table {{ border-collapse: collapse; width: 100%; }}
      th, td {{ border: 1px solid #ddd; padding: 8px; }}
      th {{ background: #f4f4f4; }}
    </style>
  </head>
  <body>
    <h1>Market Data Pipeline Report</h1>
    <p>Generated at: {summary['generated_at']}</p>
    <h2>Technical Signals</h2>
    <table>
      <thead>
        <tr><th>Symbol</th><th>Signal</th><th>Confidence</th><th>Score</th></tr>
      </thead>
      <tbody>
        {rows_html}
      </tbody>
    </table>
  </body>
</html>"""


def main() -> int:
    _bootstrap_import_path()
    _ensure_utf8_console()
    args = build_parser().parse_args()

    from src.utils.config_loader import ConfigLoader
    from src.utils.logger import configure_logging, get_logger
    from src.analyzers.technical_screener import TechnicalScreener

    configure_logging(log_dir='logs', log_filename='export_report.log', level=os.getenv('MDP_LOG_LEVEL', 'INFO'))
    logger = get_logger('export_report')

    config = ConfigLoader(args.config).load()
    processed_dir = Path(config['data_paths']['processed'])
    symbols = config['symbols'] if args.symbols.strip().lower() == 'all' else [s.strip().upper() for s in args.symbols.split(',') if s.strip()]
    if not symbols:
        raise SystemExit('Danh sách symbols rỗng.')

    screener = TechnicalScreener()
    technical_rows: List[Dict[str, Any]] = []
    for symbol in symbols:
        df = _load_processed(processed_dir, symbol)
        if df is None or df.empty:
            logger.warning('Không có dữ liệu processed cho %s', symbol)
            continue
        try:
            signal = screener.calculate_signal(df)
            technical_rows.append({'symbol': symbol, **signal})
        except Exception as exc:
            logger.warning('Tính tín hiệu thất bại %s: %s', symbol, exc)

    summary = {
        'generated_at': datetime.utcnow().isoformat(),
        'technical': technical_rows,
    }

    if args.format == 'markdown':
        content = _render_markdown(summary)
        ext = 'md'
    else:
        content = _render_html(summary)
        ext = 'html'

    if args.template:
        template_text = Path(args.template).read_text(encoding='utf-8')
        content = template_text.format(content=content, generated_at=summary['generated_at'])

    output_path = (
        Path(args.output)
        if args.output
        else Path('reports') / f"report_{datetime.utcnow().strftime('%Y%m%dT%H%M%SZ')}.{ext}"
    )
    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(content, encoding='utf-8')
    logger.info('Report written: %s', output_path)

    if args.format == 'pdf':
        pdf_path = output_path.with_suffix('.pdf')
        try:
            import weasyprint  # type: ignore

            weasyprint.HTML(string=content).write_pdf(str(pdf_path))
            logger.info('PDF written: %s', pdf_path)
        except Exception as exc:
            logger.warning('Không thể export PDF (%s). Giữ HTML/Markdown.', exc)
    return 0


if __name__ == '__main__':
    raise SystemExit(main())
