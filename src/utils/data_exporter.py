from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from tempfile import NamedTemporaryFile
from typing import Any, Dict, List, Optional

import pandas as pd

from src.utils.file_utils import ensure_dir
from src.utils.logger import get_logger


class DataExporter:
    """Export/Import dữ liệu DataFrame ra các định dạng lưu trữ phổ biến."""

    def __init__(self, processed_dir: str = 'data/processed') -> None:
        self.logger = get_logger(__name__)
        self.processed_dir = Path(processed_dir)

    def export_to_parquet(self, data: pd.DataFrame, filepath: str) -> bool:
        """
        Export DataFrame sang Parquet (gzip) và validate sau khi ghi.

        Args:
            data: DataFrame cần export.
            filepath: Đường dẫn file parquet đích.

        Returns:
            True nếu export thành công và validate OK, ngược lại False.
        """

        path = Path(filepath)
        if not ensure_dir(str(path.parent)):
            return False
        try:
            data.to_parquet(path, compression='gzip', index=False)
        except Exception as exc:
            self.logger.error('Export parquet thất bại %s: %s', path, exc)
            return False

        try:
            if not path.exists() or path.stat().st_size == 0:
                self.logger.error('Parquet output không tồn tại hoặc rỗng: %s', path)
                return False
            reloaded = pd.read_parquet(path)
            if reloaded.shape[0] != data.shape[0]:
                self.logger.warning(
                    'Validate parquet mismatch rows: %s != %s (%s)',
                    reloaded.shape[0],
                    data.shape[0],
                    path,
                )
            return True
        except Exception as exc:
            self.logger.error('Validate parquet thất bại %s: %s', path, exc)
            return False

    def export_to_csv(
        self,
        data: pd.DataFrame,
        filepath: str,
        atomic: bool = True,
        delimiter: str = ',',
        encoding: str = 'utf-8',
    ) -> bool:
        """
        Export DataFrame sang CSV.

        Args:
            data: DataFrame cần export.
            filepath: Đường dẫn file CSV.
            atomic: Nếu True sẽ ghi qua file tạm và replace để đảm bảo atomic write.
            delimiter: Ký tự phân tách (mặc định ',').
            encoding: Encoding khi ghi.

        Returns:
            True nếu thành công, ngược lại False.
        """

        path = Path(filepath)
        if not ensure_dir(str(path.parent)):
            return False
        try:
            if not atomic:
                data.to_csv(path, index=False, sep=delimiter, encoding=encoding)
                return True

            with NamedTemporaryFile(
                mode='w',
                newline='',
                delete=False,
                dir=str(path.parent),
                suffix='.tmp',
                encoding=encoding,
            ) as tmp:
                data.to_csv(tmp.name, index=False, sep=delimiter)
                tmp.flush()
                os.fsync(tmp.fileno())
                tmp_name = tmp.name
            os.replace(tmp_name, path)
            return True
        except Exception as exc:
            self.logger.error('Export CSV thất bại %s: %s', path, exc)
            return False

    def export_all_symbols(
        self,
        symbols: List[str],
        output_dir: str,
        format: str = 'parquet',
        max_workers: Optional[int] = None,
    ) -> Dict[str, Any]:
        """
        Export dữ liệu cho nhiều symbols cùng lúc (parallel).

        Mặc định đọc dữ liệu từ `processed_dir/{symbol}.parquet` (hoặc fallback `{symbol}.csv`)
        và export sang output_dir với định dạng yêu cầu.

        Args:
            symbols: Danh sách mã.
            output_dir: Thư mục output.
            format: 'parquet' hoặc 'csv'.
            max_workers: Số workers cho ThreadPoolExecutor.

        Returns:
            Summary dict gồm số thành công/thất bại và chi tiết theo symbol.
        """

        output = Path(output_dir)
        ensure_dir(str(output))
        target_format = format.lower().strip()
        if target_format not in ('parquet', 'csv'):
            raise ValueError('format chỉ hỗ trợ parquet hoặc csv.')

        results: Dict[str, Dict[str, Any]] = {}

        def export_one(sym: str) -> Dict[str, Any]:
            df = self._load_processed(sym)
            if df is None or df.empty:
                return {'success': False, 'error': 'missing_or_empty_input'}
            out_path = output / f'{sym}.{ "parquet" if target_format == "parquet" else "csv"}'
            if target_format == 'parquet':
                ok = self.export_to_parquet(df, str(out_path))
            else:
                ok = self.export_to_csv(df, str(out_path), atomic=True)
            return {'success': ok, 'output': str(out_path)}

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {executor.submit(export_one, sym): sym for sym in symbols}
            for future in as_completed(futures):
                sym = futures[future]
                try:
                    results[sym] = future.result()
                except Exception as exc:
                    results[sym] = {'success': False, 'error': str(exc)}

        succeeded = [s for s, r in results.items() if r.get('success')]
        failed = [s for s, r in results.items() if not r.get('success')]
        return {
            'total': len(symbols),
            'succeeded': len(succeeded),
            'failed': len(failed),
            'details': results,
        }

    def import_from_parquet(self, filepath: str) -> pd.DataFrame:
        """
        Import DataFrame từ Parquet và validate kiểu dữ liệu cơ bản.

        Args:
            filepath: Đường dẫn file parquet.

        Returns:
            DataFrame (có thể rỗng nếu lỗi).
        """

        path = Path(filepath)
        try:
            df = pd.read_parquet(path)
        except Exception as exc:
            self.logger.error('Import parquet thất bại %s: %s', path, exc)
            return pd.DataFrame()

        df = df.copy()
        for col in ['open', 'high', 'low', 'close', 'volume']:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')
            else:
                self.logger.warning('Thiếu cột %s trong file %s', col, path)
        for time_col in ['time', 'date', 'datetime', 'timestamp']:
            if time_col in df.columns:
                df[time_col] = pd.to_datetime(df[time_col], errors='coerce')
                break
        return df

    def _load_processed(self, symbol: str) -> Optional[pd.DataFrame]:
        parquet_path = self.processed_dir / f'{symbol}.parquet'
        csv_path = self.processed_dir / f'{symbol}.csv'
        try:
            if parquet_path.exists():
                return pd.read_parquet(parquet_path)
            if csv_path.exists():
                return pd.read_csv(csv_path)
        except Exception as exc:
            self.logger.warning('Không đọc được processed data %s: %s', symbol, exc)
        return None
