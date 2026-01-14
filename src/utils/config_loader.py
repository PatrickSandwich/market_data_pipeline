import os
from pathlib import Path
from typing import Any, Dict, Union

import yaml


class ConfigValidationError(ValueError):
    """Ngoại lệ được dùng khi cấu hình không thỏa mãn yêu cầu bắt buộc."""


class ConfigLoader:
    """Trình tải cấu hình từ file YAML và môi trường tương ứng."""

    def __init__(
        self,
        config_path: Union[str, Path] = Path('config/settings.yaml'),
    ) -> None:
        self.config_path = Path(config_path)
        self.config: Dict[str, Any] = {}

    def load(self) -> Dict[str, Any]:
        """Đọc và trả về cấu hình đã hợp nhất với override từ biến môi trường."""

        base_config = self._read_yaml()
        override_config = self._env_overrides()
        merged_config = self._merge_dicts(base_config, override_config)
        self._validate(merged_config)
        self.config = merged_config
        return merged_config

    def _read_yaml(self) -> Dict[str, Any]:
        resolved_path = self._resolve_config_path(self.config_path)
        if not resolved_path.exists():
            raise FileNotFoundError(f'Không tìm thấy file cấu hình: {resolved_path}')

        with resolved_path.open('r', encoding='utf-8') as handle:
            return yaml.safe_load(handle) or {}

    def _env_overrides(self) -> Dict[str, Any]:
        overrides: Dict[str, Any] = {}
        if symbols := os.getenv('MDP_SYMBOLS'):
            parsed = [sym.strip() for sym in symbols.split(',') if sym.strip()]
            if parsed:
                overrides['symbols'] = parsed
        if start := os.getenv('MDP_START_DATE'):
            overrides['start_date'] = start
        if end := os.getenv('MDP_END_DATE'):
            overrides['end_date'] = end
        if retry := os.getenv('MDP_RETRY'):
            try:
                overrides['retry'] = int(retry)
            except ValueError:
                raise ConfigValidationError('MDP_RETRY phải là số nguyên.')

        data_paths: Dict[str, str] = {}
        if raw_path := os.getenv('MDP_DATA_PATHS_RAW'):
            data_paths['raw'] = raw_path
        if processed_path := os.getenv('MDP_DATA_PATHS_PROCESSED'):
            data_paths['processed'] = processed_path
        if data_paths:
            overrides.setdefault('data_paths', {}).update(data_paths)

        logging_config: Dict[str, Any] = {}
        if log_level := os.getenv('MDP_LOGGING_LEVEL'):
            logging_config['level'] = log_level
        if log_dir := os.getenv('MDP_LOGGING_DIR'):
            logging_config['dir'] = log_dir
        if logging_config:
            overrides.setdefault('logging', {}).update(logging_config)

        return overrides

    def _merge_dicts(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        for key, value in base.items():
            if isinstance(value, dict):
                result[key] = value.copy()
            else:
                result[key] = value

        for key, value in override.items():
            if isinstance(value, dict) and isinstance(result.get(key), dict):
                result[key].update(value)
            else:
                result[key] = value

        return result

    def _validate(self, config: Dict[str, Any]) -> None:
        required_fields = ['symbols', 'start_date', 'end_date', 'data_paths', 'logging', 'retry']
        for field in required_fields:
            if field not in config or config[field] in (None, '', []):
                raise ConfigValidationError(f'Trường cấu hình "{field}" là bắt buộc.')
        if not isinstance(config['symbols'], list):
            raise ConfigValidationError('symbols phải là danh sách chuỗi.')
        if not isinstance(config['data_paths'], dict):
            raise ConfigValidationError('data_paths phải là từ điển chứa raw và processed.')
        if 'raw' not in config['data_paths'] or 'processed' not in config['data_paths']:
            raise ConfigValidationError('data_paths phải chứa keys "raw" và "processed".')
        if 'level' not in config['logging']:
            raise ConfigValidationError('logging.level là bắt buộc.')
        if not isinstance(config['retry'], int):
            try:
                config['retry'] = int(config['retry'])
            except (TypeError, ValueError):
                raise ConfigValidationError('retry phải là số nguyên hợp lệ.')

    def _resolve_config_path(self, path: Path) -> Path:
        """
        Resolve đường dẫn config theo Project Root thay vì phụ thuộc vào CWD.

        Logic:
        - Nếu `path` là absolute -> dùng luôn.
        - Nếu `path` tồn tại theo CWD -> dùng luôn.
        - Nếu không, sẽ resolve theo project root (dò marker `pyproject.toml` hoặc thư mục `src`).
        """

        if path.is_absolute():
            return path
        if path.exists():
            return path
        project_root = self._find_project_root(Path(__file__).resolve())
        return (project_root / path).resolve()

    def _find_project_root(self, start: Path) -> Path:
        """
        Tìm project root bằng cách đi ngược lên từ `start` và tìm marker.

        Marker ưu tiên:
        - `pyproject.toml` + thư mục `src`
        - hoặc thư mục `config` + `src`
        """

        for parent in [start] + list(start.parents):
            if (parent / 'src').is_dir() and (parent / 'pyproject.toml').exists():
                return parent
            if (parent / 'src').is_dir() and (parent / 'config').is_dir():
                return parent
        return start.parent
