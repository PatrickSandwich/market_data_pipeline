from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional

import pandas as pd


@dataclass
class ExtractionTask:
    """Định nghĩa công việc trích xuất một mã chứng khoán cụ thể."""

    task_id: str
    symbol: str
    data_type: str
    start_date: Optional[str]
    end_date: Optional[str]
    resolution: str
    config: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Chuyển đối tượng Task sang dict để dễ serialize."""

        return asdict(self)

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExtractionTask':
        """Xây dựng ExtractionTask từ dict đã deserialize."""

        return cls(**data)


@dataclass
class TaskResult:
    """Đăng ký kết quả sau khi chạy một ExtractionTask."""

    task_id: str
    symbol: str
    success: bool
    data: Optional[pd.DataFrame] = None
    error: Optional[str] = None
    row_count: int = 0
    execution_time: float = 0.0
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Chuyển dữ liệu TaskResult sang dict, convert DataFrame về records."""

        payload = asdict(self)
        if self.data is not None:
            payload['data'] = self.data.to_dict(orient='records')
        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TaskResult':
        """Xây dựng TaskResult từ dict và khôi phục DataFrame nếu cần."""

        payload = data.copy()
        if payload.get('data') is not None and isinstance(payload['data'], list):
            payload['data'] = pd.DataFrame(payload['data'])
        return cls(**payload)


@dataclass
class ExtractorResult:
    """Tổng hợp kết quả từ nhiều ExtractionTask."""

    extractor_name: str
    total_tasks: int
    successful_tasks: int
    failed_tasks: int
    results: List[TaskResult] = field(default_factory=list)
    execution_time: float = 0.0
    errors_summary: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        """Serialize ExtractorResult và các TaskResult con."""

        payload = asdict(self)
        payload['results'] = [result.to_dict() for result in self.results]
        return payload

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'ExtractorResult':
        """Tạo ExtractorResult từ dict, phục hồi TaskResult list."""

        payload = data.copy()
        if payload.get('results'):
            payload['results'] = [
                TaskResult.from_dict(res) for res in payload['results']
            ]
        return cls(**payload)
