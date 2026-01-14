from abc import ABC, abstractmethod
from concurrent.futures import ThreadPoolExecutor, as_completed
import time
import uuid
from typing import Any, Dict, Iterable, List, Optional

from src.utils.logger import get_logger

from .models import ExtractionTask, ExtractorResult, TaskResult


class BaseExtractor(ABC):
    """Lớp trừu tượng cơ sở cho mọi extractor trong pipeline."""

    name: str = 'base-extractor'
    supported_data_types: List[str] = []
    default_config: Dict[str, Any] = {}

    def __init__(self) -> None:
        self.logger = get_logger(self.__class__.__name__)

    @abstractmethod
    def extract(self, task: ExtractionTask) -> TaskResult:
        """Thực hiện công việc trích xuất cụ thể được định nghĩa bởi task."""

    def run(
        self,
        tasks: Iterable[ExtractionTask],
        parallel: bool = False,
        max_workers: Optional[int] = None,
    ) -> ExtractorResult:
        """Chạy danh sách task và trả về ExtractorResult tổng hợp."""

        tasks_list = list(tasks)
        total = len(tasks_list)
        start_time = time.monotonic()
        results: List[TaskResult] = []

        if parallel:
            with ThreadPoolExecutor(max_workers=max_workers) as executor:
                future_to_task = {
                    executor.submit(self._safe_extract, task): task
                    for task in tasks_list
                }
                for completed in as_completed(future_to_task):
                    task = future_to_task[completed]
                    result = completed.result()
                    results.append(result)
                    self._log_progress(len(results), total, task.symbol)
        else:
            for task in tasks_list:
                result = self._safe_extract(task)
                results.append(result)
                self._log_progress(len(results), total, task.symbol)

        execution_time = time.monotonic() - start_time
        successful = sum(1 for result in results if result.success)
        failed = total - successful
        errors_summary: Dict[str, int] = {}
        for result in results:
            if result.error:
                errors_summary[result.error] = errors_summary.get(result.error, 0) + 1

        return ExtractorResult(
            extractor_name=self.name,
            total_tasks=total,
            successful_tasks=successful,
            failed_tasks=failed,
            results=results,
            execution_time=execution_time,
            errors_summary=errors_summary,
        )

    def _safe_extract(self, task: ExtractionTask) -> TaskResult:
        """Bao wrapper để đảm bảo TaskResult luôn được trả về."""

        try:
            return self.extract(task)
        except Exception as exc:
            self.logger.exception('Task %s thất bại: %s', task.task_id, exc)
            return TaskResult(
                task_id=task.task_id,
                symbol=task.symbol,
                success=False,
                error=str(exc),
                row_count=0,
                execution_time=0.0,
            )

    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Kiểm tra tính hợp lệ của config trước khi tạo task."""

        if not isinstance(config, dict):
            self.logger.error('Config extractor phải là dict')
            return False
        symbols = config.get('symbols')
        if not symbols or not isinstance(symbols, list):
            self.logger.error('Config thiếu symbols dạng list')
            return False
        return True

    def build_tasks(
        self,
        symbols: List[str],
        start_date: Optional[str] = None,
        end_date: Optional[str] = None,
        resolution: str = '1D',
        data_type: Optional[str] = None,
        config: Optional[Dict[str, Any]] = None,
    ) -> List[ExtractionTask]:
        """Tạo task từ danh sách symbol và các tham số chung."""

        tasks: List[ExtractionTask] = []
        common_config = {**self.default_config.get('config', {}), **(config or {})}
        current_data_type = data_type or self.default_config.get('data_type', 'ohlcv')

        for symbol in symbols:
            task_id = f'{self.name}-{symbol}-{uuid.uuid4().hex[:8]}'
            task = ExtractionTask(
                task_id=task_id,
                symbol=symbol,
                data_type=current_data_type,
                start_date=start_date,
                end_date=end_date,
                resolution=resolution,
                config={**common_config},
            )
            tasks.append(task)
        return tasks

    def _log_progress(self, current: int, total: int, symbol: str) -> None:
        """Ghi log tiến trình xử lý task."""

        self.logger.info('Progress: %s/%s - %s', current, total, symbol)
