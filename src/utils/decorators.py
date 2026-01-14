import functools
import time
from typing import Any, Callable, Optional, TypeVar

from src.utils.logger import get_logger

LoggerCallable = Callable[..., Any]
F = TypeVar('F', bound=LoggerCallable)


def _logger():
    return get_logger(__name__)


def retry(max_attempts: int = 3, delay: float = 1.0, backoff: float = 2.0) -> Callable[[F], F]:
    """Decorator retry để thử lại hàm khi xảy ra ngoại lệ."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            current_delay = delay
            attempt = 0
            while attempt < max_attempts:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    attempt += 1
                    _logger().warning(
                        'Lỗi tại %s (attempt %s/%s): %s',
                        func.__name__,
                        attempt,
                        max_attempts,
                        exc,
                    )
                    if attempt >= max_attempts:
                        raise
                    time.sleep(current_delay)
                    current_delay *= backoff
            # Should never reach here
            return func(*args, **kwargs)

        return wrapper  # type: ignore[return-value]

    return decorator


def timer(func: F) -> F:
    """Decorator đo thời gian thực thi hàm và lưu vào thuộc tính last_duration."""

    @functools.wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> Any:
        start = time.monotonic()
        result = func(*args, **kwargs)
        duration = time.monotonic() - start
        setattr(wrapper, 'last_duration', duration)
        _logger().debug('Thời gian %s: %.3fs', func.__name__, duration)
        return result

    return wrapper  # type: ignore[return-value]


def safe_execute(default: Optional[Any] = None) -> Callable[[F], F]:
    """Decorator bảo vệ hàm khỏi ngoại lệ và trả về giá trị mặc định."""

    def decorator(func: F) -> F:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> Any:
            try:
                return func(*args, **kwargs)
            except Exception as exc:
                _logger().exception('safe_execute bắt lỗi tại %s: %s', func.__name__, exc)
                return default

        return wrapper  # type: ignore[return-value]

    return decorator
