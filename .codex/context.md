Market Data Pipeline — Project Context
Tổng Quan Dự Án
Dự án Market Data Pipeline là hệ thống thu thập và xử lý dữ liệu thị trường chứng khoán Việt Nam được viết bằng Python. Hệ thống sử dụng thư viện vnstock làm nguồn dữ liệu chính và áp dụng kiến trúc mô-đun với các thành phần riêng biệt: Extractors đảm nhiệm việc trích xuất dữ liệu, Transformers xử lý và tính toán, Analyzers thực hiện phân tích chuyên sâu, và Utilities cung cấp các hàm tiện ích chung. Dự án hỗ trợ hai chế độ chạy bao gồm Manual (danhh sách cố định) và Dynamic (quét toàn bộ thị trường tự động).

Dự án được thiết kế để xử lý quy mô lớn với hơn 1.500 mã chứng khoán từ 3 sàn giao dịch chính là HOSE, HNX và UPCAM. Hệ thống có khả năng mở rộng cao, dễ dàng bảo trì và nâng cấp thông qua kiến trúc module hóa rõ ràng. Mục tiêu cuối cùng là tạo ra một pipeline tự động hóa hoàn chỉnh từ thu thập dữ liệu, xử lý, phân tích đến tạo báo cáo cho người dùng.

Cấu Trúc Thư Mục Dự Án
market_data_pipeline/
├── config/
│   └── pipeline_config.yaml     # File cấu hình chính (YAML format)
├── data/
│   ├── raw/                     # Dữ liệu thô từ API (OHLCV, Fundamentals)
│   ├── processed/               # Dữ liệu đã xử lý và tính indicators
│   └── cache/                   # Cache files (JSON) cho MarketScanner
├── docs/                        # Tài liệu kỹ thuật và API documentation
├── notebooks/                   # Jupyter notebooks cho exploration
├── reports/                     # Báo cáo đầu ra (Markdown, HTML)
├── scripts/                     # Scripts CLI để chạy pipeline
│   ├── run_pipeline.py          # Script chạy chính
│   └── daily_update.py          # Script cập nhật hàng ngày
├── src/
│   ├── extractors/              # Module trích xuất dữ liệu
│   │   ├── base_extractor.py    # Abstract base class cho tất cả extractors
│   │   ├── price_extractor.py   # Trích xuất dữ liệu giá OHLCV
│   │   ├── fundamental_extractor.py # Trích xuất dữ liệu tài chính
│   │   └── breadth_extractor.py # Trích xuất market breadth
│   ├── transformers/            # Module xử lý và biến đổi dữ liệu
│   │   ├── data_cleaner.py      # Làm sạch và chuẩn hóa dữ liệu
│   │   └── technical_indicators.py # Tính toán chỉ báo kỹ thuật
│   ├── analyzers/               # Module phân tích
│   │   ├── technical_screener.py # Phân tích kỹ thuật và screening
│   │   ├── breadth_analyzer.py  # Phân tích market breadth
│   │   └── fundamental_analyzer.py # Phân tích cơ bản
│   ├── utils/                   # Tiện ích chung
│   │   ├── file_utils.py        # Hàm: ensure_dir(), atomic_write_csv()
│   │   ├── logger.py            # Hàm: setup_logger(), get_logger()
│   │   ├── config_loader.py     # Class: ConfigLoader
│   │   ├── market_scanner.py    # Class: MarketScanner (MỚI)
│   │   └── report_generator.py  # Class: ReportGenerator
│   └── pipeline.py              # Pipeline orchestrator chính
├── logs/                        # Log files của ứng dụng
├── requirements.txt             # Danh sách dependencies
├── README.md                    # Tài liệu hướng dẫn chính
└── pyproject.toml               # Cấu hình project Python
Tiêu Chuẩn Lập Trình (Coding Standards)
Dự án tuân thủ nghiêm ngặt các tiêu chuẩn lập trình sau để đảm bảo tính nhất quán và dễ bảo trì của codebase. Tất cả code mới được tạo ra phải tuân theo các tiêu chuẩn này để đảm bảo tính thống nhất trong toàn bộ dự án.

Ngôn Ngữ Và Comment
Tất cả tên biến, tên hàm, tên class phải sử dụng tiếng Anh và tuân thủ PEP 8. Tuy nhiên, tất cả docstrings và inline comments phải viết bằng tiếng Việt để phù hợp với người dùng chính của dự án là các nhà đầu tư và lập trình viên Việt Nam. Điều này giúp code vừa dễ đọc về mặt kỹ thuật, vừa dễ hiểu về mặt nghiệp vụ.
def calculate_moving_average(self, df: pd.DataFrame, period: int = 20) -> pd.DataFrame:
    """
    Tính Simple Moving Average cho DataFrame.
    
    Args:
        df: DataFrame chứa dữ liệu OHLCV với cột 'close'
        period: Số ngày tính MA (mặc định: 20)
    
    Returns:
        DataFrame với cột 'ma_{period}' đã được thêm vào
    """
    # Sử dụng pandasrolling để tính toán hiệu quả
    df[f"ma_{period}"] = df["close"].rolling(window=period).mean()
    return df
    Type Hints
Tất cả các hàm và phương thức phải sử dụng Type Hints đầy đủ cho parameters và return values. Việc này giúp IDE hỗ trợ autocomplete và kiểm tra lỗi type, đồng thời làm tài liệu hóa code một cách tự động. Các type phổ biến bao gồm List, Dict, Optional, Union từ module typing, và các type của pandas như pd.DataFrame, pd.Series.
from typing import List, Dict, Optional, Union
import pandas as pd

def get_history(
    self, 
    symbol: str, 
    start_date: str, 
    end_date: str, 
    resolution: str = "1D"
) -> Optional[pd.DataFrame]:
    """
    Lấy dữ liệu OHLCV cho một mã chứng khoán.
    
    Returns:
        DataFrame chứa dữ liệu OHLCV hoặc None nếu lỗi
    """
    Logging Thay Vì Print
Tuyệt đối không sử dụng print() trong code production. Thay vào đó, hãy sử dụng module logging chuẩn của Python. Điều này cho phép kiểm soát mức log (DEBUG, INFO, WARNING, ERROR, CRITICAL), định dạng log统一, và ghi log ra file. Logger được thiết lập sẵn trong src/utils/logger.py và có thể import để sử dụng.
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Các mức log sử dụng
logger.debug("Thông tin chi tiết cho debugging")
logger.info("Thông tin tổng quan về tiến trình")
logger.warning("Cảnh báo về điều kiện bất thường nhưng không nghiêm trọng")
logger.error("Lỗi xảy ra trong một phần của hệ thống")
logger.critical("Lỗi nghiêm trọng, hệ thống có thể dừng")
Error Handling
Tất cả các lệnh gọi API và thao tác với file phải được bọc trong khối try-except. Khi xảy ra lỗi, hãy log chi tiết exception và có cơ chế fallback graceful nếu có thể. Đặc biệt với MarketScanner, nếu API thất bại nhưng cache cũ tồn tại, hãy log warning và sử dụng cache thay vì crash hệ thống.
try:
    result = self.extractors["price"].get_history(symbol=symbol)
    return result
except Exception as e:
    logger.error(f"Lỗi khi trích xuất dữ liệu {symbol}: {str(e)}")
    # Fallback: trả về None để pipeline tiếp tục
    return None
    Cấu Trúc Imports
Imports phải được sắp xếp theo thứ tự sau và mỗi nhóm cách nhau một dòng trống. Điều này giúp đọc và kiểm soát imports dễ dàng hơn. Các imports từ cùng một package nên được nhóm lại với nhau.
# Standard library imports (alphabetically)
import json
import logging
import os
from datetime import datetime, date
from typing import Dict, List, Optional

# Third-party imports (alphabetically)
import pandas as pd
import yaml

# Local application imports (sử dụng absolute imports)
from src.utils.file_utils import ensure_dir
from src.utils.logger import get_logger
from src.utils.config_loader import ConfigLoader
Modules Quan Trọng Cần Tham Khảo
src/utils/file_utils.py
Module này cung cấp các hàm tiện ích cho thao tác với file và thư mục. Function quan trọng nhất là ensure_dir() được sử dụng rộng rãi trong dự án để đảm bảo thư mục tồn tại trước khi ghi file.
def ensure_dir(path: str) -> bool:
    """
    Tạo thư mục và tất cả parent directories nếu chưa tồn tại.
    
    Args:
        path: Đường dẫn thư mục cần tạo
    
    Returns:
        True nếu thành công hoặc thư mục đã tồn tại
    """
    os.makedirs(path, exist_ok=True)
    return True
    src/utils/logger.py
Module thiết lập và cấu hình hệ thống logging cho toàn bộ ứng dụng. Sử dụng setup_logger() để khởi tạo logger chính và get_logger() để lấy logger cho từng module cụ thể.
def setup_logger(
    name: str, 
    log_dir: str = "logs", 
    level: str = "INFO",
    use_json: bool = False
) -> logging.Logger:
    """
    Thiết lập logger với console và file handlers.
    
    Args:
        name: Tên logger (thường là __name__)
        log_dir: Thư mục lưu log files
        level: Mức log (DEBUG, INFO, WARNING, ERROR, CRITICAL)
        use_json: Sử dụng JSON format cho log
    
    Returns:
        Logger instance đã được cấu hình
    """
    src/utils/config_loader.py
Module đọc và quản lý cấu hình từ file YAML. Hỗ trợ nested configurations và override từ environment variables.
class ConfigLoader:
    """Class đọc và quản lý cấu hình từ file YAML."""
    
    def __init__(self, config_path: str):
        """Khởi tạo với đường dẫn file cấu hình."""
    
    def load(self) -> Dict:
        """Đọc và trả về cấu hình dưới dạng Dictionary."""
src/utils/market_scanner.py (MỚI)
Module mới được tạo để quét và lấy danh sách mã chứng khoán từ thị trường Việt Nam. Đây là module quan trọng cho chế độ Dynamic Market Scope.
class MarketScanner:
    """
    Lớp quét và lấy danh sách mã chứng khoán từ thị trường Việt Nam.
    
    Attributes:
        cache_dir: Thư mục chứa cache
        cache_file: Đường dẫn file cache JSON
    """
    
    def __init__(self, cache_dir: str = "data/cache"):
        """Khởi tạo MarketScanner."""
    
    def get_all_tickers(self, force_refresh: bool = False) -> List[str]:
        """
        Lấy danh sách tất cả mã chứng khoán đang hoạt động.
        
        Returns:
            List[str]: Danh sách các mã chứng khoán đã được lọc
        """

Dependencies Chính
Dự án sử dụng các thư viện sau làm dependencies chính. Khi tạo code mới, cần import đúng các thư viện này theo cấu trúc đã định.

Thư viện	Phiên bản	Mục đích
vnstock	>= 0.2.0	Lấy dữ liệu chứng khoán Việt Nam
pandas	>= 1.5.0	Xử lý và phân tích dữ liệu dạng bảng
pyyaml	>= 6.0	Đọc file cấu hình YAML
python-dateutil	>= 2.8.0	Xử lý ngày tháng
Yêu Cầu Cho Code Mới
Khi Codex hoặc lập trình viên tạo code mới cho dự án, cần tuân thủ các yêu cầu sau để đảm bảo tính nhất quán. Code mới phải tuân thủ đầy đủ các coding standards đã nêu ở trên bao gồm Vietnamese comments, Type Hints, và logging. Code phải tương thích với kiến trúc module hiện tại và không được phá vỡ các interfaces đã có. Tất cả API calls phải có error handling và graceful fallback khi có thể. Code phải có thể chạy trong cả hai chế độ Manual và Dynamic mà không cần sửa đổi.

Nếu có bất kỳ thắc mắc nào về coding standards hoặc cần hỗ trợ, hãy tham khảo các files mẫu trong src/utils/ trước khi viết code mới.