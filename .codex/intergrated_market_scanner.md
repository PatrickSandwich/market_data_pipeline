System Role (Vai trò hệ thống):
Bạn là một Software Architect với chuyên môn cao về Python Design Patterns và Orchestrator Design. Bạn có khả năng đọc hiểu kiến trúc hệ thống hiện có và tích hợp các module mới một cách liền mạch mà không phá vỡ cấu trúc cũ.

Context (Bối cảnh):
Tôi đang phát triển dự án "Market Data Pipeline". Tại thời điểm này:

Module src/utils/market_scanner.py đã được hoàn thành và có sẵn method get_all_tickers() -> List[str].
File điều khiển chính là src/pipeline.py (Pipeline Orchestrator), hiện tại đang chạy dựa trên danh sách symbols cố định từ file cấu hình.
File cấu hình là config/pipeline_config.yaml.
Task (Nhiệm vụ):
Hãy viết code để tích hợp MarketScanner vào Pipeline, cho phép hệ thống chuyển đổi linh hoạt giữa chế độ chạy thủ công (Manual) và chế độ quét toàn thị trường (Dynamic).

Các bước thực hiện chi tiết:

Bước 1: Cập nhật cấu hình YAML
Đề xuất code để cập nhật config/pipeline_config.yaml (hoặc tạo section mới trong đó) với cấu trúc sau:

yaml

# Cấu hình phạm vi thị trường
market_scope:
  mode: "dynamic"  # "dynamic" = quét tự động, "manual" = dùng list symbols
  # Dùng trong chế độ manual
  symbols: 
    - VNM
    - MWG
Bước 2: Tích hợp Logic vào src/pipeline.py
Tôi cần bạn viết code cập nhật cho class Pipeline trong src/pipeline.py (tập trung vào method run() hoặc _prepare_extraction_tasks()). Logic cần thực hiện như sau:

Import: Import MarketScanner từ src.utils.market_scanner.
Đọc Config: Đọc section market_scope.mode.
Xác định nguồn Symbols:
Nếu mode == "dynamic": Khởi tạo MarketScanner, gọi get_all_tickers() và gán vào biến danh sách.
Nếu mode == "manual": Lấy danh sách từ config['market_scope']['symbols'].
Logging (Tiếng Việt):
Nếu Dynamic: Ghi log INFO là: "Khởi chạy chế độ DYNAMIC - Quét toàn thị trường. Tổng số mã phát hiện: {len(tickers)}".
Nếu Manual: Ghi log INFO là: "Khởi chạy chế độ MANUAL - Danh sách tùy chỉnh. Số lượng mã: {len(tickers)}".
Xử lý lỗi: Nếu ở chế độ Dynamic mà MarketScanner gặp lỗi (ví dụ mất mạng), hãy bắt exception, log lỗi ERROR, và dừng pipeline hoặc thực hiện cơ chế fallback (nếu có thể) thay vì crash thô bạo.
Yêu cầu Code:

Sử dụng Type Hints (List[str], Optional, v.v.).
Viết Docstrings bằng Tiếng Việt cho các method được chỉnh sửa.
Code phải clean, tuân thủ PEP 8 và phù hợp với kiến trúc module hiện có.
Đừng viết lại toàn bộ file pipeline nếu không cần thiết, chỉ cung cấp đoạn code thay đổi và hướng dẫn vị trí chèn.