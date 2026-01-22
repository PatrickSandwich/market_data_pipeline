# Market Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](#) [![License: MIT](https://img.shields.io/badge/License-MIT-green)](#)

## Tổng quan (Overview)

**Market Data Pipeline** là dự án Python dùng thư viện `vnstock` để **thu thập – làm sạch – tính toán chỉ báo – phân tích tự động** dữ liệu thị trường chứng khoán Việt Nam (VN-Index và các mã cổ phiếu).

Điểm nhấn:
- **Tự động hoá** chạy hằng ngày (scheduler/cron) và hỗ trợ chạy qua CLI.
- **Thiết kế mô-đun** theo kiến trúc: Extractors → Transformers → Analyzers.
- **Market Breadth Analysis** để đánh giá sức khỏe thị trường và regime.

## Tính năng nổi bật (Key Features)

| Tính năng | Mô tả |
|---|---|
| **Market Scanner** | Tự động quét danh sách mã chứng khoán từ HOSE, HNX, UPCOM (qua `MarketScanner`) |
| **Dynamic/Manual Mode** | Hỗ trợ chế độ quét tự động toàn thị trường (Dynamic) hoặc danh sách cố định (Manual) |
| **Market Scope Filter** | Giảm số lượng mã cần xử lý bằng cách lọc theo phạm vi thị trường: `hsx_only`, `hsx_hnx`, `core` (lọc UPCOM top N), `all` |
| **Intelligent Caching** | Cache danh sách mã theo ngày để giảm số lượng gọi API (`data/cache/all_tickers_cache.json`) |
| **Financial Statements** | Lấy Income Statement, Balance Sheet, Cash Flow (có fallback tương thích nhiều phiên bản `vnstock`) |
| **Technical Indicators** | MA, RSI, MACD, Bollinger Bands, ATR và các metrics liên quan |
| **Market Breadth Analysis** | Phân tích sức khỏe thị trường, market regime, leading sectors |
| **Error Resilience** | Pipeline không crash khi 1 mã lỗi; tự skip, log rõ ràng và tiếp tục xử lý |
| **Performance Controls** | Tối ưu hiệu năng với `performance.max_concurrent_requests` và `performance.max_retries` (có thể override bằng `--parallel`) |

## Cấu trúc thư mục (Project Structure)

```text
market_data_pipeline/
├─ config/
│  ├─ pipeline_config.yaml          # Cấu hình chính (khuyến nghị cho CLI)
│  └─ settings.yaml                 # Cấu hình mặc định (tương thích ngược)
├─ data/
│  ├─ raw/                          # Data thô (tuỳ theo cấu hình/collector)
│  └─ processed/                    # Data đã xử lý + indicators (parquet/csv)
│  └─ cache/                        # Cache danh sách mã (MarketScanner)
├─ logs/                            # Log theo từng lần chạy
├─ reports/
│  ├─ daily/                        # Báo cáo daily (Markdown)
│  ├─ weekly/                       # Báo cáo weekly (Markdown)
│  └─ templates/
│     └─ report_template.html       # Template HTML tham khảo
├─ scripts/
│  ├─ run_pipeline.py               # CLI entry point (daily/full/analysis/validate)
│  ├─ daily_update.py               # Scheduler chạy 18:00 mỗi ngày
│  └─ export_report.py              # Export báo cáo (Markdown/HTML/PDF nếu có)
└─ src/
   ├─ extractors/                   # Module trích xuất dữ liệu (vnstock)
   ├─ scanners/                     # Lọc/chuẩn hoá danh sách symbols (market scope)
   ├─ transformers/                 # Làm sạch + chỉ báo kỹ thuật
   ├─ analyzers/                    # Phân tích + screening
   ├─ utils/                        # Logging, config, decorators, export, report generator...
   └─ pipeline.py                   # Orchestrator kết hợp toàn bộ modules
```

## Cài đặt (Installation)

**Yêu cầu:** Python **3.9+**

```bash
# 1) Clone repo
git clone <your-repo-url>
cd market_data_pipeline

# 2) Cài dependencies
pip install -r requirements.txt

# 3) Cài vnstock (bắt buộc)
pip install vnstock
```

Ghi chú:
- Để ghi Parquet, môi trường thường cần thêm `pyarrow` hoặc `fastparquet`:
  ```bash
  pip install pyarrow
  ```

## Cấu hình (Configuration)

Pipeline đọc cấu hình từ file YAML (mặc định CLI dùng `config/pipeline_config.yaml`, có thể override bằng `--config`).
File cấu hình **bắt buộc** có các trường: `start_date`, `end_date`, `data_paths`, `logging`, `retry`, và danh sách `symbols` (hoặc `market_scope.symbols` khi chạy Manual).

Ví dụ `config/pipeline_config.yaml`:

```yaml
market_scope:
  mode: "dynamic"  # "dynamic" = quét tự động, "manual" = dùng list symbols
  scope: "core"    # "all" | "core" | "hsx_only" | "hsx_hnx"
  # (Optional) Các sàn cần quét (khi mode = "dynamic")
  exchanges: ["HOSE", "HNX", "UPCOM"]
  # (Optional) Bộ lọc tại MarketScanner
  filters:
    exclude_etf: true
    exclude_suspended: true
  symbols:         # (chỉ dùng khi mode = "manual") hoặc fallback khi dynamic lỗi
    - VNM
    - MWG

market_scope_settings:
  # Chỉ áp dụng khi scope = "core"
  upcom_max_symbols: 50
  # Nếu vnstock Listing trả về các cột thanh khoản (avg_value/avg_volume/market_cap) thì sẽ sort theo cột này.
  upcom_sort_by: "avg_value"
  include_exchanges:
    all: ["HSX", "HNX", "UPCOM"]
    core: ["HSX", "HNX", "UPCOM"]
    hsx_only: ["HSX"]
    hsx_hnx: ["HSX", "HNX"]

symbols: ["VNM", "HPG", "FPT"]
start_date: "2024-01-01"
end_date: "2026-01-15"  # hoặc để ngày hiện tại trong workflow của bạn
resolution: "1D"

data_paths:
  raw: "data/raw"
  processed: "data/processed"

logging:
  level: "INFO"
  dir: "logs"

retry: 3

performance:
  # Nếu bạn không truyền --parallel, pipeline sẽ dùng giá trị này làm default
  max_concurrent_requests: 10
  api_timeout: 30
  max_retries: 3
```

### Cấu hình phạm vi thị trường (`market_scope`)

| Trường | Ý nghĩa |
|---|---|
| `market_scope.mode` | `dynamic` = tự quét toàn thị trường, `manual` = dùng danh sách cố định |
| `market_scope.symbols` | Danh sách mã dùng cho Manual hoặc fallback khi Dynamic lỗi |
| `market_scope.scope` | Phạm vi lọc thị trường (tối ưu hiệu năng): `hsx_only`, `hsx_hnx`, `core`, `all` |

### Market Scope Filter (tối ưu hiệu năng)

Pipeline hỗ trợ lọc phạm vi thị trường để giảm số lượng symbols cần xử lý (đặc biệt UPCOM), giúp giảm thời gian chạy đáng kể.

| Scope | Sàn bao gồm | Ước tính số lượng | Use case |
|---|---|---:|---|
| `all` | HSX + HNX + UPCOM | ~1730 | Research/backtest toàn diện |
| `core` | HSX + HNX + top UPCOM | ~700–800 | Default cân bằng coverage/performance |
| `hsx_hnx` | HSX + HNX | ~700 | Tập trung niêm yết chính thức |
| `hsx_only` | HSX | ~400 | Speed-optimized |

Ghi chú:
- `core` sẽ giữ tối đa `market_scope_settings.upcom_max_symbols` mã UPCOM. Nếu không có cột thanh khoản để sort, hệ thống sẽ fallback lấy top N mã UPCOM theo thứ tự hiện có.
| `market_scope.exchanges` | Danh sách sàn cần ưu tiên (nếu dữ liệu API trả về có trường sàn, scanner sẽ lọc theo sàn) |
| `market_scope.filters.exclude_etf` | Loại ETF khỏi danh sách quét (mặc định bật) |
| `market_scope.filters.exclude_suspended` | Loại mã bị treo/dừng/không hoạt động (mặc định bật khi có `status`) |
| `market_scope.force_refresh` | Bỏ qua cache tickers và gọi API mới (có thể bật qua CLI `--force-refresh`) |

**Override bằng biến môi trường (tuỳ chọn)** (đã hỗ trợ trong `ConfigLoader`):

| Biến | Ý nghĩa |
|---|---|
| `MDP_SYMBOLS` | Danh sách mã, dạng `VNM,HPG,FPT` |
| `MDP_START_DATE` / `MDP_END_DATE` | Override ngày chạy |
| `MDP_DATA_PATHS_RAW` / `MDP_DATA_PATHS_PROCESSED` | Override đường dẫn dữ liệu |
| `MDP_LOGGING_LEVEL` / `MDP_LOGGING_DIR` | Override cấu hình logging |
| `MDP_RETRY` | Số lần retry khi lỗi |
| `MDP_FORCE_REFRESH` | Bỏ qua cache tickers (set `1/true/yes`) |
| `LOG_JSON_OUTPUT` | Bật JSON logs (`true/false`) |

Lưu ý về `vnstock`:
- API/field có thể thay đổi theo phiên bản; dự án đã có **fallback handling** ở một số extractor.
- Khi gọi dữ liệu nhiều mã, nên giữ `sleep(1)` để giảm nguy cơ bị rate-limit.

## Hướng dẫn sử dụng (Usage)

### Cách 1: Chạy qua CLI (Scripts)

#### Chế độ Dynamic (quét tự động)

```yaml
# config/pipeline_config.yaml
market_scope:
  mode: "dynamic"
```

Chạy pipeline sẽ tự động quét toàn thị trường và cache danh sách mã theo ngày:
```bash
python scripts/run_pipeline.py --mode daily
```

Bỏ qua cache tickers và gọi API mới:
```bash
python scripts/run_pipeline.py --mode daily --force-refresh
```

#### Chế độ Manual (danh sách cố định)

```yaml
# config/pipeline_config.yaml
market_scope:
  mode: "manual"
  symbols:
    - VNM
    - MWG
    - FPT
```

Chạy cập nhật hằng ngày:
```bash
python scripts/run_pipeline.py --mode daily
```

Chạy full pipeline:
```bash
python scripts/run_pipeline.py --mode full
```

Phân tích cho danh sách mã:
```bash
python scripts/run_pipeline.py --mode analysis --symbols VNM,HPG
```

Validate chất lượng dữ liệu:
```bash
python scripts/run_pipeline.py --mode validate --symbols all
```

### Tối ưu hiệu năng khi chạy toàn thị trường

- Giảm universe bằng `market_scope.scope`:
  - `hsx_only` (nhanh nhất), `hsx_hnx`, hoặc `core` (lọc UPCOM top N).
- Điều chỉnh concurrency qua CLI:
  ```bash
  python scripts/run_pipeline.py --mode daily --symbols all --parallel 10
  ```
- Nếu không truyền `--parallel`, pipeline dùng `performance.max_concurrent_requests` trong YAML.

Chạy job tự động lúc 18:00 mỗi ngày (test nhanh với `--once`):
```bash
python scripts/daily_update.py --once
```

Export report (Markdown/HTML):
```bash
python scripts/export_report.py --format markdown --symbols all
python scripts/export_report.py --format html --symbols all
```

### Cách 2: Sử dụng Python API

```python
from src.pipeline import Pipeline

pipeline = Pipeline(config_path="config/pipeline_config.yaml")
result = pipeline.run_daily_update(parallel_workers=4)
print(result)
```

## Quy trình dữ liệu (Data Flow)

```text
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ Market Scanner  │────▶│    Extractors   │────▶│  Transformers   │
│ (HOSE/HNX/UPCOM)│     │ Price/Fundamental/Breadth │ Clean/Indicators│
└─────────────────┘     └─────────────────┘     └─────────────────┘
                                                       │
                                                       ▼
                        ┌─────────────────┐     ┌─────────────────┐
                        │     Reports     │◀────│    Analyzers    │
                        │ Markdown/HTML   │     │ Tech/Breadth    │
                        └─────────────────┘     └─────────────────┘
```

## Thông báo lỗi (Notifications)

Pipeline có sẵn hook gửi Telegram (tuỳ chọn) khi thất bại/cảnh báo:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## Requirements

| Package | Phiên bản | Mục đích |
|---|---:|---|
| `vnstock` | `>= 3.0.0` | Lấy dữ liệu chứng khoán Việt Nam (giá, market breadth, financial statements) |
| `pandas` | `>= 1.5.0` | Xử lý dữ liệu dạng bảng |
| `pyyaml` | `>= 6.0` | Đọc cấu hình YAML |

## Troubleshooting

### Lỗi không lấy được dữ liệu Financial Statements
- Kiểm tra kết nối internet và thử chạy lại.
- Nếu đang chạy nhiều mã, ưu tiên chạy theo batch nhỏ hoặc tăng retry.
- Với chế độ Dynamic, thử chạy `python scripts/run_pipeline.py --mode daily --force-refresh` để bypass cache tickers.
- Kiểm tra mã chứng khoán có tồn tại không và có dữ liệu báo cáo theo kỳ mong muốn.

### Pipeline bị dừng ở một mã
- Pipeline được thiết kế để tự skip mã lỗi và tiếp tục chạy các mã còn lại.
- Xem log trong `logs/` để biết mã nào lỗi và lý do.
- Kiểm tra kết quả output trong `data/processed/` và `reports/`.

### Cache không được cập nhật
- Xóa file cache: `data/cache/all_tickers_cache.json`
- Chạy lại với `--force-refresh` để tạo cache mới.

## Version History

### v1.2.0 (2026-01-21) - Market Scope Filter + Performance Tuning
- Thêm `MarketScopeFilter` để giảm số lượng mã xử lý theo `market_scope.scope` (lọc UPCOM top N khi `core`).
- Thêm cấu hình `market_scope_settings` và `performance` (default concurrency + retries).
- Cải thiện độ bền: lọc/loại symbol lỗi, xử lý lỗi theo từng mã, và log `PIPELINE SUMMARY` cho mỗi lần chạy.

### v1.1.0 (2026-01-15) - Phase 3: Market Scanner Integration
- Thêm module `MarketScanner` quét tự động danh sách mã chứng khoán.
- Hỗ trợ chế độ Dynamic (quét toàn thị trường) và Manual (danh sách cố định).
- Thêm cơ chế cache thông minh giảm API calls.
- Tối ưu error handling, pipeline không crash khi 1 mã lỗi.
- Cập nhật cấu hình với section `market_scope`.

### v1.0.0 (2026-01-14) - Initial Release
- Phiên bản đầu tiên.
- Basic OHLCV extraction.
- Technical indicators calculation.
- Basic reporting.

## License

MIT (tham khảo badge). Nếu bạn muốn public repo, hãy thêm file `LICENSE` để đồng bộ thông tin license.
