# Market Data Pipeline

[![Python](https://img.shields.io/badge/Python-3.9%2B-blue)](#) [![License: MIT](https://img.shields.io/badge/License-MIT-green)](#)

## Tổng quan (Overview)

**Market Data Pipeline** là dự án Python dùng thư viện `vnstock` để **thu thập – làm sạch – tính toán chỉ báo – phân tích tự động** dữ liệu thị trường chứng khoán Việt Nam (VN-Index và các mã cổ phiếu).

Điểm nhấn:
- **Tự động hoá** chạy hằng ngày (scheduler/cron) và hỗ trợ chạy qua CLI.
- **Thiết kế mô-đun** theo kiến trúc: Extractors → Transformers → Analyzers.
- **Market Breadth Analysis** để đánh giá sức khỏe thị trường và regime.

## Tính năng nổi bật (Key Features)

| Nhóm | Mô tả |
|---|---|
| Data Extraction | Trích xuất OHLCV, realtime, intraday, fundamental, market breadth từ `vnstock` |
| Data Cleaning | Chuẩn hoá timezone, dedupe, forward-fill, validate dữ liệu OHLCV/financial/breadth |
| Technical Indicators | SMA/EMA/RSI/MACD/Bollinger/ATR/Volatility/Volume metrics/Returns (pandas + numpy thuần) |
| Technical Screening | Breakout, trend, divergence, signal scoring, screening nhiều mã |
| Market Breadth | Market health score, market regime, leading sectors, correlation, market summary |
| Reporting & Export | Export dữ liệu (Parquet/CSV), tạo báo cáo daily/weekly (Markdown), template HTML tuỳ biến |

## Cấu trúc thư mục (Project Structure)

```text
market_data_pipeline/
├─ config/
│  └─ settings.yaml                 # Cấu hình chạy pipeline (tự tạo)
├─ data/
│  ├─ raw/                          # Data thô (tuỳ theo cấu hình/collector)
│  └─ processed/                    # Data đã xử lý + indicators (parquet/csv)
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

Pipeline đọc cấu hình từ `config/settings.yaml`. File này **bắt buộc** có các trường: `symbols`, `start_date`, `end_date`, `data_paths`, `logging`, `retry`.

Ví dụ `config/pipeline_config.yaml`:

```yaml
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
```

**Override bằng biến môi trường (tuỳ chọn)** (đã hỗ trợ trong `ConfigLoader`):

| Biến | Ý nghĩa |
|---|---|
| `MDP_SYMBOLS` | Danh sách mã, dạng `VNM,HPG,FPT` |
| `MDP_START_DATE` / `MDP_END_DATE` | Override ngày chạy |
| `MDP_DATA_PATHS_RAW` / `MDP_DATA_PATHS_PROCESSED` | Override đường dẫn dữ liệu |
| `MDP_LOGGING_LEVEL` / `MDP_LOGGING_DIR` | Override cấu hình logging |
| `MDP_RETRY` | Số lần retry khi lỗi |
| `LOG_JSON_OUTPUT` | Bật JSON logs (`true/false`) |

Lưu ý về `vnstock`:
- API/field có thể thay đổi theo phiên bản; dự án đã có **fallback handling** ở một số extractor.
- Khi gọi dữ liệu nhiều mã, nên giữ `sleep(1)` để giảm nguy cơ bị rate-limit.

## Hướng dẫn sử dụng (Usage)

### Cách 1: Chạy qua CLI (Scripts)

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
Raw Data (vnstock)
   ↓
Cleaning & Normalization (DataCleaner)
   ↓
Technical Indicators (TechnicalIndicators)
   ↓
Analysis / Screening (TechnicalScreener, BreadthAnalyzer, FundamentalAnalyzer)
   ↓
Persist (data/processed/*.parquet) + Reports (reports/*)
```

## Thông báo lỗi (Notifications)

Pipeline có sẵn hook gửi Telegram (tuỳ chọn) khi thất bại/cảnh báo:
- `TELEGRAM_BOT_TOKEN`
- `TELEGRAM_CHAT_ID`

## License

MIT (tham khảo badge). Nếu bạn muốn public repo, hãy thêm file `LICENSE` để đồng bộ thông tin license.
