from datetime import datetime
import pandas as pd
from vnstock import Listing, Quote
from time import sleep
import os
import tqdm


def download_hsx_ohlcv_excel(
    start_date: str = '2024-01-01',
    end_date: str | None = None,
    target_path: str = 'data/hsx_all.xlsx',
    rate_limit_sleep: float = 1.0,
) -> None:
    """Download HSX history once and store every ticker in `target_path`."""
    os.makedirs('data', exist_ok=True)
    end_date = end_date or datetime.utcnow().strftime('%Y-%m-%d')

    listing = Listing(source='vci')
    symbols = (
        listing.symbols_by_exchange()
        .query('exchange == "HSX" & type == "STOCK"')['symbol']
        .tolist()
    )

    with pd.ExcelWriter(target_path, engine='openpyxl', datetime_format='yyyy-mm-dd') as writer:
        for sym in tqdm.tqdm(symbols):
            try:
                df = Quote(symbol=sym, source='vci').history(
                    start=start_date,
                    end=end_date,
                    interval='1D',
                )
                if df.index.name is not None:
                    df = df.reset_index()
                safe_sheet = sym[:31]
                df.to_excel(writer, sheet_name=safe_sheet, index=False)
            except Exception as exc:
                tqdm.tqdm.write(f'Skipping {sym}: {exc}')
            finally:
                sleep(rate_limit_sleep)
