from datetime import datetime
from pathlib import Path

import pandas as pd
from time import sleep
from vnstock import Listing, Quote
import tqdm


RAW_DATA_DIR = Path(r"D:\market_data_pipeline\data\raw\ohlcv")
RAW_DATA_DIR.mkdir(parents=True, exist_ok=True)


def download_all_hsx_ohlcv(
    start_date: str = '2024-01-01',
    end_date: str | None = None,
    rate_limit_sleep: float = 1.0,
) -> None:
    """Download OHLCV for every HSX symbol and save each CSV to the raw folder."""
    end_date = end_date or datetime.utcnow().strftime('%Y-%m-%d')

    listing = Listing(source='vci')
    symbols = (
        listing.symbols_by_exchange()
        .query('exchange == "HSX" & type == "STOCK"')['symbol']
        .tolist()
    )

    for sym in tqdm.tqdm(symbols, desc='HSX OHLCV'):
        try:
            df = Quote(symbol=sym, source='vci').history(
                start=start_date,
                end=end_date,
                interval='1D',
            )
            if df.index.name is not None:
                df = df.reset_index()
            df.to_csv(RAW_DATA_DIR / f'{sym}.csv', index=False)
        except Exception as exc:
            tqdm.tqdm.write(f'Skipping {sym}: {exc}')
        finally:
            sleep(rate_limit_sleep)


def download_hsx_ohlcv_sample(
    start_date: str = '2023-01-02',
    end_date: str = '2025-07-12',
    max_symbols: int = 50,
    rate_limit_sleep: float = 1.0,
) -> None:
    """Download OHLCV for the first `max_symbols` HSX symbols."""
    listing = Listing(source='vci')
    symbols = (
        listing.symbols_by_exchange()
        .query('exchange == "HSX" & type == "STOCK"')
        .head(max_symbols)['symbol']
        .tolist()
    )

    for sym in tqdm.tqdm(symbols, desc='HSX OHLCV sample'):
        try:
            df = Quote(symbol=sym, source='vci').history(
                start=start_date,
                end=end_date,
                interval='1D',
            )
            if df.index.name is not None:
                df = df.reset_index()
            df.to_csv(RAW_DATA_DIR / f'{sym}.csv', index=False)
        except Exception as exc:
            tqdm.tqdm.write(f'Skipping {sym}: {exc}')
        finally:
            sleep(rate_limit_sleep)
