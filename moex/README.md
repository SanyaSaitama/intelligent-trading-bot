# MOEX Quotes Service

A Python service that fetches real-time quotes and market data from the Moscow Exchange (MOEX) Information System Server (ISS) API and stores them in a SQLite database.

## Features

- Fetches current orderbook data (bids/asks) for specified securities
- Retrieves market data including last price, volume, high/low prices
- Stores data in a local SQLite database with timestamped records
- Runs continuously with configurable intervals
- Includes error handling and logging

## Requirements

- Python 3.6+
- No external dependencies (uses only standard library)

## Installation

1. Ensure you have Python 3.6+ installed
2. Clone or download the MOEX folder contents
3. Run the service

## Usage

### Basic Usage

```bash
python moex_service.py
```
### Initialize database only

```bash
python moex_service.py --init
```

### Fetch current quotes (one-time)

```bash
python moex_service.py --fetch SBER GAZP
```

### Fetch historical data for a specific date

```bash
python moex_service.py --history 2026-03-10
```

You can also specify a different MOEX board:

```bash
python moex_service.py --history 2026-03-10 --history-board TQBR
```
### Configuration

Edit the `moex_config.json` file to configure the service:

```json
{
    "db_path": "moex_quotes.db",
    "securities": ["SBER", "GAZP", "LKOH", "ROSN", "VTBR", "TATN", "MGNT", "NVTK", "YNDX", "POLY"],
    "interval": 60,
    "engine": "stock",
    "market": "shares"
}
```

- `db_path`: Path to the SQLite database file
- `securities`: List of security tickers to monitor
- `interval`: Update interval in seconds
- `engine`: MOEX engine (e.g., "stock")
- `market`: MOEX market (e.g., "shares")

## Database Schema

The service creates a normalized schema that stores market metadata, securities, quotes, and full orderbook depth.

### exchanges
- `exchange_id`: Primary key
- `name`: Exchange name (e.g., MOEX)
- `url`: Base API URL
- `created_at`: Creation timestamp

### markets
- `market_id`: Primary key
- `exchange_id`: Foreign key → `exchanges.exchange_id`
- `engine`: MOEX engine (e.g., `stock`)
- `market`: MOEX market (e.g., `shares`)
- `board`: Optional board identifier

### securities
- `security_id`: Primary key
- `secid`: Security ticker (unique)
- `isin`: Security ISIN
- `short_name`: Short name
- `long_name`: Full name
- `lot_size`: Lot size
- `currency`: Trading currency
- `board_id`: Board identifier
- `market_id`: Foreign key → `markets.market_id`
- `created_at` / `updated_at`: Timestamps

### security_metadata
- `meta_id`: Primary key
- `market_id`: Foreign key → `markets.market_id`
- `response_type`: e.g., `current_securities`
- `columns_json`: Stored column list for parsing
- `fetched_at`: Timestamp

### quotes
- `quote_id`: Primary key
- `security_id`: Foreign key → `securities.security_id`
- `timestamp`: Snapshot timestamp
- `last_price`: Last traded price
- `last_change`: Price change
- `open_price`: Opening price
- `high_price`: Daily high
- `low_price`: Daily low
- `volume`: Volume
- `value`: Trade value
- `source`: API endpoint identifier
- `raw_json`: Stored raw JSON response (optional)

### orderbook_snapshots
- `orderbook_id`: Primary key
- `security_id`: Foreign key → `securities.security_id`
- `timestamp`: Snapshot timestamp
- `source`: API endpoint identifier
- `raw_json`: Stored raw JSON response (optional)

### orderbook_levels
- `level_id`: Primary key
- `orderbook_id`: Foreign key → `orderbook_snapshots.orderbook_id`
- `side`: `bid` or `ask`
- `level`: Price level (1 = top)
- `price`: Price at level
- `quantity`: Volume at level

## API Reference

The service uses the MOEX ISS API endpoints:

- `/iss/engines/stock/markets/shares/securities/{SECID}/orderbook.json` - Orderbook data
- `/iss/engines/stock/markets/shares/securities.json` - Securities market data

## Logging

Logs are written to `moex_service.log` and also displayed on console. Log levels:
- INFO: Normal operations
- ERROR: Errors and exceptions

## Notes

- The service fetches public data that doesn't require authentication
- Rate limiting is implemented to avoid overloading the MOEX servers
- Data is stored with timestamps for historical analysis
- The service can be stopped with Ctrl+C

## Troubleshooting

- Check `moex_service.log` for error details
- Ensure internet connection is available
- Verify that the securities codes are valid MOEX tickers
- Check database file permissions

## License

Based on MOEX ISS API examples, updated for Python 3.