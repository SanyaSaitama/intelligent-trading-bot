# MOEX Quotes Service

A Python service that fetches real-time quotes and market data from the Moscow Exchange (MOEX) Information System Server (ISS) API and stores them in a SQLite database.

## Features

- Fetches current orderbook data (bids/asks) for specified securities
- Retrieves market data including last price, volume, high/low prices
- Fetches trade data for securities and markets (current and historical)
- Retrieves candlestick (OHLC) data with configurable intervals
- Gets market securities listings and board-specific data
- Fetches index analytics and market indices
- Provides comprehensive historical data for securities and markets
- Gets security specifications and index memberships
- Retrieves aggregated trading results and market statistics
- Accesses market turnovers, correlations, and capitalization data
- Fetches reference data (engines, markets, boards, security groups)
- Gets MOEX news and events
- Provides futures and options market data
- Accesses OTC and repo market data
- Gets stock splits, deviation coefficients, and quoted securities
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

## Available API Methods

The `MicexISSClient` class provides **31 comprehensive methods** for accessing MOEX ISS data, organized by category:

### Current Market Data (7 methods)
- `get_current_orderbook(engine, market, security)` - Orderbook for a security
- `get_current_securities(engine, market)` - Current securities in a market
- `get_security_trades(engine, market, security)` - Recent trades for a security
- `get_security_candles(engine, market, security, interval=1)` - Candlestick data
- `get_market_trades(engine, market)` - All trades in a market
- `get_market_orderbook(engine, market)` - Orderbooks for all securities in market
- `get_current_prices()` - Current prices for all securities

### Board-Specific Data (4 methods)
- `get_board_securities(engine, market, board)` - Securities on a trading board
- `get_board_trades(engine, market, board)` - Trades on a trading board
- `get_board_orderbook(engine, market, board)` - Orderbooks on a trading board
- `get_board_candles(engine, market, board, security, interval=1)` - Candles on a board

### Historical Data (7 methods)
- `get_history_securities(engine, market, board, date)` - Historical securities by board (legacy)
- `get_history_securities_simple(engine, market, date)` - Historical securities for a date
- `get_history_security(engine, market, security, from_date, to_date)` - Historical data for security
- `get_history_trades(engine, market, security, from_date, to_date)` - Historical trades
- `get_history_candles(engine, market, security, from_date, to_date, interval=1)` - Historical candles

### Security Information (3 methods)
- `get_security_spec(security)` - Security specification
- `get_security_indices(security)` - Indices containing the security
- `get_security_aggregates(security, date)` - Aggregated results for a date

### Market Statistics & Analytics (8 methods)
- `get_index_analytics()` - Stock market index analytics
- `get_turnovers()` - Summary turnovers by market
- `get_engine_turnovers(engine)` - Turnovers for an engine
- `get_market_turnovers(engine, market)` - Turnovers for a market
- `get_secstats(engine, market)` - Intermediate end-of-day results
- `get_correlations()` - Correlation coefficients
- `get_capitalization()` - Market capitalization data
- `get_deviation_coeffs()` - Deviation coefficients for analysis

### Reference Data (7 methods)
- `get_engines()` - List of trading engines
- `get_markets(engine)` - Markets in an engine
- `get_boards(engine, market)` - Trading boards in a market
- `get_securities_list()` - All securities on MOEX
- `get_index()` - Global ISS reference data
- `get_securitygroups()` - List of security groups
- `get_securitygroup(securitygroup)` - Details for a security group

### News & Events (2 methods)
- `get_sitenews()` - MOEX news
- `get_events()` - MOEX events

### Futures & Options (3 methods)
- `get_futures_series()` - List of futures series
- `get_options_series()` - List of options series
- `get_optionboard(asset)` - Option board for an asset

### OTC & Repo Markets (3 methods)
- `get_otc_markets()` - List of OTC markets
- `get_otc_daily(market, date)` - Daily OTC data
- `get_otc_monthly(market, year, month)` - Monthly OTC data

### Additional Statistics (3 methods)
- `get_splits()` - Stock splits and consolidations
- `get_quoted_securities()` - Securities with market quotations

```bash
python moex_service.py --history 2026-03-10 --history-board TQBR
```
### Configuration

Edit the `moex_config.json` file to configure the service:

```json
{
    "db_path": "moex_quotes.db",
    "securities": {
        "stock": ["SBER", "GAZP", "LKOH", "ROSN", "VTBR", "TATN", "MGNT", "NVTK", "YNDX", "POLY"],
        "futures": ["MXH6", "BRJ6"]
    },
    "interval": 60,
    "engines": {
        "stock": ["shares"],
        "futures": ["forts"]
    }
}
```

- `db_path`: Path to the SQLite database file
- `securities`: Dictionary mapping engines to lists of security tickers
- `interval`: Update interval in seconds
- `engines`: Dictionary mapping MOEX engines to lists of markets
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