# Real-Time Cryptocurrency Data Fetcher

A Python application that monitors cryptocurrency markets across multiple exchanges, calculates arbitrage opportunities, and stores real-time data in a database.

## Features

- **Multi-Exchange Support**: Monitors BTC prices across 11 exchanges:
  - Kraken (BTC/USD)
  - Luno (BTC/ZAR)
  - Bitbns (BTC/INR)
  - Upbit (BTC/KRW)
  - Novadax (BTC/BRL)
  - BitFlyer (BTC/JPY)
  - Binance.US (BTC/USD)
  - BTCTurk (BTC/TRY)
  - Bitso (BTC/MXN)
  - Coins.ph (BTC/PHP)
  - Bithumb (BTC/KRW)

- **Arbitrage Detection**: Calculates arbitrage opportunities between exchanges
- **Forex Integration**: Fetches real-time forex rates for currency conversions
- **Data Storage**: Stores OHLCV data, order book information, and arbitrage calculations in a database
- **Automatic Retry Logic**: Robust error handling with retry mechanisms for API failures
- **Real-time Monitoring**: Updates data every minute

## Requirements

- Python 3.7+
- PostgreSQL or compatible database
- API keys for:
  - Kraken Exchange
  - Luno Exchange
  - ForexRateAPI.com

## Installation

1. Clone or download this repository

2. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```

3. Set up environment variables:
   ```bash
   export KRAKEN_API_KEY="your_kraken_api_key"
   export KRAKEN_API_KEY_SECRET="your_kraken_api_secret"
   export LUNO_API_KEY="your_luno_api_key"
   export LUNO_API_KEY_SECRET="your_luno_api_secret"
   export FOREX_API_KEY="your_forex_api_key"
   export DATABASE_URL="postgresql://user:password@localhost/dbname"
   ```

   Or create a `.env` file:
   ```
   KRAKEN_API_KEY=your_kraken_api_key
   KRAKEN_API_KEY_SECRET=your_kraken_api_secret
   LUNO_API_KEY=your_luno_api_key
   LUNO_API_KEY_SECRET=your_luno_api_secret
   FOREX_API_KEY=your_forex_api_key
   DATABASE_URL=postgresql://user:password@localhost/dbname
   ```

## Usage

Run the script:
```bash
python "real_time_fetcher.py"
```

The script will:
- Connect to all exchanges
- Initialize the database connection
- Fetch and store data every minute
- Calculate arbitrage opportunities
- Log significant arbitrage opportunities (>0.5%)

## Database Schema

The script creates a table `realtime_crypto_data` with the following structure:

- `id`: Primary key (Integer)
- `timestamp`: DateTime of data collection
- For each exchange: `open`, `high`, `low`, `close`, `volume`, `ask`, `bid`, `ask_volume`, `bid_volume`, `volume_24h`, `spread_pct`
- Forex rates: `usd_inr_rate`, `usd_krw_rate`, `usd_zar_rate`, `usd_brl_rate`, `usd_jpy_rate`, `usd_try_rate`, `usd_mxn_rate`, `usd_php_rate`
- Arbitrage calculations between exchanges

## Exchange Data Collected

For each exchange, the script collects:
- OHLCV (Open, High, Low, Close, Volume) data
- Order book data (best bid/ask prices)
- Bid/ask volumes
- 24-hour volume
- Bid-ask spread percentage

## Arbitrage Calculation

The script calculates arbitrage opportunities by:
1. Buying BTC on Kraken at ask price (USD)
2. Selling BTC on another exchange at bid price (converted to USD)
3. Comparing the profit margin

Positive values indicate profitable arbitrage opportunities.

## Error Handling

- Automatic retry mechanism for API failures (3 attempts)
- Database connection retry with 5-second intervals
- Automatic exchange reconnection after 5 consecutive failures
- Critical error handling with automatic script restart

## Logging

The script logs:
- Successful data storage with timestamps
- Arbitrage opportunities greater than 0.5%
- Errors and retry attempts

## Notes

- The script runs indefinitely until interrupted
- Data is fetched and stored every minute
- Only Kraken and Luno require API keys for authentication
- Other exchanges use public endpoints (no API keys required)


