import ccxt
import requests
import pandas as pd
from datetime import datetime
import time
import traceback
from sqlalchemy import create_engine, Column, Integer, Float, DateTime
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
import os
import logging

Base = declarative_base()

class RealTimeData(Base):
    __tablename__ = 'realtime_crypto_data'
    
    id = Column(Integer, primary_key=True)
    timestamp = Column(DateTime, nullable=False)
    
    # Kraken BTC/USD data
    btc_usd_open = Column(Float)
    btc_usd_high = Column(Float)
    btc_usd_low = Column(Float)
    btc_usd_close = Column(Float)
    btc_usd_volume = Column(Float)
    btc_usd_ask = Column(Float)
    btc_usd_bid = Column(Float)
    btc_usd_ask_volume = Column(Float)
    btc_usd_bid_volume = Column(Float)
    btc_usd_volume_24h = Column(Float)
    btc_usd_spread_pct = Column(Float)
    
    # Luno BTC/ZAR data
    btc_zar_open = Column(Float)
    btc_zar_high = Column(Float)
    btc_zar_low = Column(Float)
    btc_zar_close = Column(Float)
    btc_zar_volume = Column(Float)
    btc_zar_ask = Column(Float)
    btc_zar_bid = Column(Float)
    btc_zar_ask_volume = Column(Float)
    btc_zar_bid_volume = Column(Float)
    btc_zar_volume_24h = Column(Float)
    btc_zar_spread_pct = Column(Float)

    # Bitbns BTC/INR data
    bitbns_open = Column(Float)
    bitbns_high = Column(Float)
    bitbns_low = Column(Float)
    bitbns_close = Column(Float)
    bitbns_volume = Column(Float)
    bitbns_ask = Column(Float)
    bitbns_bid = Column(Float)
    bitbns_ask_volume = Column(Float)
    bitbns_bid_volume = Column(Float)
    bitbns_volume_24h = Column(Float)
    bitbns_spread_pct = Column(Float)

    # Upbit BTC/KRW data
    upbit_open = Column(Float)
    upbit_high = Column(Float)
    upbit_low = Column(Float)
    upbit_close = Column(Float)
    upbit_volume = Column(Float)
    upbit_ask = Column(Float)
    upbit_bid = Column(Float)
    upbit_ask_volume = Column(Float)
    upbit_bid_volume = Column(Float)
    upbit_volume_24h = Column(Float)
    upbit_spread_pct = Column(Float)

   # Novadax BTC/BRL data
    novadax_open = Column(Float)
    novadax_high = Column(Float)
    novadax_low = Column(Float)
    novadax_close = Column(Float)
    novadax_volume = Column(Float)
    novadax_ask = Column(Float)
    novadax_bid = Column(Float)
    novadax_ask_volume = Column(Float)
    novadax_bid_volume = Column(Float)
    novadax_volume_24h = Column(Float)
    novadax_spread_pct = Column(Float)

    # BitFlyer BTC/JPY data
    bitflyer_open = Column(Float)
    bitflyer_high = Column(Float)
    bitflyer_low = Column(Float)
    bitflyer_close = Column(Float)
    bitflyer_volume = Column(Float)
    bitflyer_ask = Column(Float)
    bitflyer_bid = Column(Float)
    bitflyer_ask_volume = Column(Float)
    bitflyer_bid_volume = Column(Float)
    bitflyer_volume_24h = Column(Float)
    bitflyer_spread_pct = Column(Float)

    # Binance.US BTC/USD data
    binanceus_open = Column(Float)
    binanceus_high = Column(Float)
    binanceus_low = Column(Float)
    binanceus_close = Column(Float)
    binanceus_volume = Column(Float)
    binanceus_ask = Column(Float)
    binanceus_bid = Column(Float)
    binanceus_ask_volume = Column(Float)
    binanceus_bid_volume = Column(Float)
    binanceus_volume_24h = Column(Float)
    binanceus_spread_pct = Column(Float)

    # BTCTurk BTC/TRY data
    btcturk_open = Column(Float)
    btcturk_high = Column(Float)
    btcturk_low = Column(Float)
    btcturk_close = Column(Float)
    btcturk_volume = Column(Float)
    btcturk_ask = Column(Float)
    btcturk_bid = Column(Float)
    btcturk_ask_volume = Column(Float)
    btcturk_bid_volume = Column(Float)
    btcturk_volume_24h = Column(Float)
    btcturk_spread_pct = Column(Float)
    
    # Bitso BTC/MXN data
    bitso_open = Column(Float)
    bitso_high = Column(Float)
    bitso_low = Column(Float)
    bitso_close = Column(Float)
    bitso_volume = Column(Float)
    bitso_ask = Column(Float)
    bitso_bid = Column(Float)
    bitso_ask_volume = Column(Float)
    bitso_bid_volume = Column(Float)
    bitso_volume_24h = Column(Float)
    bitso_spread_pct = Column(Float)
    
    # Coins.ph BTC/PHP data
    coinsph_open = Column(Float)
    coinsph_high = Column(Float)
    coinsph_low = Column(Float)
    coinsph_close = Column(Float)
    coinsph_volume = Column(Float)
    coinsph_ask = Column(Float)
    coinsph_bid = Column(Float)
    coinsph_ask_volume = Column(Float)
    coinsph_bid_volume = Column(Float)
    coinsph_volume_24h = Column(Float)
    coinsph_spread_pct = Column(Float)
    
    # Bithumb BTC/KRW data
    bithumb_open = Column(Float)
    bithumb_high = Column(Float)
    bithumb_low = Column(Float)
    bithumb_close = Column(Float)
    bithumb_volume = Column(Float)
    bithumb_ask = Column(Float)
    bithumb_bid = Column(Float)
    bithumb_ask_volume = Column(Float)
    bithumb_bid_volume = Column(Float)
    bithumb_volume_24h = Column(Float)
    bithumb_spread_pct = Column(Float)
    
    # Forex rates for arbitrage
    usd_inr_rate = Column(Float)
    usd_krw_rate = Column(Float)
    usd_zar_rate = Column(Float)
    usd_brl_rate = Column(Float)
    usd_jpy_rate = Column(Float)
    usd_try_rate = Column(Float)
    usd_mxn_rate = Column(Float)
    usd_php_rate = Column(Float)
    
    # Arbitrage opportunity
    kraken_luno_arbitrage = Column(Float)
    kraken_bitbns_arbitrage = Column(Float)
    kraken_upbit_arbitrage = Column(Float)
    kraken_novadax_arbitrage = Column(Float)
    kraken_bitflyer_arbitrage = Column(Float)
    kraken_binanceus_arbitrage = Column(Float)
    kraken_btcturk_arbitrage = Column(Float)
    kraken_bitso_arbitrage = Column(Float)
    kraken_coinsph_arbitrage = Column(Float)
    kraken_bithumb_arbitrage = Column(Float)

class RealTimeFetcher:
    def __init__(self):
        # Set up logging
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')
        # API Keys stored directly in the script
        kraken_api_key = os.getenv("KRAKEN_API_KEY")
        kraken_api_secret = os.getenv("KRAKEN_API_KEY_SECRET")
        luno_api_key = os.getenv("LUNO_API_KEY")
        luno_api_secret = os.getenv("LUNO_API_KEY_SECRET")
        self.forexrateapi_key = os.getenv("FOREX_API_KEY")
        
        # Initialize exchange connections with retry mechanism
        self.initialize_exchanges(kraken_api_key, kraken_api_secret, 
                                luno_api_key, luno_api_secret)
        
        # Initialize database
        while True:
            try:
                self.engine = create_engine(os.environ["DATABASE_URL"])
                Base.metadata.create_all(self.engine)
                self.Session = sessionmaker(bind=self.engine)
                break
            except Exception as e:
                print(f"Error initializing database: {e}")
                print("Retrying in 5 seconds...")
                time.sleep(5)

    def initialize_exchanges(self, kraken_api_key, kraken_api_secret, 
                           luno_api_key, luno_api_secret):
        """Initialize exchange connections with retry mechanism"""
        while True:
            try:
                self.kraken = ccxt.kraken({
                    'apiKey': kraken_api_key,
                    'secret': kraken_api_secret,
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })
                
                self.luno = ccxt.luno({
                    'apiKey': luno_api_key,
                    'secret': luno_api_secret,
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Bitbns (public endpoints, no API key needed)
                self.bitbns = ccxt.bitbns({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Upbit (public endpoints, no API key needed)
                self.upbit = ccxt.upbit({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Novadax (public endpoints, no API key needed)
                self.novadax = ccxt.novadax({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize BitFlyer (public endpoints, no API key needed)
                self.bitflyer = ccxt.bitflyer({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Binance.US (public endpoints, no API key needed)
                self.binanceus = ccxt.binanceus({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize BTCTurk (public endpoints, no API key needed)
                self.btcturk = ccxt.btcturk({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Bitso (public endpoints, no API key needed)
                self.bitso = ccxt.bitso({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Coins.ph (public endpoints, no API key needed)
                self.coinsph = ccxt.coinsph({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })

                # Initialize Bithumb (public endpoints, no API key needed)
                self.bithumb = ccxt.bithumb({
                    'enableRateLimit': True,
                    'options': {'adjustForTimeDifference': True}
                })
                break
            except Exception as e:
                print(f"Error initializing exchanges: {e}")
                print("Retrying in 5 seconds...")
                time.sleep(5)

    def fetch_with_retry(self, fetch_func, max_retries=3, retry_delay=5):
        """Generic retry mechanism for fetch operations"""
        for attempt in range(max_retries):
            try:
                return fetch_func()
            except Exception as e:
                if attempt == max_retries - 1:  # Last attempt
                    print(f"Failed after {max_retries} attempts: {e}")
                    return None
                print(f"Attempt {attempt + 1} failed: {e}")
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    def calculate_forward_arbitrage(self, kraken_data, luno_data, usd_zar_rate):
        """
        Calculate forward arbitrage opportunity:
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Luno
        3. Sell BTC for ZAR on Luno at bid price
        4. Convert back to USD for comparison
        
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            # Cost to buy 1 BTC on Kraken in USD
            kraken_buy_price_usd = kraken_data['ask']
            
            # Revenue from selling 1 BTC on Luno in ZAR
            luno_sell_price_zar = luno_data['bid']
            
            # Convert Luno revenue to USD
            luno_sell_price_usd = luno_sell_price_zar / usd_zar_rate
            
            # Calculate arbitrage percentage
            arbitrage_pct = ((luno_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            
            return arbitrage_pct
            
        except Exception as e:
            print(f"Error calculating arbitrage: {e}")
            return None

    def calculate_kraken_bitbns_arbitrage(self, kraken_data, bitbns_data, usd_inr_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Bitbns (BTC/INR):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Bitbns
        3. Sell BTC for INR on Bitbns at bid price
        4. Convert INR to USD for comparison using USD/INR
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            bitbns_sell_price_inr = bitbns_data['bid']
            bitbns_sell_price_usd = bitbns_sell_price_inr / usd_inr_rate
            arbitrage_pct = ((bitbns_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Bitbns arbitrage: {e}")
            return None

    def calculate_kraken_upbit_arbitrage(self, kraken_data, upbit_data, usd_krw_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Upbit (BTC/KRW):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Upbit
        3. Sell BTC for KRW on Upbit at bid price
        4. Convert KRW to USD for comparison using USD/KRW
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            upbit_sell_price_krw = upbit_data['bid']
            upbit_sell_price_usd = upbit_sell_price_krw / usd_krw_rate
            arbitrage_pct = ((upbit_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Upbit arbitrage: {e}")
            return None

    def calculate_kraken_novadax_arbitrage(self, kraken_data, novadax_data, usd_brl_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Novadax (BTC/BRL):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Novadax
        3. Sell BTC for BRL on Novadax at bid price
        4. Convert BRL to USD for comparison using USD/BRL (divide BRL by USD/BRL)
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            novadax_sell_price_brl = novadax_data['bid']
            novadax_sell_price_usd = novadax_sell_price_brl / usd_brl_rate
            arbitrage_pct = ((novadax_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Novadax arbitrage: {e}")
            return None

    def calculate_kraken_bitflyer_arbitrage(self, kraken_data, bitflyer_data, usd_jpy_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and BitFlyer (BTC/JPY):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to BitFlyer
        3. Sell BTC for JPY on BitFlyer at bid price
        4. Convert JPY to USD for comparison using USD/JPY
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            bitflyer_sell_price_jpy = bitflyer_data['bid']
            bitflyer_sell_price_usd = bitflyer_sell_price_jpy / usd_jpy_rate
            arbitrage_pct = ((bitflyer_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-BitFlyer arbitrage: {e}")
            return None

    def fetch_kraken_data(self):
        def _fetch():
            ohlcv = self.kraken.fetch_ohlcv('BTC/USD', '1m', limit=1)[0]
            order_book = self.kraken.fetch_order_book('BTC/USD', limit=20)
            ticker = self.kraken.fetch_ticker('BTC/USD')
            return {
                'open': ohlcv[1],
                'high': ohlcv[2],
                'low': ohlcv[3],
                'close': ohlcv[4],
                'volume': ohlcv[5],
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def fetch_luno_data(self):
        def _fetch():
            ohlcv = self.luno.fetch_ohlcv('BTC/ZAR', '1m', limit=1)[0]
            order_book = self.luno.fetch_order_book('BTC/ZAR', limit=20)
            ticker = self.luno.fetch_ticker('BTC/ZAR')
            return {
                'open': ohlcv[1],
                'high': ohlcv[2],
                'low': ohlcv[3],
                'close': ohlcv[4],
                'volume': ohlcv[5],
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def fetch_usd_zar_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=ZAR'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['ZAR'])
        return self.fetch_with_retry(_fetch)

    def fetch_usdt_usd_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USDT&symbols=USD'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['USD'])
        return self.fetch_with_retry(_fetch)

    def fetch_usd_inr_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=INR'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['INR'])
        return self.fetch_with_retry(_fetch)

    def fetch_usd_krw_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=KRW'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['KRW'])
        return self.fetch_with_retry(_fetch)

    def fetch_usd_brl_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=BRL'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['BRL'])
        return self.fetch_with_retry(_fetch)

    def fetch_bitbns_data(self):
        def _fetch():
            try:
                trades = self.bitbns.fetch_trades('BTC/INR', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.bitbns.fetch_ticker('BTC/INR')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.bitbns.fetch_ticker('BTC/INR')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.bitbns.fetch_order_book('BTC/INR', limit=20)
            ticker = self.bitbns.fetch_ticker('BTC/INR')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def fetch_upbit_data(self):
        def _fetch():
            ohlcv = self.upbit.fetch_ohlcv('BTC/KRW', '1m', limit=1)[0]
            order_book = self.upbit.fetch_order_book('BTC/KRW', limit=20)
            ticker = self.upbit.fetch_ticker('BTC/KRW')
            return {
                'open': ohlcv[1],
                'high': ohlcv[2],
                'low': ohlcv[3],
                'close': ohlcv[4],
                'volume': ohlcv[5],
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def fetch_novadax_brl_usd_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=BRL&symbols=USD'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['USD'])
        return self.fetch_with_retry(_fetch)

    def fetch_novadax_data(self):
        def _fetch():
            try:
                trades = self.novadax.fetch_trades('BTC/BRL', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.novadax.fetch_ticker('BTC/BRL')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.novadax.fetch_ticker('BTC/BRL')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.novadax.fetch_order_book('BTC/BRL', limit=20)
            ticker = self.novadax.fetch_ticker('BTC/BRL')
            usd_brl_rate = self.fetch_usd_brl_rate()
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None),
                'usd_brl_rate': usd_brl_rate
            }
        return self.fetch_with_retry(_fetch)

    def fetch_usd_jpy_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=JPY'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['JPY'])
        return self.fetch_with_retry(_fetch)

    def fetch_bitflyer_data(self):
        def _fetch():
            try:
                trades = self.bitflyer.fetch_trades('BTC/JPY', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.bitflyer.fetch_ticker('BTC/JPY')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.bitflyer.fetch_ticker('BTC/JPY')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.bitflyer.fetch_order_book('BTC/JPY', limit=20)
            ticker = self.bitflyer.fetch_ticker('BTC/JPY')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def fetch_binanceus_data(self):
        def _fetch():
            ohlcv = self.binanceus.fetch_ohlcv('BTC/USD', '1m', limit=1)[0]
            order_book = self.binanceus.fetch_order_book('BTC/USD', limit=20)
            ticker = self.binanceus.fetch_ticker('BTC/USD')
            return {
                'open': ohlcv[1],
                'high': ohlcv[2],
                'low': ohlcv[3],
                'close': ohlcv[4],
                'volume': ohlcv[5],
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def calculate_kraken_binanceus_arbitrage(self, kraken_data, binanceus_data):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Binance.US (BTC/USD):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Binance.US
        3. Sell BTC for USD on Binance.US at bid price
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            binanceus_sell_price_usd = binanceus_data['bid']
            arbitrage_pct = ((binanceus_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Binance.US arbitrage: {e}")
            return None

    def fetch_usd_try_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=TRY'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['TRY'])
        return self.fetch_with_retry(_fetch)

    def fetch_btcturk_data(self):
        def _fetch():
            try:
                trades = self.btcturk.fetch_trades('BTC/TRY', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.btcturk.fetch_ticker('BTC/TRY')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.btcturk.fetch_ticker('BTC/TRY')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.btcturk.fetch_order_book('BTC/TRY', limit=20)
            ticker = self.btcturk.fetch_ticker('BTC/TRY')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def calculate_kraken_btcturk_arbitrage(self, kraken_data, btcturk_data, usd_try_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and BTCTurk (BTC/TRY):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to BTCTurk
        3. Sell BTC for TRY on BTCTurk at bid price
        4. Convert TRY to USD for comparison using USD/TRY
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            btcturk_sell_price_try = btcturk_data['bid']
            btcturk_sell_price_usd = btcturk_sell_price_try / usd_try_rate
            arbitrage_pct = ((btcturk_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-BTCTurk arbitrage: {e}")
            return None

    def fetch_usd_mxn_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=MXN'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['MXN'])
        return self.fetch_with_retry(_fetch)

    def fetch_bitso_data(self):
        def _fetch():
            try:
                trades = self.bitso.fetch_trades('BTC/MXN', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.bitso.fetch_ticker('BTC/MXN')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.bitso.fetch_ticker('BTC/MXN')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.bitso.fetch_order_book('BTC/MXN', limit=20)
            ticker = self.bitso.fetch_ticker('BTC/MXN')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def calculate_kraken_bitso_arbitrage(self, kraken_data, bitso_data, usd_mxn_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Bitso (BTC/MXN):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Bitso
        3. Sell BTC for MXN on Bitso at bid price
        4. Convert MXN to USD for comparison using USD/MXN
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            bitso_sell_price_mxn = bitso_data['bid']
            bitso_sell_price_usd = bitso_sell_price_mxn / usd_mxn_rate
            arbitrage_pct = ((bitso_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Bitso arbitrage: {e}")
            return None

    def fetch_usd_php_rate(self):
        def _fetch():
            url = f'https://api.forexrateapi.com/v1/latest?api_key={self.forexrateapi_key}&base=USD&symbols=PHP'
            response = requests.get(url)
            data = response.json()
            return float(data['rates']['PHP'])
        return self.fetch_with_retry(_fetch)

    def fetch_coinsph_data(self):
        def _fetch():
            try:
                trades = self.coinsph.fetch_trades('BTC/PHP', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.coinsph.fetch_ticker('BTC/PHP')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.coinsph.fetch_ticker('BTC/PHP')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.coinsph.fetch_order_book('BTC/PHP', limit=20)
            ticker = self.coinsph.fetch_ticker('BTC/PHP')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def calculate_kraken_coinsph_arbitrage(self, kraken_data, coinsph_data, usd_php_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Coins.ph (BTC/PHP):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Coins.ph
        3. Sell BTC for PHP on Coins.ph at bid price
        4. Convert PHP to USD for comparison using USD/PHP
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            coinsph_sell_price_php = coinsph_data['bid']
            coinsph_sell_price_usd = coinsph_sell_price_php / usd_php_rate
            arbitrage_pct = ((coinsph_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Coins.ph arbitrage: {e}")
            return None

    def fetch_bithumb_data(self):
        def _fetch():
            try:
                trades = self.bithumb.fetch_trades('BTC/KRW', limit=100)
                now = int(time.time() * 1000)
                one_minute_ago = now - 60_000
                recent_trades = [t for t in trades if t['timestamp'] >= one_minute_ago]
                if recent_trades:
                    prices = [t['price'] for t in recent_trades]
                    volumes = [t['amount'] for t in recent_trades]
                    open_ = prices[0]
                    high_ = max(prices)
                    low_ = min(prices)
                    close_ = prices[-1]
                    volume_ = sum(volumes)
                else:
                    ticker = self.bithumb.fetch_ticker('BTC/KRW')
                    open_ = high_ = low_ = close_ = ticker['last']
                    volume_ = ticker.get('baseVolume', 0)
            except Exception:
                ticker = self.bithumb.fetch_ticker('BTC/KRW')
                open_ = high_ = low_ = close_ = ticker['last']
                volume_ = ticker.get('baseVolume', 0)
            order_book = self.bithumb.fetch_order_book('BTC/KRW', limit=20)
            ticker = self.bithumb.fetch_ticker('BTC/KRW')
            return {
                'open': open_,
                'high': high_,
                'low': low_,
                'close': close_,
                'volume': volume_,
                'ask': order_book['asks'][0][0],
                'bid': order_book['bids'][0][0],
                'ask_volume': sum(ask[1] for ask in order_book['asks'][:20]),
                'bid_volume': sum(bid[1] for bid in order_book['bids'][:20]),
                'volume_24h': ticker.get('baseVolume', None)
            }
        return self.fetch_with_retry(_fetch)

    def calculate_kraken_bithumb_arbitrage(self, kraken_data, bithumb_data, usd_krw_rate):
        """
        Calculate arbitrage between Kraken (BTC/USD) and Bithumb (BTC/KRW):
        1. Buy BTC with USD on Kraken at ask price
        2. Transfer to Bithumb
        3. Sell BTC for KRW on Bithumb at bid price
        4. Convert KRW to USD for comparison using USD/KRW
        Returns arbitrage percentage (positive means profitable opportunity)
        """
        try:
            kraken_buy_price_usd = kraken_data['ask']
            bithumb_sell_price_krw = bithumb_data['bid']
            bithumb_sell_price_usd = bithumb_sell_price_krw / usd_krw_rate
            arbitrage_pct = ((bithumb_sell_price_usd - kraken_buy_price_usd) / kraken_buy_price_usd) * 100
            return arbitrage_pct
        except Exception as e:
            print(f"Error calculating Kraken-Bithumb arbitrage: {e}")
            return None

    def store_data(self, kraken_data, luno_data, usd_zar_rate, bitbns_data, upbit_data, novadax_data, bitflyer_data, binanceus_data, btcturk_data, bitso_data, coinsph_data, bithumb_data, usd_inr_rate, usd_krw_rate, usd_brl_rate, usd_jpy_rate, usd_try_rate, usd_mxn_rate, usd_php_rate):
        max_retries = 3
        for attempt in range(max_retries):
            session = self.Session()
            try:
                # Calculate arbitrages
                forward_arbitrage = self.calculate_forward_arbitrage(
                    kraken_data, luno_data, usd_zar_rate
                )
                kraken_bitbns_arbitrage = self.calculate_kraken_bitbns_arbitrage(
                    kraken_data, bitbns_data, usd_inr_rate
                )
                kraken_upbit_arbitrage = self.calculate_kraken_upbit_arbitrage(
                    kraken_data, upbit_data, usd_krw_rate
                )
                kraken_novadax_arbitrage = self.calculate_kraken_novadax_arbitrage(
                    kraken_data, novadax_data, usd_brl_rate
                )
                kraken_bitflyer_arbitrage = self.calculate_kraken_bitflyer_arbitrage(
                    kraken_data, bitflyer_data, usd_jpy_rate
                )
                kraken_binanceus_arbitrage = self.calculate_kraken_binanceus_arbitrage(
                    kraken_data, binanceus_data
                )
                kraken_btcturk_arbitrage = self.calculate_kraken_btcturk_arbitrage(
                    kraken_data, btcturk_data, usd_try_rate
                )
                kraken_bitso_arbitrage = self.calculate_kraken_bitso_arbitrage(
                    kraken_data, bitso_data, usd_mxn_rate
                )
                kraken_coinsph_arbitrage = self.calculate_kraken_coinsph_arbitrage(
                    kraken_data, coinsph_data, usd_php_rate
                )
                kraken_bithumb_arbitrage = self.calculate_kraken_bithumb_arbitrage(
                    kraken_data, bithumb_data, usd_krw_rate
                )
                # Calculate bid-ask spread percentages
                btc_usd_spread_pct = 100 * (kraken_data['ask'] - kraken_data['bid']) / ((kraken_data['ask'] + kraken_data['bid']) / 2)
                btc_zar_spread_pct = 100 * (luno_data['ask'] - luno_data['bid']) / ((luno_data['ask'] + luno_data['bid']) / 2)
                bitbns_spread_pct = 100 * (bitbns_data['ask'] - bitbns_data['bid']) / ((bitbns_data['ask'] + bitbns_data['bid']) / 2)
                upbit_spread_pct = 100 * (upbit_data['ask'] - upbit_data['bid']) / ((upbit_data['ask'] + upbit_data['bid']) / 2)
                novadax_spread_pct = 100 * (novadax_data['ask'] - novadax_data['bid']) / ((novadax_data['ask'] + novadax_data['bid']) / 2)
                bitflyer_spread_pct = 100 * (bitflyer_data['ask'] - bitflyer_data['bid']) / ((bitflyer_data['ask'] + bitflyer_data['bid']) / 2)
                coinbase_spread_pct = 100 * (binanceus_data['ask'] - binanceus_data['bid']) / ((binanceus_data['ask'] + binanceus_data['bid']) / 2)
                btcturk_spread_pct = 100 * (btcturk_data['ask'] - btcturk_data['bid']) / ((btcturk_data['ask'] + btcturk_data['bid']) / 2)
                bitso_spread_pct = 100 * (bitso_data['ask'] - bitso_data['bid']) / ((bitso_data['ask'] + bitso_data['bid']) / 2)
                coinsph_spread_pct = 100 * (coinsph_data['ask'] - coinsph_data['bid']) / ((coinsph_data['ask'] + coinsph_data['bid']) / 2)
                bithumb_spread_pct = 100 * (bithumb_data['ask'] - bithumb_data['bid']) / ((bithumb_data['ask'] + bithumb_data['bid']) / 2)
                entry = RealTimeData(
                    timestamp=datetime.utcnow(),
                    # Kraken data
                    btc_usd_open=kraken_data['open'],
                    btc_usd_high=kraken_data['high'],
                    btc_usd_low=kraken_data['low'],
                    btc_usd_close=kraken_data['close'],
                    btc_usd_volume=kraken_data['volume'],
                    btc_usd_ask=kraken_data['ask'],
                    btc_usd_bid=kraken_data['bid'],
                    btc_usd_ask_volume=kraken_data['ask_volume'],
                    btc_usd_bid_volume=kraken_data['bid_volume'],
                    btc_usd_volume_24h=kraken_data.get('volume_24h', None),
                    btc_usd_spread_pct=btc_usd_spread_pct,
                    # Luno data
                    btc_zar_open=luno_data['open'],
                    btc_zar_high=luno_data['high'],
                    btc_zar_low=luno_data['low'],
                    btc_zar_close=luno_data['close'],
                    btc_zar_volume=luno_data['volume'],
                    btc_zar_ask=luno_data['ask'],
                    btc_zar_bid=luno_data['bid'],
                    btc_zar_ask_volume=luno_data['ask_volume'],
                    btc_zar_bid_volume=luno_data['bid_volume'],
                    btc_zar_volume_24h=luno_data.get('volume_24h', None),
                    btc_zar_spread_pct=btc_zar_spread_pct,
                    # Bitbns data
                    bitbns_open=bitbns_data['open'],
                    bitbns_high=bitbns_data['high'],
                    bitbns_low=bitbns_data['low'],
                    bitbns_close=bitbns_data['close'],
                    bitbns_volume=bitbns_data['volume'],
                    bitbns_ask=bitbns_data['ask'],
                    bitbns_bid=bitbns_data['bid'],
                    bitbns_ask_volume=bitbns_data['ask_volume'],
                    bitbns_bid_volume=bitbns_data['bid_volume'],
                    bitbns_volume_24h=bitbns_data.get('volume_24h', None),
                    bitbns_spread_pct=bitbns_spread_pct,
                    # Upbit data
                    upbit_open=upbit_data['open'],
                    upbit_high=upbit_data['high'],
                    upbit_low=upbit_data['low'],
                    upbit_close=upbit_data['close'],
                    upbit_volume=upbit_data['volume'],
                    upbit_ask=upbit_data['ask'],
                    upbit_bid=upbit_data['bid'],
                    upbit_ask_volume=upbit_data['ask_volume'],
                    upbit_bid_volume=upbit_data['bid_volume'],
                    upbit_volume_24h=upbit_data.get('volume_24h', None),
                    upbit_spread_pct=upbit_spread_pct,
                    # Novadax data
                    novadax_open=novadax_data['open'],
                    novadax_high=novadax_data['high'],
                    novadax_low=novadax_data['low'],
                    novadax_close=novadax_data['close'],
                    novadax_volume=novadax_data['volume'],
                    novadax_ask=novadax_data['ask'],
                    novadax_bid=novadax_data['bid'],
                    novadax_ask_volume=novadax_data['ask_volume'],
                    novadax_bid_volume=novadax_data['bid_volume'],
                    novadax_volume_24h=novadax_data.get('volume_24h', None),
                    novadax_spread_pct=novadax_spread_pct,
                    # BitFlyer data
                    bitflyer_open=bitflyer_data['open'],
                    bitflyer_high=bitflyer_data['high'],
                    bitflyer_low=bitflyer_data['low'],
                    bitflyer_close=bitflyer_data['close'],
                    bitflyer_volume=bitflyer_data['volume'],
                    bitflyer_ask=bitflyer_data['ask'],
                    bitflyer_bid=bitflyer_data['bid'],
                    bitflyer_ask_volume=bitflyer_data['ask_volume'],
                    bitflyer_bid_volume=bitflyer_data['bid_volume'],
                    bitflyer_volume_24h=bitflyer_data.get('volume_24h', None),
                    bitflyer_spread_pct=bitflyer_spread_pct,
                    # Binance.US data
                    binanceus_open=binanceus_data['open'],
                    binanceus_high=binanceus_data['high'],
                    binanceus_low=binanceus_data['low'],
                    binanceus_close=binanceus_data['close'],
                    binanceus_volume=binanceus_data['volume'],
                    binanceus_ask=binanceus_data['ask'],
                    binanceus_bid=binanceus_data['bid'],
                    binanceus_ask_volume=binanceus_data['ask_volume'],
                    binanceus_bid_volume=binanceus_data['bid_volume'],
                    binanceus_volume_24h=binanceus_data.get('volume_24h', None),
                    binanceus_spread_pct=coinbase_spread_pct,
                    # BTCTurk data
                    btcturk_open=btcturk_data['open'],
                    btcturk_high=btcturk_data['high'],
                    btcturk_low=btcturk_data['low'],
                    btcturk_close=btcturk_data['close'],
                    btcturk_volume=btcturk_data['volume'],
                    btcturk_ask=btcturk_data['ask'],
                    btcturk_bid=btcturk_data['bid'],
                    btcturk_ask_volume=btcturk_data['ask_volume'],
                    btcturk_bid_volume=btcturk_data['bid_volume'],
                    btcturk_volume_24h=btcturk_data.get('volume_24h', None),
                    btcturk_spread_pct=btcturk_spread_pct,
                    # Bitso data
                    bitso_open=bitso_data['open'],
                    bitso_high=bitso_data['high'],
                    bitso_low=bitso_data['low'],
                    bitso_close=bitso_data['close'],
                    bitso_volume=bitso_data['volume'],
                    bitso_ask=bitso_data['ask'],
                    bitso_bid=bitso_data['bid'],
                    bitso_ask_volume=bitso_data['ask_volume'],
                    bitso_bid_volume=bitso_data['bid_volume'],
                    bitso_volume_24h=bitso_data.get('volume_24h', None),
                    bitso_spread_pct=bitso_spread_pct,
                    # Coins.ph data
                    coinsph_open=coinsph_data['open'],
                    coinsph_high=coinsph_data['high'],
                    coinsph_low=coinsph_data['low'],
                    coinsph_close=coinsph_data['close'],
                    coinsph_volume=coinsph_data['volume'],
                    coinsph_ask=coinsph_data['ask'],
                    coinsph_bid=coinsph_data['bid'],
                    coinsph_ask_volume=coinsph_data['ask_volume'],
                    coinsph_bid_volume=coinsph_data['bid_volume'],
                    coinsph_volume_24h=coinsph_data.get('volume_24h', None),
                    coinsph_spread_pct=coinsph_spread_pct,
                    # Bithumb data
                    bithumb_open=bithumb_data['open'],
                    bithumb_high=bithumb_data['high'],
                    bithumb_low=bithumb_data['low'],
                    bithumb_close=bithumb_data['close'],
                    bithumb_volume=bithumb_data['volume'],
                    bithumb_ask=bithumb_data['ask'],
                    bithumb_bid=bithumb_data['bid'],
                    bithumb_ask_volume=bithumb_data['ask_volume'],
                    bithumb_bid_volume=bithumb_data['bid_volume'],
                    bithumb_volume_24h=bithumb_data.get('volume_24h', None),
                    bithumb_spread_pct=bithumb_spread_pct,
                    # Exchange rates and arbitrage
                    usd_zar_rate=usd_zar_rate,
                    usd_inr_rate=usd_inr_rate,
                    usd_krw_rate=usd_krw_rate,
                    usd_brl_rate=usd_brl_rate,
                    usd_jpy_rate=usd_jpy_rate,
                    usd_try_rate=usd_try_rate,
                    usd_mxn_rate=usd_mxn_rate,
                    usd_php_rate=usd_php_rate,
                    kraken_luno_arbitrage=forward_arbitrage,
                    kraken_bitbns_arbitrage=kraken_bitbns_arbitrage,
                    kraken_upbit_arbitrage=kraken_upbit_arbitrage,
                    kraken_novadax_arbitrage=kraken_novadax_arbitrage,
                    kraken_bitflyer_arbitrage=kraken_bitflyer_arbitrage,
                    kraken_binanceus_arbitrage=kraken_binanceus_arbitrage,
                    kraken_btcturk_arbitrage=kraken_btcturk_arbitrage,
                    kraken_bitso_arbitrage=kraken_bitso_arbitrage,
                    kraken_coinsph_arbitrage=kraken_coinsph_arbitrage,
                    kraken_bithumb_arbitrage=kraken_bithumb_arbitrage
                )
                session.add(entry)
                session.commit()
                logging.info(f"Data stored successfully at {entry.timestamp}")
                if forward_arbitrage and abs(forward_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Luno): {forward_arbitrage:.2f}%")
                if kraken_bitbns_arbitrage and abs(kraken_bitbns_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Bitbns): {kraken_bitbns_arbitrage:.2f}%")
                if kraken_upbit_arbitrage and abs(kraken_upbit_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Upbit): {kraken_upbit_arbitrage:.2f}%")
                if kraken_novadax_arbitrage and abs(kraken_novadax_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Novadax): {kraken_novadax_arbitrage:.2f}%")
                if kraken_bitflyer_arbitrage and abs(kraken_bitflyer_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-BitFlyer): {kraken_bitflyer_arbitrage:.2f}%")
                if kraken_binanceus_arbitrage and abs(kraken_binanceus_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Binance.US): {kraken_binanceus_arbitrage:.2f}%")
                if kraken_btcturk_arbitrage and abs(kraken_btcturk_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-BTCTurk): {kraken_btcturk_arbitrage:.2f}%")
                if kraken_bitso_arbitrage and abs(kraken_bitso_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Bitso): {kraken_bitso_arbitrage:.2f}%")
                if kraken_coinsph_arbitrage and abs(kraken_coinsph_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Coins.ph): {kraken_coinsph_arbitrage:.2f}%")
                if kraken_bithumb_arbitrage and abs(kraken_bithumb_arbitrage) > 0.5:
                    print(f"Significant arbitrage opportunity (Kraken-Bithumb): {kraken_bithumb_arbitrage:.2f}%")
                return True
            except Exception as e:
                print(f"Error storing data (attempt {attempt + 1}): {e}")
                session.rollback()
                if attempt == max_retries - 1:
                    print("Failed to store data after all retries")
                time.sleep(5)
            finally:
                session.close()
        return False

    def run(self):
        print("Starting real-time data fetcher...")
        consecutive_failures = 0
        while True:
            try:
                current_time = datetime.utcnow()
                # Fetch all required data
                kraken_data = self.fetch_kraken_data()
                luno_data = self.fetch_luno_data()
                usd_zar_rate = self.fetch_usd_zar_rate()
                bitbns_data = self.fetch_bitbns_data()
                upbit_data = self.fetch_upbit_data()
                novadax_data = self.fetch_novadax_data()
                bitflyer_data = self.fetch_bitflyer_data()
                binanceus_data = self.fetch_binanceus_data()
                btcturk_data = self.fetch_btcturk_data()
                bitso_data = self.fetch_bitso_data()
                coinsph_data = self.fetch_coinsph_data()
                bithumb_data = self.fetch_bithumb_data()
                usd_inr_rate = self.fetch_usd_inr_rate()
                usd_krw_rate = self.fetch_usd_krw_rate()
                usd_brl_rate = self.fetch_usd_brl_rate()
                usd_jpy_rate = self.fetch_usd_jpy_rate()
                usd_try_rate = self.fetch_usd_try_rate()
                usd_mxn_rate = self.fetch_usd_mxn_rate()
                usd_php_rate = self.fetch_usd_php_rate()
                if all([kraken_data, luno_data, usd_zar_rate, bitbns_data, upbit_data, novadax_data, bitflyer_data, binanceus_data, btcturk_data, bitso_data, coinsph_data, bithumb_data, usd_inr_rate, usd_krw_rate, usd_brl_rate, usd_jpy_rate, usd_try_rate, usd_mxn_rate, usd_php_rate]):
                    if self.store_data(kraken_data, luno_data, usd_zar_rate, bitbns_data, upbit_data, novadax_data, bitflyer_data, binanceus_data, btcturk_data, bitso_data, coinsph_data, bithumb_data, usd_inr_rate, usd_krw_rate, usd_brl_rate, usd_jpy_rate, usd_try_rate, usd_mxn_rate, usd_php_rate):
                        print(f"Data stored successfully at {current_time}")
                        consecutive_failures = 0  # Reset failure counter
                    else:
                        consecutive_failures += 1
                else:
                    consecutive_failures += 1
                    print("Failed to fetch some data")
                if consecutive_failures >= 5:
                    print("Too many consecutive failures, reinitializing connections...")
                    self.initialize_exchanges(kraken_api_key, kraken_api_secret, 
                                           luno_api_key, luno_api_secret)
                    consecutive_failures = 0
                sleep_time = 60 - datetime.utcnow().second
                time.sleep(sleep_time)
            except Exception as e:
                print(f"Unexpected error in main loop: {e}")
                print(traceback.format_exc())
                consecutive_failures += 1
                time.sleep(5)

if __name__ == "__main__":
    while True:
        try:
            fetcher = RealTimeFetcher()
            fetcher.run()
        except Exception as e:
            print(f"Critical error: {e}")
            print(traceback.format_exc())
            print("Restarting the entire script in 10 seconds...")
            time.sleep(10) 
