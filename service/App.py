from pathlib import Path
from typing import Union
import json
import os
from datetime import datetime, date, timedelta
import re

import pandas as pd

from common.model_store import *
from common.types import AccountBalances, MT5AccountInfo

PACKAGE_ROOT = Path(__file__).parent.parent
#PACKAGE_PARENT = '..'
#SCRIPT_DIR = os.path.dirname(os.path.realpath(os.path.join(os.getcwd(), os.path.expanduser(__file__))))
#sys.path.append(os.path.normpath(os.path.join(SCRIPT_DIR, PACKAGE_PARENT)))
#PACKAGE_ROOT = os.path.dirname(os.path.abspath(__file__))


class App:
    """Globally visible variables."""

    # System
    loop = None  # asyncio main loop
    sched = None  # Scheduler

    analyzer = None  # Store and analyze data

    #
    # State of the server (updated after each interval)
    #
    # State 0 or None or empty means ok. String and other non empty objects mean error
    error_status = 0  # Networks, connections, exceptions etc. what does not allow us to work at all
    server_status = 0  # If server allow us to trade (maintenance, down etc.)
    account_status = 0  # If account allows us to trade (funds, suspended etc.)
    trade_state_status = 0  # Something wrong with our trading logic (wrong use, inconsistent state etc. what we cannot recover)

    df = None  # Data from the latest analysis

    # Trade simulator
    transaction = None
    # Trade binance
    status = None  # BOUGHT, SOLD, BUYING, SELLING
    order = None  # Latest or current order
    order_time = None  # Order submission time

    # Account Info
    # Available assets for trade
    # Can be set by the sync/recover function or updated by the trading algorithm
    # base_quantity = "0.04108219"  # BTC owned (on account, already bought, available for trade)
    # quote_quantity = "1000.0"  # USDT owned (on account, available for trade)
    account_info: Union[AccountBalances, MT5AccountInfo] = AccountBalances()

    #
    # Trader. Status data retrieved from the server. Below are examples only.
    #
    system_status = {"status": 0, "msg": "normal"}  # 0: normal，1：system maintenance
    symbol_info = {}
    # account_info = {}

    model_store: ModelStore = None

    #
    # Constant configuration parameters
    #
    config = {
        # Venue 
        "venue": "", # filled from env
        
        # Binance
        "api_key": "", # filled from env
        "api_secret": "", # filled from env
        
        # MetaTrader5
        "mt5_account_id": "", # filled from env
        "mt5_password": "", # filled from env
        "mt5_server": "", # filled from env

        # Telegram
        "telegram_bot_token": "",  # Source address of messages # filled from env
        "telegram_chat_id": "",  # Destination address of messages # filled from env

        # Optional LLM key for pattern adjudication notifier
        "llm_api_key": "", # filled from env

        #
        # Conventions for the file and column names
        #
        "merge_file_name": "data.csv",
        "feature_file_name": "features.csv",
        "matrix_file_name": "matrix.csv",
        "predict_file_name": "predictions.csv",  # predict, predict-rolling
        "signal_file_name": "signals.csv",
        "signal_models_file_name": "signal_models",

        "model_folder": "MODELS",

        "time_column": "timestamp",

        # File locations
        "data_folder": "C:/DATA_ITB",  # Location for all source and generated data/models

        # ==============================================
        # === DOWNLOADER, MERGER and (online) READER ===

        # Symbol determines sub-folder and used in other identifiers
        "symbol": "BTCUSDT",  # BTCUSDT ETHUSDT ^gspc EURUSD

        # This parameter determines time raster (granularity) for the data
        # It is pandas frequency
        "freq": "1min",

        # This list is used for downloading and then merging data
        # "folder" is symbol name for downloading. prefix will be added column names during merge
        "data_sources": [],

        # ==========================
        # === FEATURE GENERATION ===

        # What columns to pass to which feature generator and how to prefix its derived features
        # Each executes one feature generation function applied to columns with the specified prefix
        "feature_sets": [],

        # ========================
        # === LABEL GENERATION ===

        "label_sets": [],

        # ===========================
        # === MODEL TRAIN/PREDICT ===
        #     predict off-line and on-line

        "label_horizon": 0,  # This number of tail rows will be excluded from model training
        "train_length": 0,  # train set maximum size. algorithms may decrease this length

        # List all features to be used for training/prediction by selecting them from the result of feature generation
        # The list of features can be found in the output of the feature generation (but not all must be used)
        # Currently the same feature set for all algorithms
        "train_features": [],

        # Labels to be used for training/prediction by all algorithms
        # List of available labels can be found in the output of the label generation (but not all must be used)
        "labels": [],

        # Algorithms and their configurations to be used for training/prediction
        "algorithms": [],

        # ===========================
        # ONLINE (PREDICTION) PARAMETERS
        # Minimum history length required to compute derived features
        "features_horizon": 10,

        # ===============
        # === SIGNALS ===

        "signal_sets": [],

        # =====================
        # === NOTIFICATIONS ===

        "score_notification_model": {},
        "diagram_notification_model": {},

        # ===============
        # === TRADING ===
        "trade_model": {
            "no_trades_only_data_processing": False,  # in market or out of market processing is excluded (all below parameters ignored)
            "test_order_before_submit": False,  # Send test submit to the server as part of validation
            "simulate_order_execution": False,  # Instead of real orders, simulate their execution (immediate buy/sell market orders and use high price of klines for limit orders)

            "percentage_used_for_trade": 99,  # in % to the available USDT quantity, that is, we will derive how much BTC to buy using this percentage
            "limit_price_adjustment": 0.005,  # Limit price of orders will be better than the latest close price (0 means no change, positive - better for us, negative - worse for us)
        },

        "simulate_model": {},

        # =====================
        # === BINANCE TRADER ===
        "base_asset": "",  # BTC ETH
        "quote_asset": "",

        # ==================
        # === COLLECTORS ===
        "collector": {
            "folder": "DATA",
            "flush_period": 300,  # seconds
            "depth": {
                "folder": "DEPTH",
                "symbols": ["BTCUSDT", "ETHBTC", "ETHUSDT", "IOTAUSDT", "IOTABTC", "IOTAETH"],
                "limit": 100,  # Legal values (depth): '5, 10, 20, 50, 100, 500, 1000, 5000' <100 weight=1
                "freq": "1min",  # Pandas frequency
            },
            "stream": {
                "folder": "STREAM",
                # Stream formats:
                # For kline channel: <symbol>@kline_<interval>, Event type: "e": "kline", Symbol: "s": "BNBBTC"
                # For depth channel: <symbol>@depth<levels>[@100ms], Event type: NO, Symbol: NO
                # btcusdt@ticker
                "channels": ["kline_1m", "depth20"],  # kline_1m, depth20, depth5
                "symbols": ["BTCUSDT", "ETHBTC", "ETHUSDT", "IOTAUSDT", "IOTABTC", "IOTAETH"],
                # "BTCUSDT", "ETHBTC", "ETHUSDT", "IOTAUSDT", "IOTABTC", "IOTAETH"
            }
        },
    }


def data_provider_problems_exist():
    if App.error_status != 0:
        return True
    if App.server_status != 0:
        return True
    return False


def problems_exist():
    if App.error_status != 0:
        return True
    if App.server_status != 0:
        return True
    if App.account_status != 0:
        return True
    if App.trade_state_status != 0:
        return True
    return False


def load_config(config_file):
    _load_env_file(PACKAGE_ROOT / ".env")

    if config_file:
        config_file_path = PACKAGE_ROOT / config_file
        with open(config_file_path, encoding='utf-8') as json_file:
            #conf_str = json.load(json_file)
            conf_str = json_file.read()

            # Remove everything starting with // and till the line end
            conf_str = re.sub(r"//.*$", "", conf_str, flags=re.M)

            conf_json = json.loads(conf_str)
            App.config.update(conf_json)

    _apply_env_overrides()


def _load_env_file(env_path: Path):
    if not env_path.exists():
        return

    with open(env_path, encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            key = key.strip()
            value = value.strip()

            if (value.startswith('"') and value.endswith('"')) or (value.startswith("'") and value.endswith("'")):
                value = value[1:-1]

            if key and key not in os.environ:
                os.environ[key] = value


def _apply_env_overrides():
    env_map = {
        "venue": ["ITB_VENUE", "VENUE"],
        "api_key": ["ITB_API_KEY", "BINANCE_API_KEY", "API_KEY"],
        "api_secret": ["ITB_API_SECRET", "BINANCE_API_SECRET", "API_SECRET"],
        "mt5_account_id": ["ITB_MT5_ACCOUNT_ID", "MT5_ACCOUNT_ID"],
        "mt5_password": ["ITB_MT5_PASSWORD", "MT5_PASSWORD"],
        "mt5_server": ["ITB_MT5_SERVER", "MT5_SERVER"],
        "telegram_bot_token": ["ITB_TELEGRAM_BOT_TOKEN", "TELEGRAM_BOT_TOKEN"],
        "telegram_chat_id": ["ITB_TELEGRAM_CHAT_ID", "TELEGRAM_CHAT_ID"],
        "llm_api_key": ["ITB_LLM_API_KEY", "OPENAI_API_KEY", "LLM_API_KEY"],
        "moex_db_path": ["ITB_MOEX_DB_PATH", "MOEX_DB_PATH"],
    }

    for config_key, env_keys in env_map.items():
        for env_key in env_keys:
            value = os.getenv(env_key)
            if value is None or value == "":
                continue

            # Keep current config types where possible.
            current = App.config.get(config_key)
            if isinstance(current, bool):
                App.config[config_key] = value.lower() in ["1", "true", "yes", "on"]
            elif isinstance(current, int):
                try:
                    App.config[config_key] = int(value)
                except ValueError:
                    App.config[config_key] = value
            elif isinstance(current, float):
                try:
                    App.config[config_key] = float(value)
                except ValueError:
                    App.config[config_key] = value
            else:
                App.config[config_key] = value
            break


if __name__ == "__main__":
    pass
