from common.types import Venue


def get_trader_functions(venue: Venue) -> dict[str, callable]:
    """
    Return a dict of the four trader-related callables for the given venue.

    Example:
        funcs = get_trader_functions(Venue.BINANCE)
        funcs["trader"](...)
        funcs["update_order_status"](...)
    """
    if venue == Venue.BINANCE:
        from outputs.trader_binance import (
            trader_binance,
            update_account_balance as update_account_balance_binance,
            update_order_status as update_order_status_binance,
            update_trade_status as update_trade_status_binance,
        )
        return {
            "trader": trader_binance,
            "update_account_balance": update_account_balance_binance,
            "update_order_status": update_order_status_binance,
            "update_trade_status": update_trade_status_binance,
        }
    elif venue == Venue.MT5:
        from outputs.trader_mt5 import (
            trader_mt5,
            update_account_balance as update_account_balance_mt5,
            update_order_status as update_order_status_mt5,
            update_trade_status as update_trade_status_mt5,
        )

        return {
            "trader": trader_mt5,
            "update_account_balance": update_account_balance_mt5,
            "update_order_status": update_order_status_mt5,
            "update_trade_status": update_trade_status_mt5,
        }
    elif venue == Venue.MOEX:
        async def _no_op_async(*args, **kwargs):
            return None

        def _no_op(*args, **kwargs):
            return None

        return {
            "trader": _no_op_async,
            "update_account_balance": _no_op,
            "update_order_status": _no_op,
            "update_trade_status": _no_op_async,
        }
    else:
        raise ValueError(f"Unknown trader venue: {venue!r}")
