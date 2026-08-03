from typing import Any

import numpy as np
from numba import from_dtype, float64, int64
from numba.experimental import jitclass

from .types import state_values_dtype


class StateValues:
    arr: from_dtype(state_values_dtype)[:]

    def __init__(self, arr: np.ndarray[Any, state_values_dtype]):
        self.arr = arr

    @property
    def position(self) -> float64:
        """
        Returns the open position.
        """
        return self.arr[0].position

    @property
    def balance(self) -> float64:
        """
        Returns the cash balance.

        Backtest only; in live this is always 0 (no live path computes it).
        """
        return self.arr[0].balance

    @property
    def fee(self) -> float64:
        """
        Returns the accumulated fee.

        Backtest only; in live this is always 0 (no live path computes it).
        """
        return self.arr[0].fee

    @property
    def num_trades(self) -> int64:
        """
        Returns the total number of trades.

        Backtest only; in live this is always 0 (no live path computes it).
        """
        return self.arr[0].num_trades

    @property
    def trading_volume(self) -> float64:
        """
        Returns the total trading volume.

        Backtest only; in live this is always 0 (no live path computes it).
        """
        return self.arr[0].trading_volume

    @property
    def trading_value(self) -> float64:
        """
        Returns the total trading value.

        Backtest only; in live this is always 0 (no live path computes it).
        """
        return self.arr[0].trading_value


StateValues_ = jitclass(StateValues)
