from symphony.data_classes import PriceHistory
from symphony.enum import Column


class IndicatorKit:
    """
    IndicatorKit:

        Contains some helpful methods
    """

    @staticmethod
    def get_true_high(price_history: PriceHistory, index: int) -> float:
        """
        Get the true high of the specified index. The true high is the greater
            value of the high at `index` or the previous close

        :param price_history: (`data_classes.PriceHistory`) Price history
        :param index: (`int`) The index to get
        :return: (`float`) True high
        """
        return max(
            price_history.price_history[Column.HIGH].iloc[index],
            price_history.price_history[Column.CLOSE].iloc[index - 1]
        )

    @staticmethod
    def get_true_low(price_history: PriceHistory, index: int) -> float:
        """
        Get the true low of the specified index. The true low is the lesser
            value of the low at `index` or the previous close

        :param price_history: (`data_classes.PriceHistory`) Price history
        :param index: (`int`) The index to get
        :return: (`float`) True low
        """
        return min(
            price_history.price_history[Column.LOW].iloc[index],
            price_history.price_history[Column.CLOSE].iloc[index - 1]
        )

    @staticmethod
    def get_true_range_of_interval(price_history: PriceHistory, start_index: int, end_index: int) -> float:
        """
        Calculates the true range of an interval

        :param price_history: (`PriceHistory`) Standard price history
        :param start_index: (`int`) Start index
        :param end_index: (`int`) End index
        :return: (`float`) True range
        """

        true_high = max(
            [
                IndicatorKit.get_true_high(price_history, i) for i in range(start_index, end_index + 1)
            ]
        )
        true_low = min(
            [
                IndicatorKit.get_true_low(price_history, i) for i in range(start_index, end_index + 1)
            ]
        )
        return true_high - true_low

    @staticmethod
    def get_true_range(price_history: PriceHistory, index: int) -> float:
        """
        Get true range of a specific bar. Simply the difference between the true high and true low

        :param price_history: (`PriceHistory`) Standard price history
        :param index: (`int`) Bar index
        :return: (`float`) True range
        """
        true_high = IndicatorKit.get_true_high(price_history, index)
        true_low = IndicatorKit.get_true_low(price_history, index)
        return abs(true_high - true_low)

