import numpy as np
from symphony.indicators.demarkcountdown.td_price_flip import bullish_price_flip, bearish_price_flip
from symphony.indicators.demarkcountdown.td_setup import td_buy_setup, td_sell_setup, td_setup, buy_setup_keys, \
    sell_setup_keys
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.schema.schema_utils import schema


@schema(filename='indicators/demarkcountdown/td_buy_countdown_schema.json')
def td_buy_countdown(flow: dict, setups: dict = {}, cancellation_qualifier_I: bool = False,
                     cancellation_qualifier_II: bool = False, **kwargs) -> dict:
    """
    Calculates the buy countdown and returns all buy countdown
        components as indicator object (price flips, setups, perfect setups, TDST)

    Args:
        flow (dict): Flow object
        setups (dict, optional): Optionally supply the setups object returned by td_setup
        cancellation_qualifier_I (bool, optional): CQI. Defaults to false
        cancellation_qualifier_II (bool, optional): CQII. Defaults to false
        **kwargs (dict, optional): Any settings to pass to setup or price flip
    
    Returns:
        (dict): Indicator object with all buy channels
    """

    bearish_price_flips = bearish_price_flip(flow, **kwargs)
    bullish_price_flips = bullish_price_flip(flow, **kwargs)
    if not setups:
        setups = td_setup(flow, bullish_price_flips=bullish_price_flips["bullish_price_flips"],
                          bearish_price_flips=bearish_price_flips["bearish_price_flips"], **kwargs)

    opens = IndicatorKit.get_points(flow, "open")
    highs = IndicatorKit.get_points(flow, "high")
    lows = IndicatorKit.get_points(flow, "low")
    closes = IndicatorKit.get_points(flow, "close")

    np_buy_setups = np.array(setups["buy_setups"])
    np_sell_setups = np.array(setups["sell_setups"])
    buy_setup_indices = np.where(np_buy_setups == 1)[0]
    sell_setup_indices = np.where(np_sell_setups == 1)[0]

    # Upper bound for the count is either the next sell setup or the most recent bar
    upper_bounds = [
        len(flow["price_history"]) - 1 if not np.where(sell_setup_indices > buy_setup_index)[0].size
        else sell_setup_indices[np.where(sell_setup_indices > buy_setup_index)[0][0]]
        for buy_setup_index in buy_setup_indices
    ]

    # Address cancellation qualifiers

    if cancellation_qualifier_I | cancellation_qualifier_II:
        active_buy_setups = np.zeros(len(flow["price_history"]), dtype=int)
    else:
        active_buy_setups = np.copy(np_buy_setups)

    if cancellation_qualifier_I:
        """
        TD Buy Countdown Cancellation Qualifier I
        If
            The size of the true range of the most recently completed TD Buy Setup is equal to,
            or greater than, the size of the previous TD Buy Setup, but less than 1.618 times its size, 
        Then
            A TD Setup recycle will occur; that is, whichever TD Buy Setup has the larger true
            range will become the active TD Buy Setup. 
        """
        for i, buy_setup_index in enumerate(buy_setup_indices[1:]):
            true_range_curr = IndicatorKit.get_true_range(flow, buy_setup_index - 8,
                                                          setups["buy_setups_true_end"][buy_setup_index])
            true_range_prev = IndicatorKit.get_true_range(flow, buy_setup_indices[i] - 8,
                                                          setups["buy_setups_true_end"][buy_setup_indices[i]])
            if true_range_curr >= true_range_prev and true_range_curr <= 1.618 * true_range_prev:
                active_buy_setups[buy_setup_indices[i]] = 1
            else:
                active_buy_setups[buy_setup_index] = 1
    if cancellation_qualifier_II:
        """
        	TD	Buy	Countdown Cancellation Qualifier II (a TD Buy Setup Within a TD	Buy	Setup)
            If
                The market has completed a TD Buy Setup that has a closing range within the true
                range of the prior TD Buy Setup, without recording a TD Sell Setup between the two,
            And if
                The current TD Buy Setup has a price extreme within the true range of the prior TD Buy Setup,
            Then
                The prior TD Buy Setup is the active TD Setup, and the TD Buy Countdown relating to it remains intact.
        """
        for i, buy_setup_index in enumerate(buy_setup_indices[1:]):
            true_high_prev = max([IndicatorKit.get_true_high(flow, i) for i in range(buy_setup_indices[i] - 8,
                                                                                     setups["buy_setups_true_end"][
                                                                                         buy_setup_indices[i]] + 1)])
            true_low_prev = min([IndicatorKit.get_true_low(flow, i) for i in range(buy_setup_indices[i] - 8,
                                                                                   setups["buy_setups_true_end"][
                                                                                       buy_setup_indices[i]] + 1)])
            closes_curr = closes[buy_setup_index - 8:setups["buy_setups_true_end"][buy_setup_index] + 1]
            lows_curr = lows[buy_setup_index - 8:setups["buy_setups_true_end"][buy_setup_index] + 1]
            highs_curr = highs[buy_setup_index - 8:setups["buy_setups_true_end"][buy_setup_index] + 1]

            highest_close_curr = max(closes_curr)
            lowest_close_curr = min(closes_curr)
            highest_high_curr = max(highs_curr)
            lowest_low_curr = min(lows_curr)

            if not [sell_setup_index for sell_setup_index in sell_setup_indices if
                    sell_setup_index > buy_setup_indices[i] and sell_setup_index < buy_setup_index] \
                    and \
                    (highest_close_curr < true_high_prev and lowest_close_curr > true_low_prev) and (
                    highest_high_curr < true_high_prev and lowest_low_curr > true_low_prev):
                active_buy_setups[buy_setup_indices[i]] = 1
            else:
                active_buy_setups[buy_setup_index] = 1

    # If cancellation qualifiers are enabled, use the 'active' array
    if cancellation_qualifier_I | cancellation_qualifier_II:
        buy_setup_indices = np.where(active_buy_setups == 1)[0]

    buy_countdowns = np.zeros(len(flow["price_history"]), dtype=int)
    for buy_setup_index, upper_bound in zip(buy_setup_indices, upper_bounds):
        count = 0
        bar8_close = 0.0
        for i in range(buy_setup_index, upper_bound):
            # Cancel buy countdown if there is a true low over TDST resistance
            if IndicatorKit.get_true_low(flow, i) > IndicatorKit.pad_to_length(setups["tdst_resistance"], len(closes))[i]:
                break
            if closes[i] <= lows[i - 2]:
                if count < 12:
                    count += 1
                elif count == 12 and lows[i] < bar8_close:
                    buy_countdowns[i] = 1
                    break

            if not bar8_close and count == 8:
                bar8_close = closes[i]

    # Remove keys relating to sell setup
    for key in sell_setup_keys:
        setups.pop(key)

    return {
        **bearish_price_flips,
        **setups,
        "active_buy_setups": active_buy_setups.tolist(),
        "buy_countdowns": buy_countdowns.tolist()
    }


@schema(filename='indicators/demarkcountdown/td_sell_countdown_schema.json')
def td_sell_countdown(flow: dict, setups: dict = {}, cancellation_qualifier_I: bool = False,
                      cancellation_qualifier_II: bool = False, **kwargs) -> dict:
    """
    Calculates the sell countdown and returns all sell countdown
        components as indicator object (price flips, setups, perfect setups, TDST)

    Args:
        flow (dict): Flow object
        cancellation_qualifier_I (bool, optional): CQI. Defaults to false
        cancellation_qualifier_II (bool, optional): CQII. Defaults to false
        **kwargs (dict): Any settings to pass to setup or price flip
    
    Returns:
        (dict): Indicator object with all sell channels
    """

    bearish_price_flips = bearish_price_flip(flow, **kwargs)
    bullish_price_flips = bullish_price_flip(flow, **kwargs)
    if not setups:
        setups = td_setup(flow, bullish_price_flips=bullish_price_flips["bullish_price_flips"],
                          bearish_price_flips=bearish_price_flips["bearish_price_flips"], **kwargs)

    opens = IndicatorKit.get_points(flow, "open")
    highs = IndicatorKit.get_points(flow, "high")
    lows = IndicatorKit.get_points(flow, "low")
    closes = IndicatorKit.get_points(flow, "close")

    np_buy_setups = np.array(setups["buy_setups"])
    np_sell_setups = np.array(setups["sell_setups"])
    buy_setup_indices = np.where(np_buy_setups == 1)[0]
    sell_setup_indices = np.where(np_sell_setups == 1)[0]

    # Upper bound for the count is either the start of the next buy setup or the most recent bar
    upper_bounds = [
        len(flow["price_history"]) - 1 if not np.where(buy_setup_indices > sell_setup_index)[0].size
        else buy_setup_indices[np.where(buy_setup_indices > sell_setup_index)[0][0]]
        for sell_setup_index in buy_setup_indices
    ]

    # Address cancellation qualifiers

    if cancellation_qualifier_I | cancellation_qualifier_II:
        active_sell_setups = np.zeros(len(flow["price_history"]), dtype=int)
    else:
        active_sell_setups = np.copy(np_sell_setups)

    if cancellation_qualifier_I:
        """
        TD Buy Countdown Cancellation Qualifier I
        If
            The size of the true range of the most recently completed TD Buy Setup is equal to,
            or greater than, the size of the previous TD Buy Setup, but less than 1.618 times its size, 
        Then
            A TD Setup recycle will occur; that is, whichever TD Buy Setup has the larger true
            range will become the active TD Buy Setup. 
        """
        for i, sell_setup_index in enumerate(sell_setup_indices[1:]):
            true_range_curr = IndicatorKit.get_true_range(flow, sell_setup_index - 8,
                                                          setups["sell_setups_true_end"][sell_setup_index])
            true_range_prev = IndicatorKit.get_true_range(flow, sell_setup_indices[i] - 8,
                                                          setups["sell_setups_true_end"][sell_setup_indices[i]])
            if true_range_curr >= true_range_prev and true_range_curr <= 1.618 * true_range_prev:
                active_sell_setups[sell_setup_indices[i]] = 1
            else:
                active_sell_setups[sell_setup_index] = 1
    if cancellation_qualifier_II:
        """
        	TD	Buy	Countdown Cancellation Qualifier II (a TD Buy Setup Within a TD	Buy	Setup)
            If
                The market has completed a TD Buy Setup that has a closing range within the true
                range of the prior TD Buy Setup, without recording a TD Sell Setup between the two,
            And if
                The current TD Buy Setup has a price extreme within the true range of the prior TD Buy Setup,
            Then
                The prior TD Buy Setup is the active TD Setup, and the TD Buy Countdown relating to it remains intact.
        """
        for i, sell_setup_index in enumerate(sell_setup_indices[1:]):
            true_high_prev = max([IndicatorKit.get_true_high(flow, i) for i in range(sell_setup_indices[i] - 8,
                                                                                     setups["sell_setups_true_end"][
                                                                                         sell_setup_indices[i]] + 1)])
            true_low_prev = min([IndicatorKit.get_true_low(flow, i) for i in range(sell_setup_indices[i] - 8,
                                                                                   setups["sell_setups_true_end"][
                                                                                       sell_setup_indices[i]] + 1)])
            closes_curr = closes[sell_setup_index - 8:setups["sell_setups_true_end"][sell_setup_index] + 1]
            lows_curr = lows[sell_setup_index - 8:setups["sell_setups_true_end"][sell_setup_index] + 1]
            highs_curr = highs[sell_setup_index - 8:setups["sell_setups_true_end"][sell_setup_index] + 1]

            highest_close_curr = max(closes_curr)
            lowest_close_curr = min(closes_curr)
            highest_high_curr = max(highs_curr)
            lowest_low_curr = min(lows_curr)

            if not [buy_setup_index for buy_setup_index in buy_setup_indices if
                    buy_setup_index > sell_setup_indices[i] and buy_setup_index < sell_setup_index] \
                    and \
                    (highest_close_curr < true_high_prev and lowest_close_curr > true_low_prev) and (
                    highest_high_curr < true_high_prev and lowest_low_curr > true_low_prev):
                active_sell_setups[sell_setup_indices[i]] = 1
            else:
                active_sell_setups[sell_setup_index] = 1

    # If cancellation qualifiers are enabled, use the 'active' array
    if cancellation_qualifier_I | cancellation_qualifier_II:
        sell_setup_indices = np.where(active_sell_setups == 1)[0]

    sell_countdowns = np.zeros(len(flow["price_history"]), dtype=int)
    for sell_setup_index, upper_bound in zip(sell_setup_indices, upper_bounds):
        count = 1 if closes[sell_setup_index] > highs[sell_setup_index - 2] else 0
        bar8_close = 0.0
        for i in range(sell_setup_index + 1, upper_bound):
            # Cancel buy countdown if there is a true low over TDST resistance
            if IndicatorKit.get_true_high(flow, i) < IndicatorKit.pad_to_length(setups["tdst_support"], len(closes))[i]:
                break

            if closes[i] >= highs[i - 2]:
                if count < 12:
                    count += 1
                elif count == 12 and highs[i] > bar8_close:
                    sell_countdowns[i] = 1
                    break

            if not bar8_close and count == 8:
                bar8_close = closes[i]

    for key in buy_setup_keys:
        setups.pop(key)

    return {
        **bullish_price_flips,
        **setups,
        "active_sell_setups": active_sell_setups.tolist(),
        "sell_countdowns": sell_countdowns.tolist()
    }


@schema(filename='indicators/demarkcountdown/td_countdown_schema.json')
def td_countdown(flow: dict, cancellation_qualifier_I: bool = False, cancellation_qualifier_II: bool = False,
                 **kwargs) -> dict:
    """
    Calculates both the buy and sell countdowns and returns as a unified
        indicator object

    Args:
        flow (dict): Flow object
        **kwargs (dict): Any settings to pass to setup or price flip
    
    Returns:
        (dict): Indicator object with all channels
    """

    bearish_price_flips = bearish_price_flip(flow, **kwargs)
    bullish_price_flips = bullish_price_flip(flow, **kwargs)

    setups = td_setup(flow, bullish_price_flips=bullish_price_flips["bullish_price_flips"],
                      bearish_price_flips=bearish_price_flips["bearish_price_flips"])

    return {
        **td_buy_countdown(flow, setups=setups.copy(), cancellation_qualifier_I=cancellation_qualifier_I,
                           cancellation_qualifier_II=cancellation_qualifier_II),
        **td_sell_countdown(flow, setups=setups.copy(), cancellation_qualifier_I=cancellation_qualifier_I,
                            cancellation_qualifier_II=cancellation_qualifier_II)
    }
