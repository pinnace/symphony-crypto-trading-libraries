from typing import List
from symphony.schema.schema_kit import SchemaKit
from symphony.schema.schema_utils import SchemaUtils
from symphony.indicators.indicator_registry import IndicatorRegistry
from symphony.indicators.indicator_kit import IndicatorKit
from symphony.indicators.indicator_exception import IndicatorError
from symphony.indicators.indicators import Indicators


from pprint import pprint
class IndicatorFlow():
    """Takes a standard price history object and set of indicators
        (as enums from Indicators) and applies them to an internal flow object

    Args:
        price_history (dict): Standard price history object
        indicators (list): List of indicators

    """

    def __init__(self, price_history: dict, indicators: list, indicator_settings: List[dict]):

        # Some sanity checks
        if IndicatorRegistry.PRICE_HISTORY in indicators:
            raise IndicatorError(__name__ + ": Cannot include PRICE_HISTORY in indicator list")

        if not all(indicator in list(IndicatorRegistry) for indicator in indicators):
            raise IndicatorError(__name__ + ": One of the indicators not found in registry: {}".format(*list(IndicatorRegistry)))

        SchemaUtils.validate_price_history(price_history)
        self._flow = SchemaKit.standard_flow()

        self._flow["price_history"] = IndicatorKit.price_history_to_arr(price_history)

        assert(len(indicators) == len(indicator_settings))

        
        if indicators:
            for indicator, settings in zip(indicators, indicator_settings):
                indicator_obj = SchemaKit.standard_indicator_for_flow()

                indicator_obj["name"] = indicator.name
                indicator_obj["settings"] = settings
                indicator_obj["data"] = getattr(Indicators, indicator.name.lower())(self._flow, **settings)
                
                self._flow["indicators"].append(
                    indicator_obj
                    )





    

    