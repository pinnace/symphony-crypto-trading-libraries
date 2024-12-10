import unittest
import sys
import logging
from symphony.enum import Exchange, Timeframe
from symphony.utils.time import round_to_timeframe
from symphony.utils import chunk_times
from symphony.config import USE_MODIN

if USE_MODIN:
    import modin.pandas as pd
else:
    import pandas as pd


class UtilsTimeTest(unittest.TestCase):

    def test_round_to_timeframe(self):
        test_1m = pd.Timestamp("2021-01-01 00:00:01")
        res_1m = round_to_timeframe(test_1m, Timeframe.M1)
        self.assertEquals(res_1m, pd.Timestamp("2021-01-01 00:00:00"))

        test_30m = pd.Timestamp("2021-01-01 13:14:11")
        res_30m = round_to_timeframe(test_30m, Timeframe.M30)
        self.assertEquals(res_30m, pd.Timestamp("2021-01-01 13:00:00"))

        test_1hr = pd.Timestamp("2021-01-01 11:42:34")
        res_1hr = round_to_timeframe(test_1hr, Timeframe.H1)
        self.assertEquals(res_1hr, pd.Timestamp("2021-01-01 11:00:00"))

        test_4hr = pd.Timestamp("2021-01-01 01:00:00")
        res_4hr = round_to_timeframe(test_4hr, Timeframe.H4)
        self.assertEquals(res_4hr, pd.Timestamp("2021-01-01 00:00:00"))

        test_4hr = pd.Timestamp("2021-01-01 02:00:00")
        res_4hr = round_to_timeframe(test_4hr, Timeframe.H4)
        self.assertEquals(res_4hr, pd.Timestamp("2021-01-01 00:00:00"))

        test_4hr = pd.Timestamp("2021-01-01 06:00:00")
        res_4hr = round_to_timeframe(test_4hr, Timeframe.H4)
        self.assertEquals(res_4hr, pd.Timestamp("2021-01-01 04:00:00"))

        test_4hr = pd.Timestamp("2021-01-01 23:00:00")
        res_4hr = round_to_timeframe(test_4hr, Timeframe.H4)
        self.assertEquals(res_4hr, pd.Timestamp("2021-01-01 20:00:00"))

        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

    def test_chunk_times(self):
        start_time = pd.Timestamp("2017-08-01 01:00:00")
        end_time = pd.Timestamp("2017-08-01 01:17:00")

        chunks = chunk_times(start_time, Timeframe.M1, 3, end=end_time, end_inclusive=True)
        expected_chunks_inclusive = [
            (pd.Timestamp('2017-08-01 01:00:00', tz='utc'), pd.Timestamp('2017-08-01 01:03:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:04:00', tz='utc'), pd.Timestamp('2017-08-01 01:07:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:08:00', tz='utc'), pd.Timestamp('2017-08-01 01:11:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:12:00', tz='utc'), pd.Timestamp('2017-08-01 01:15:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:16:00', tz='utc'), pd.Timestamp('2017-08-01 01:17:00', tz='utc'))
        ]
        self.assertEquals(chunks, expected_chunks_inclusive)
        chunks = chunk_times(start_time, Timeframe.M1, 3, end=end_time, end_inclusive=False)

        expected_chunks_exclusive = [
            (pd.Timestamp('2017-08-01 01:00:00', tz='utc'), pd.Timestamp('2017-08-01 01:04:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:04:00', tz='utc'), pd.Timestamp('2017-08-01 01:08:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:08:00', tz='utc'), pd.Timestamp('2017-08-01 01:12:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:12:00', tz='utc'), pd.Timestamp('2017-08-01 01:16:00', tz='utc')),
            (pd.Timestamp('2017-08-01 01:16:00', tz='utc'), pd.Timestamp('2017-08-01 01:18:00', tz='utc'))
        ]
        self.assertEquals(chunks, expected_chunks_exclusive)



        print(__name__ + "." + sys._getframe().f_code.co_name + ": Unit test passed")

if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout)
    logging.getLogger("UtilsTimeTest.test_round_to_timeframe").setLevel(logging.DEBUG)
    logging.getLogger("UtilsTimeTest.test_chunk_times").setLevel(logging.DEBUG)
    unittest.main()
