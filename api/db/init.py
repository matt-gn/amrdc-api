from unittest import TestResult, TestLoader
from test import TestAPI
from aws_db import init_aws_table
from realtime_db import init_realtime_table, init_aggregate_table

if __name__ == "__main__":
    result = TestResult()
    suite = TestLoader().loadTestsFromTestCase(TestAPI)
    suite.run(result)
    if not result.wasSuccessful():
        init_aws_table()
        init_realtime_table()
        init_aggregate_table()
