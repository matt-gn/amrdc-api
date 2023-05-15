from unittest import TestResult, TestLoader
from datetime import datetime
from test import TestAPI
from aws_db import init_aws_table
from realtime_db import init_realtime_table, init_aggregate_table

if __name__ == "__main__":
    result = TestResult()
    suite = TestLoader().loadTestsFromTestCase(TestAPI)
    suite.run(result)
    if not result.wasSuccessful():
        print(f"{datetime.now()}\tStarting database initialization")
        init_aws_table()
        update_realtime_table()
        print(f"{datetime.now()}\tDone")
    else:
        print(f"{datetime.now()}\tAll database tests passed. Starting application")