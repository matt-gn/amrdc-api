from unittest import TestResult, TestLoader
from datetime import datetime
from test import TestAPI
from aws_db import init_aws_table
from realtime_db import update_realtime_table

if __name__ == "__main__":
    result = TestResult()
    suite = TestLoader().loadTestsFromTestCase(TestAPI)
    suite.run(result)
    if not result.wasSuccessful():
        print(f"{datetime.now()}\tStarting AWS database initialization")
        init_aws_table()
        print(f"{datetime.now()}\tDone")
        print(f"{datetime.now()}\tStarting Realtime database initialization")
        update_realtime_table()
        print(f"{datetime.now()}\tDone")
    else:
        print(f"{datetime.now()}\tAll database tests passed. Starting application")