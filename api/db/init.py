from unittest import TestResult, TestLoader
from datetime import datetime
from aws_db import init_aws_table
from realtime_db import update_realtime_table
from test import *

if __name__ == "__main__":
    result = TestResult()
    suite = TestLoader().loadTestsFromTestCase(test.TestAWS)
    suite.run(result)
    if not result.wasSuccessful():
        print(f"{datetime.now()}\tStarting AWS database initialization")
        init_aws_table()
        print(f"{datetime.now()}\tDone")
    print(f"{datetime.now()}\tStarting Realtime database initialization")
    update_realtime_table()
    print(f"{datetime.now()}\tDone")
    test.test_all()
    print(f"{datetime.now()}\tStarting application")