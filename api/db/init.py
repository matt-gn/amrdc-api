from datetime import datetime
from aws_db import init_aws_table, rebuild_aws_table
from realtime_db import rebuild_realtime_table
import test

if __name__ == "__main__":
    db_initialized = test.verify_db()
    if not db_initialized:
        print(f"{datetime.now()}\tStarting AWS database initialization")
        init_aws_table()
        print(f"{datetime.now()}\tDone")
    else:
        print(f"{datetime.now()}\tStarting AWS database update")
        rebuild_aws_table()
        print(f"{datetime.now()}\tDone")
    print(f"{datetime.now()}\tStarting Realtime database build")
    rebuild_realtime_table()
    print(f"{datetime.now()}\tDone")
    test.test_db()
    print(f"{datetime.now()}\tStarting application")
