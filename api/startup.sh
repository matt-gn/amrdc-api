#!/bin/sh
sleep 10
cd /api
python make_gifs.py
python db/init.py
touch output.log
crond
uvicorn api:app --proxy-headers --host=0.0.0.0 --port=8000
