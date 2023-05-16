#!/bin/sh
crond
cd /api
python make_gifs.py
python db/init.py
uvicorn api:app --proxy-headers --host=0.0.0.0 --port=8000
