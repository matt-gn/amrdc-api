#!/bin/sh
sleep 20
cd /api
python db/init.py
python make_gifs.py
uvicorn api:app --proxy-headers --host=0.0.0.0 --port=8000
