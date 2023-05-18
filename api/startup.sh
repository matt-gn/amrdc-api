#!/bin/sh

sleep 10
cd /api
python make_gifs.py
python db/init.py
touch output.log
crond
gunicorn api:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8000
