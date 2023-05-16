# Start with Alpine Linux image
FROM alpine:latest
LABEL maintainer="mnoojin@madisoncollege.edu"

# Install Python, postgres
RUN apk add --update dcron python3 py3-pip

# Install requirements.txt
COPY ./requirements.txt /requirements.txt
RUN pip install --no-cache-dir --upgrade -r /requirements.txt

# Set cronjobs: aws_db, realtime_db, make_gifs
COPY ./api/cron /var/spool/cron/crontabs/root
RUN crond

# Copy code to container
COPY ./api /api

# Set permissions
RUN chmod +x /api/db/init.py /api/db/aws_db.py /api/db/realtime_db.py /api/make_gifs.py /api/startup.sh

# Run startup script
CMD ["/api/startup.sh"]

# Labels
LABEL org.label-schema.schema-version="1.0"
LABEL org.label-schema.name="amrdc/amrdc_api"
LABEL org.label-schema.description="AMRDC Web API for Data Warehouse & visualization tools"
LABEL org.label-schema.url="https://amrdcdata.ssec.wisc.edu/"