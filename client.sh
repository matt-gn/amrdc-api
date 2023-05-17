#!/bin/sh

psql -U ${POSTGRES_USER} <<-END
    CREATE USER ${CLIENT_USER} WITH PASSWORD ${CLIENT_PASSWORD};
    GRANT CONNECT ON DATABASE amrdc_api TO ${CLIENT_USER};
    GRANT SELECT ON aws_10min TO ${CLIENT_USER};
    GRANT SELECT ON aws_realtime TO ${CLIENT_USER};
END
