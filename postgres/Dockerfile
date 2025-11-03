FROM postgres:15-alpine

# Copy initialization SQL (runs automatically on first start)
COPY init.sql /docker-entrypoint-initdb.d/

# Set data directory and declare it as a volume so it can be mapped to host storage
ENV PGDATA=/var/lib/postgresql/data
VOLUME ["/var/lib/postgresql/data"]