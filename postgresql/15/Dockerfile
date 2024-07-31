FROM postgres:15

RUN apt-get update \
	&& apt-get install -y --no-install-recommends postgresql-15-pglogical \
	&& rm -rf /var/lib/apt/lists/*  /usr/share/doc


