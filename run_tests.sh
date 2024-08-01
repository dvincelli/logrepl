#!/bin/bash

docker-compose -f docker-compose.yml up --build --abort-on-container-exit -d

docker exec -i logrepl-logrepl-1 pdm run python -m logrepl verify
#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl dump
#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl load

docker exec -i logrepl-logrepl-1 pdm run python -m logrepl setup all
docker exec -i logrepl-logrepl-1 pdm run python -m logrepl teardown all

#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl teardown subscriber
#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl teardown provider
#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl teardown replication_set
#docker exec -i logrepl-logrepl-1 pdm run python -m logrepl teardown subscription
