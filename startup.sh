#!/bin/bash

if [ ! -f /var/lib/neo4j/data/neostore.db ]; then
  neo4j-admin database load neo4j --from-path=/var/lib/neo4j/import
fi

neo4j console