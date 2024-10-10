<h2>docker-compose для поднятия базы данных Neo4j</h2>

нужно положить neo4j.dump файл с базой данных в папку import

структура каталога:

- neo4j
    - docker-compose.yaml
    - .env
    - data
    - import
        - neo4j.dump
    - logs
