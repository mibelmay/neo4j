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

запуск:
docker-compose up -d

логи можно посмотреть командой:
docker-compose logs neo4j

после поднятия контейнера Neo4j Browser будет доступен по адресу: http://localhost:7474/browser/
попросит авторизоваться, нужно ввести логин: neo4j и пароль: neo4j
после этого попросит изменить пароль