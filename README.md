<h2>docker-compose для поднятия базы данных Neo4j и Apache Airflow</h2>

нужно положить neo4j.dump файл с базой данных в папку import

структура каталога:

- dwh_project
    - docker-compose.yaml
    - .env
    - data (пустая папка)
    - import
        - neo4j.dump
    - logs (пустая папка)
    - dags
    - airflow-logs (пустая папка для логов airflow)

запуск: docker-compose up -d

логи neo4j можно посмотреть командой: docker-compose logs neo4j

<p>Apache Airflow UI будет доступен по адресу: http://localhost:8080/ (логин и пароль admin)</p>
<p>Neo4j Browser будет доступен по адресу: http://localhost:7474/browser/</p>
<p>попросит авторизоваться, нужно ввести логин: neo4j и пароль: neo4j</p>
<p>после этого попросит изменить пароль</p>
