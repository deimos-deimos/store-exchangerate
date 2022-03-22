# store-exchangerate
Built as a test assignment

## Assignment requirements

With [exchangerate.host](https://exchangerate.host/) API prepare ETL using Airflow and python for loading BTC/USD currency pair.
It should run every 3 hours and  put data in DB (pair, date, current exchange rate).

As an advanced task provide a way to fill database with historical data.

### Clarifications

Write code in Python.

Use any version of Airflow as a scheduler.

Use any base of your choice: MongoDB, Postgres, Clickhouse.

Wrap everything in containers and run with docker-compose.

For running Airflow in docker-compose you may use: [https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html](https://airflow.apache.org/docs/apache-airflow/stable/start/docker.html) 

### Result format

Upload code to github.
 
Add startup and validation instructions and a short description of what was done.

## Requirements
Docker and Docker-compose

## Startup instructions
1. Clone this repository
2. Run `sudo sh setup-and-run.sh`
3. Visit localhost:8080 and look into exchangerate DAG
4. After successful DAG run, checkout Postgres database with uri `postgres://exchangerate:exchangerate@localhost:5432/exchangerate`. The required data is in `model.prices_rate` table 

### note
If 5432 port for Postgres is not suitable for you, change it in two files:
1. Docker-compose file
2. setup-and-run.sh 7 and 9 lines
## Additional features
* Loads historical data
* Loads any number of currency pairs form Exchangerate. Just change variable `pairs` in Airflow and run DAG.
## Design comment
I initially wanted to try out two unfamiliar Airflow features in this assignment: Python venv operator and XCom. 
It appears they do not work nicely together, so I decided to omit XCom to meet the deadline.