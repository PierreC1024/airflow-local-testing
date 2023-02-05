# airflow-local-testing

Local deployment of Airflow with Docker.
Requirements:
- Docker
- Make

## Deploy Airflow Locally

```bash
make start-airflow
```

### Test a DAG
Write your DAGs under the `dags` directory. This is synchronized to the Airflow UI every 10s.

### Change Airflow Version
Edit the `AIRFLOW_VERSION` variable in the `.env` file.

### Change Airflow configurations
Edit the Environment variables in the `.env` file.


## Stop Airflow

```bash
make stop-airflow
```