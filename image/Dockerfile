FROM apache/airflow:2.5.1-python3.8

RUN pip install --user google-cloud-bigquery-reservation==1.8.0

ENTRYPOINT ["/usr/bin/dumb-init", "--", "/entrypoint"]
CMD ["--help"]
