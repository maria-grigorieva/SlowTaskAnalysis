# -*- coding: utf-8 -*-

from __future__ import print_function

import os
import re
import subprocess
import datetime as dt
from elasticsearch import Elasticsearch, helpers
from datetime import timedelta
import csv

from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator


args = {
    'owner': 'InVEx',
    'start_date': dt.datetime(2020, 6, 1, 00, 00, 00),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

# DAG description
# The first one is to calculate and load the data
dag = DAG(
    dag_id='site_efficiency',
    default_args=args,
    schedule_interval='10 0 * * *',
    max_active_runs=4,
    tags=['panda', 'oracle', 'elasticsearch']
)

# The second one is to clear ES if needed
dag_clear = DAG(
    dag_id='site_efficiency_clear',
    default_args=args,
    schedule_interval=None,
    max_active_runs=1,
    tags=['elasticsearch']
)

# Mapping types
_mappings = {
    "text": {
        "type": "text",
        "fields": {
            "keyword": {
                "type": "keyword",
                "ignore_above": 256
            }
        }
    },
    "date": {
        "type": "date",
        "format": "yyyy-MM-dd"
    },
    "geo": {"type": "geo_point"},
    "integer": {"type": "integer"},
    "float": {"type": "float"},
    "long": {"type": "long"}
}

mapping = {
    "mappings": {
        "properties": {
            "tstamp_day": _mappings["date"],
            "queue": _mappings["text"],
            "site_name": _mappings["text"],
            "site_location": _mappings["geo"],
            "walltime_hours": _mappings["float"],
            "actualcorecount_total": _mappings["integer"],
            "corecount_total": _mappings["integer"],
            "cpu_utilization": _mappings["float"],
            "cpu_utilization_fixed": _mappings["float"],
            "cputime_hours": _mappings["float"],
            "finished_jobs": _mappings["integer"],
            "failed_jobs": _mappings["integer"],
            "site_efficiency": _mappings["float"]
        }
    }
}


# [START extract_data]
def extract_data(**kwargs):
    execution_date = kwargs['execution_date']

    output_path = f"/data/sites-efficiency/efficiency-{execution_date.strftime('%Y-%m-%d')}-airflow.csv"
    kwargs['ti'].xcom_push(key='output_path', value=output_path)

    from_date = (execution_date - dt.timedelta(days=1)).strftime('%d-%m-%Y')
    to_date = execution_date.strftime('%d-%m-%Y')

    cmd = f'/srv/SlowTaskAnalysis/venv/bin/python3 export_efficiency.py' \
        f' --from {from_date} --to {to_date} --output {output_path}'

    process = subprocess.Popen(cmd, cwd='/srv/SlowTaskAnalysis/app/django_react', shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    stdout, stderr = process.communicate()

    print(stdout.decode("utf-8"))
    if process.returncode != 0:
        updated_log = re.sub('--connection ".+"', "", cmd)
        print(stderr.decode("utf-8"))
        raise subprocess.CalledProcessError(1, updated_log)
# [END extract_data]


# [START push_to_elasticsearch]
def push_to_elasticsearch(**kwargs):
    execution_date = kwargs['execution_date']

    ti = kwargs['ti']
    file_path = ti.xcom_pull(key='output_path')
    if os.path.exists(file_path):
        connection = Elasticsearch(hosts=["localhost"])
        index_name = f"queues-metrics-v1-{execution_date.strftime('%Y-%m')}"
        print(f"Index to push inside: {index_name}")

        if not connection.indices.exists(index=index_name):
            print(f"Creating index: {index_name}")
            connection.indices.create(index=index_name, body=mapping)
            print(f"Index created")

        with open(file_path, "r") as file:
            reader = csv.DictReader(file)
            helpers.bulk(connection, reader, index=index_name)
    else:
        print(f"Nothing to push, file not found: {file_path}")
# [END push_to_elasticsearch]


# [START clear_elasticsearch]
def clear_elasticsearch(**kwargs):
    connection = Elasticsearch(hosts=["localhost"])
    if connection.indices.exists(index="queues-metrics-v1-*"):
        connection.indices.delete(index="queues-metrics-v1-*")
# [END clear_elasticsearch]


t1 = PythonOperator(
    task_id='extract_data',
    provide_context=True,
    python_callable=extract_data,
    dag=dag,
)

t2 = PythonOperator(
    task_id='push_to_elasticsearch',
    provide_context=True,
    python_callable=push_to_elasticsearch,
    dag=dag,
)

t3 = PythonOperator(
    task_id='clear_elasticsearch',
    provide_context=True,
    python_callable=clear_elasticsearch,
    dag=dag_clear
)

t1 >> t2
