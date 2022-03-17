# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import os
import logging
from datetime import datetime, timedelta

from dagcontext.airflowutil.varloader import AirflowVarialbeLoader
from dagcontext.configurations.constants import Constants
from dagcontext.configurations.env_config import EnvironmentConfiguration

from tasks.exampletasks import ExampleTasks

# Import the right objects based on airflow version 2.2.1 vs 1.10.12
from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except Exception as ex:  # pylint: disable=broad-except
    from airflow.operators.python_operator import PythonOperator

log = logging.getLogger(__name__)

with DAG(
    dag_id="context_example",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    dagrun_timeout=timedelta(minutes=60),
    tags=["demo"],
    params={"OAK": "Examples"},
) as dag:


    # Load up the deployment details for processing
    environment_config = EnvironmentConfiguration(
        # Anything you want to load from the os.environ settings
        environment_variables= [
            Constants.OAK_IDENTITY.IDENTITY_ENDPOINT,
            Constants.OAK_IDENTITY.IDENTITY_HEADER,
            Constants.OAK_IDENTITY.OAK_HOST
        ],
        # Anything you want to load from Airflow variables
        variable_loader = AirflowVarialbeLoader(),
        airflow_variables = [
            Constants.AIRFLOW_VARS.COGSRCH_SEARCH_INSTANCES
        ],
    )

    # Set up paths because we'll use them for logging, activity log, xcom passing
    # where payload > 64KB
    airlfow_folder = os.getcwd()
    dag_folder = os.path.join(airlfow_folder, "dags")
    temp_folder = os.path.join(dag_folder, "tmp/example")

    if not os.path.exists(temp_folder):
        os.makedirs(temp_folder)

    environment_config.update_config(Constants.ENVIRONMENT.WORKING_DIRECTORY, airlfow_folder)
    environment_config.update_config(Constants.ENVIRONMENT.DAG_DIRECTORY, dag_folder)
    environment_config.update_config(Constants.ENVIRONMENT.TEMP_DIRECTORY, temp_folder)  

    # Show Context and Get some data
    show_context = PythonOperator(
        task_id="show_context",
        python_callable=ExampleTasks.show_context,
        op_kwargs=environment_config.get_config(),
        provide_context=True,
    )

    settings = environment_config.get_config(
        {
            Constants.AIRFLOW_CTX.XCOM_TARGET: ["show_context"]
        }
    )
    consume_xcom = PythonOperator(
        task_id="consume_xcom",
        python_callable=ExampleTasks.consume_xcom,
        op_kwargs=settings,
        provide_context=True,
    )

    # Dag Flow
    show_context >> consume_xcom

