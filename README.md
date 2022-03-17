# apache-airflow-dag

This project contains the basis of an Apache Airflow DAG executing within a managed OSDU instance. While it doesn't do a lot, it is a template to be used in other development efforts by generating a context (DagContext) object out of multiple points of data. 

The DagContext object is used within each individual Airflow Task making available a wide array of configuration information to the developer so that the developer need only focus on the specific task operations at hand. 

## DagContext Data
- os.environ settings
- Airflow Variable settings
- Execution context object - specifically for a payload which looks like:
```python
{
    "runId": "unique_run_id",
    "executionContext": {
        "id": "record_id or list of record_ids"",
        "dataPartitionId": "some_data_partition",
        "kind": "osdu:wks:dataset--File.Generic:1.0.0"
    }
}
```
- Managed identity collection of access tokens
    - Managed instance in Azure running in AKS with User Managed Identity. Typically used when that identity is given access rights to other Azure resources.
    - Managed instance identity within the OSDU environment (calling file/storage/etc api's)
- Parses and makes available XCOM data that is passed to an individual task. 

## Additional Solutions

### Inflight Records context/inflight
Due to some timing concerns with a recent deployment in which one stage that performed AI took a considerable amount of time to complete, some records were being processed more than once.

To resolve this, add in a unique record ID into the InflightTracker class. This class persists out a record for each file a DAG has claimed ownership to so your task should first check to see if a file is in-flight. If so, leave it alone, if not add it to the in-flight tracking and continue with processing. 

It is DAG responsiblity to clear out it's in-flight cache, so any task AFTER the use of the InflightTracker class MUST clean the cache before completing. See the task /tasks/exampletasks:consume_xcom for an example. 

### Activity Log
This is an additional logging function to write out files to the DAG path /tmp/example (set in the DAG itself) and creates a log file of the accumulated information pushed by ALL tasks in the process. 

The log file name will match the run-id of the DAG execution. 

The log cache is limited to 50 most recent logs and can be configured dagcontext/generic/activelog with the ACTIVITY_LOG_CACHE_SIZE field. 

# Environment
You will need to have a conda environment to run this example with the following contained within it. 
azure-identity            1.7.0
azure-storage-blob        12.9.0

<b>NOTE:</b> You will be running on an Airflow instance so airflow.* will be in that environment

# Examples
The DAG itself is defined in the ./example_dag.py file but there are two individual tasks that are used to consume the context and XCOM data between the tasks in ./tasks/exampletasks.py

# Packaging
To create the DAG package, zip up the following into a zip file that is NOT called dagcontext or tasks

- ./dagcontext
- ./tasks
- ./__ init __.py
- ./example_dag.py