

class Environment:
    ENVIRONMENT_SETTINGS = "environment_settings"
    WORKING_DIRECTORY = "working_directory"
    TEMP_DIRECTORY = "temp_directory"
    DAG_DIRECTORY = "dag_directory"

    AUTHENTICATION = "authentication"

class AirflowContextConstants:
    """
    Constant fields found in the airflow context passed to a DG
    """

    XCOM_TARGET = "xcom_target"
    TASK_INSTANCE = "task_instance"
    TASK_PARAMS = "params"
    TASK_DAGRUN = "dag_run"
    TASK_DAGRUN_EXECUTION_CONTEXT = "execution_context"

    # When triggered by the OAK API we seem to get an auth_token
    # in the payload from the call. We might be able to search for
    # that and side-step the other auth code when launched by OAK, however, 
    # according to Kishore it's the user token and we just don't know the 
    # expiry time....not worth the risk I guess that it expires in the middle
    # of processing. The other issue might arise where the user has access
    # to the workflow API but not the FileService API...so there's that.
    TASK_CONTEXT_AUTHENTICATION = "authToken"

class AirflowExecutionConstants:
    """
    Fields expected in the JSON execution confinguration of the DAG
    """

    SYSTEM_RUN_ID = "runId"
    OPTIONAL_SYSTEM_RUN_ID = "run_id"
    
    SYSTEM_EXECUTION_CONTEXT = "executionContext"  # Dict holding the following values also see constants
                                                   # as this isn't neccesarily the right thing. 
    SYSTEM_FILE_ID = "id"  # Testing is blob name
    SYSTEM_PARTITION_ID = "dataPartitionId"  # Testiong is container
    SYSTEM_FILE_KIND = "kind"  # Tsting is account


class AirflowVariablesConstants:
    """
    Names of Airflow variables to load
    """ 
    COGSRCH_SEARCH_INSTANCES = "km_search_instances"

class XCOMPersistanceConstants:
    XCOM_PERSIST_PATH = "xcom_data"
    INFLIGHT_PERSIST_PATH = "inflight"

    """Tasks have a persistance here so it can be found, when using XCOM 
    persistance each one has to have a unique name"""
    EXAMPLE_TASK = "example.json"

class XcomDataConstants:
    """
    XCOM Field names used to pass data between tasks
    """
    FIRST_TAKS_XCOM_PERSIST_NAME = "first_task.json"
    FIRST_TASK_XCOM_NAME = "first_task"

    TASK_DATA_EXAMPLE = "example_data"

class OakIdentityConstants:
    """
    For pulling identity information. 

    The fields below are supposed to live in the os.environ for the DAG, however on 
    a OneBox deployment these are NOT correct. 
    """
    # os.environ: MSI fields required in the environment for MSI auth token
    IDENTITY_ENDPOINT = "IDENTITY_ENDPOINT"
    IDENTITY_HEADER = "IDENTITY_HEADER"

    # os.environ: Host for the current instance of OAK to talk with
    OAK_HOST = "AIRFLOW_VAR_AZURE_DNS_HOST"

    # Defaults
    OAK_DEFAULT_IDENTITY_ENDPOINT = "http://169.254.169.254/metadata/identity/oauth2/token"
    OAK_SYSTEM_IDENTITY_ENDPOINT_URL = "{identity_endpoint}?api-version=2018-02-01&resource=https%3A%2F%2Fmanagement.azure.com%2F"

class LogConstants:
    """constants for logging"""
    ACTIVITY_LOG_DIRECTORY = "activity_log"

class Constants:
    ENVIRONMENT = Environment
    # AIrflow context fields
    AIRFLOW_CTX = AirflowContextConstants
    # Airflow execution context
    AIRFLOW_EX_CTX = AirflowExecutionConstants
    # Airflow variables to load
    AIRFLOW_VARS = AirflowVariablesConstants
    # XCOM Persistance fields
    XCOM_PERSIST = XCOMPersistanceConstants
    # XCOM Data fields
    XCOM_DATA = XcomDataConstants
    # OAK Identity
    OAK_IDENTITY = OakIdentityConstants
    # Logging
    LOG = LogConstants
