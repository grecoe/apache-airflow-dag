
import typing
from dagcontext.context.dagcontext import PropertyClass, DagContext
from dagcontext.generic.activelog import ActivityLog
from dagcontext.configurations.constants import Constants
from dagcontext.authentication.identityprovider import IdentitySelector

class ExampleTasks:
    """
    Tasks that interact with the cog search instance. No special requirements as the calls
    all rely on the api key which has to have been obtained first.
    """

    @staticmethod
    def show_context(**context):
        """
        Example task showing how to 
        1. Collect execution context information from the expected payload
        2. Collect environment information collected from the system
        3. How to persist larger XCOM payloads for the next task
        """
        context = DagContext(context)
        context.summarize()


        # Get settings that you KNOW are in the context object
        file_ids = context.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.SYSTEM_FILE_ID)
        oak_partition = context.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.SYSTEM_PARTITION_ID)
        run_id = context.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.SYSTEM_RUN_ID)
        ActivityLog.log_segment("Airflow Execution Information")
        ActivityLog.log_info("Run ID:", run_id)
        ActivityLog.log_info("OAK File ID:", file_ids)
        ActivityLog.log_info("OAK Partition:", oak_partition)

        # Get settings that would have been loaded in environment OR airflow variables
        search_instances = context.get_value(PropertyClass.Environment, Constants.AIRFLOW_VARS.COGSRCH_SEARCH_INSTANCES)
        temp_directory = context.get_value(PropertyClass.Environment, Constants.ENVIRONMENT.TEMP_DIRECTORY)
        oak_host = context.get_value(PropertyClass.Environment, Constants.OAK_IDENTITY.OAK_HOST)

        ActivityLog.log_segment("Environment Information")
        ActivityLog.log_info("OAK HOST:", oak_host)
        ActivityLog.log_info("Temp Directory:", temp_directory)
        ActivityLog.log_info("Search Instances:", search_instances)

        # Airflow has a 64KB limit, not strictly enforced, for XCOM data being passed
        # between tasks. Use a unique name for the data and unique field names between tasks
        # so that there is no conflict. To pass as a bundle, persist the data to disk via
        # the context object
        data_of_interest = {
            Constants.XCOM_DATA.TASK_DATA_EXAMPLE : "Example data being passed"
        }
        # Bundle it into an object to persist. bit if it's small enough you can just return the 
        # following data object
        xcom_data = {Constants.XCOM_DATA.FIRST_TASK_XCOM_NAME: data_of_interest}

        # But when large, persist to disk and pass the location, context knows how to unbundle it
        xcom_loction = context.xcom_persist_save(Constants.XCOM_DATA.FIRST_TAKS_XCOM_PERSIST_NAME, xcom_data)
        return xcom_loction

    @staticmethod
    def consume_xcom(**context):
        """
        Example showing 
        1. How to retrieve information from the XCOM data being passed
        2. How to retrieve system and oak auth tokens 
        3. How to ensure you clear up any files that could potentially left behind by either
            - Inflight tracking of records
            - XCOM payloads persisted to disk
        """

        context = DagContext(context)
        context.summarize()
        ActivityLog.log_segment("Consuming XCOM")
        try:
            # Get XCOM field data passed by the task above (first task)
            xcom_field_value = context.get_value(PropertyClass.XCOM, Constants.XCOM_DATA.TASK_DATA_EXAMPLE)
            ActivityLog.log_info(
                "XCOM Field Value for {}".format(Constants.XCOM_DATA.TASK_DATA_EXAMPLE),
                xcom_field_value)
            
            # Get authentication tokens for default and system
            default_token = context.get_authentication_token(IdentitySelector.DefaultCredential)
            system_auth_token = context.get_authentication_token(IdentitySelector.OakSystem)
            ActivityLog.log_info(
                "Authentication tokens:", 
                "Default Token: {}".format(default_token),
                "OAK Token: {}".format(system_auth_token)
            ) 

            # Now intentionally raise so the data is cleared
            ActivityLog.log_info("Intentional exception to come on missing XCOM Data to clear files")
            context.get_value(PropertyClass.XCOM, "Field Doesn't Exist")

        except Exception as ex:
            # For all follow on tasks, you MUST clear this data yourself so that there 
            # is no accumulating cruft in the temp folders other than logs.
            ActivityLog.log_error(ex)
            context.inflight_tracker.abandon(ex)
            context.xcom_persist_clear(False)
            raise ex
