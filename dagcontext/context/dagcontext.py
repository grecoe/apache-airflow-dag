# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import json
import os
import typing
import shutil
from enum import Enum
from pprint import pprint
from dagcontext.authentication.identityprovider import IdentitySelector
from dagcontext.authentication.authfactory import AuthFactory
from dagcontext.configurations.airflowctx_config import AirflowContextConfiguration
from dagcontext.configurations.constants import Constants
from dagcontext.context.inflight import InflightTracker
from dagcontext.generic.activelog import ActivityLog

class PropertyClass(Enum):
    Environment = "Environment"
    AirflowContext = "AirflowContext"
    XCOM = "XCOM"

class DagContext:
    """
    Class that is provided the context object of the DAG execution. With that information
    it is parsed to give straighforward access to whatever the task requires.

    - The raw Airflow context object (dict)
    - Parsed out execution configuration
    - Parsed out deployment information (content of exampleconf.json) if present
    - The xcom result to be used (identified in the context with the Constants.AIRFLOW_CTX.XCOM_TARGET key)
    """

    def __init__(self, context):
        """
        Parses out all of the neccesary information from a context obejct passed
        to the DAG and DAG Tasks during execution.

        This data also contains the settings from the environment and MAY 
        include XCOM data from other tasks.

        Parameters:
        context: Conetxt passed by Airflow to the DAG/task. Data includes information
                 about the execution including a params which is the config passed to
                 the dag for execution.

                 This information may be extended with xcom results from other tasks.
                 See the main DAG implementation.

        Returns:
        None
 
        Throws:
        None
        """
        # Main context object
        self.context = context
        # Settings from the context['params'] property (dynamic)
        self.airflow_context:AirflowContextConfiguration = AirflowContextConfiguration(context)
        # Settings from environment
        self.environment_settings = None
        # Any XCOM data passed along
        self.xcom_target = {}
        # Instances covered by 
        self.inflight_tracker:InflightTracker = None

        # Authentication object
        self.authentication_tokens:typing.Dict[IdentitySelector, str] = None

        # Verify that the context has a task instance AND that there is a defined xcom_target
        # in teh payload. 
        if Constants.AIRFLOW_CTX.TASK_INSTANCE in context and Constants.AIRFLOW_CTX.XCOM_TARGET in context:
            targets = context[Constants.AIRFLOW_CTX.XCOM_TARGET]
            if not isinstance(targets, list):
                targets = [targets]

            for target in targets:
                self.xcom_target[target] = self.xcom_persist_load(target)

        # Get the environment settings (os.environ and airflow vars) that may be part of this 
        # context object. 
        if Constants.ENVIRONMENT.ENVIRONMENT_SETTINGS in context:
            self.environment_settings = context[Constants.ENVIRONMENT.ENVIRONMENT_SETTINGS]
            if isinstance(self.environment_settings, str):
                self.environment_settings = json.loads(self.environment_settings)

        # Collect the RUN ID of this task run as it's required for setting up duplciate processing
        # prevention using the InflightTracker object.
        self.run_id = self.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.SYSTEM_RUN_ID, False)
        if not self.run_id:
            self.run_id = self.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.OPTIONAL_SYSTEM_RUN_ID, False)

        # When testing locally and not in Airflow this might be a problem, but set up the inflight
        # tracker so we can keep track of what this specific instance is processing.
        if self.run_id:
            # Set up inflight tracking
            self.inflight_path = os.path.join(
                self.get_value(PropertyClass.Environment, Constants.ENVIRONMENT.TEMP_DIRECTORY),
                Constants.XCOM_PERSIST.INFLIGHT_PERSIST_PATH
            )
            self.inflight_tracker = InflightTracker(self.run_id, self.inflight_path)

            # Set up activity log
            ActivityLog.ACTIVITY_LOG_DIRECTORY = os.path.join(
                self.get_value(PropertyClass.Environment, Constants.ENVIRONMENT.TEMP_DIRECTORY),
                Constants.LOG.ACTIVITY_LOG_DIRECTORY
            )
            ActivityLog.ACTIVITY_LOG_BASETASK_ID = self.run_id


        # Again, for testing, don't throw if not there (won't be) but will be in OAK execution.
        id_value = self.get_value(PropertyClass.AirflowContext, Constants.AIRFLOW_EX_CTX.SYSTEM_FILE_ID, False)
        if not isinstance(id_value, list):
            self.airflow_context.put_attribute(Constants.AIRFLOW_EX_CTX.SYSTEM_FILE_ID, [id_value])

    def get_authentication_token(self, selector:IdentitySelector) -> str:
        """
        Force the auth factory to try and find the following tokens
        
        AzureCLI
        Default
        SystemMSI 

        Returns a dict of values where keys are IdentitySelector(Enum) values and values are
        the actual token acquired, if any
        """
        if self.authentication_tokens is None:
            self.authentication_tokens = AuthFactory.load_authentication(
                    self.get_value(PropertyClass.Environment, Constants.OAK_IDENTITY.IDENTITY_ENDPOINT), 
                    self.get_value(PropertyClass.Environment, Constants.OAK_IDENTITY.IDENTITY_HEADER)
                )

        return_token = None
        if selector in self.authentication_tokens:
            return_token = self.authentication_tokens[selector]
        return return_token

    def get_value(self, propClass:PropertyClass, field_name:str, except_on_misssing:bool = True) -> typing.Any:
        """
        Put a setting by name into one of the configuration objects contained in this 
        instance of the context. 

        Supported Types: 
            PropertyClass.AirflowContext
            PropertyClass.Environment
            PropertyClass.XCOM
        Parameters:

        propClass:
            Instance of the Enum PropertyClass
        field_name:
            Name of the property to set
        field_value:
            Value to set

        Returns:
            None

        Throws:
            KeyError if field name is None 
        """

        if field_name is None:
            DagContext._except_on_missing_key(propClass.value, "No field name presented")
        
        property_present:bool = False
        return_value = None
        if propClass == PropertyClass.AirflowContext:
            if self.airflow_context.has_attribute(field_name):
                property_present = True
                return_value = self.airflow_context.get_attribute(field_name)
        elif propClass == PropertyClass.Environment:
            if field_name in self.environment_settings:
                property_present = True
                return_value = self.environment_settings[field_name]
        elif propClass == PropertyClass.XCOM:
            return_value = self.__find_child_xcom_target(field_name, None)
            if return_value is not None:
                property_present = True
        else:
            ActivityLog.log_warning("{} is invalid PropertyClass".format(propClass))

        if property_present == False and except_on_misssing == True:
            DagContext._except_on_missing_key(propClass.value, field_name)

        return return_value

    def put_value(self, propClass:PropertyClass, field_name:str, field_value:typing.Any):
        """
        Put a setting by name into one of the configuration objects contained in this 
        instance of the context. 

        Supported Types: PropertyClass.AirflowContext, PropertyClass.Environment

        Parameters:

        propClass:
            Instance of the Enum PropertyClass
        field_name:
            Name of the property to set
        field_value:
            Value to set

        Returns:
            None

        Throws:
            KeyError if field name is None 
        """

        if field_name is None:
            DagContext._except_on_missing_key(propClass.value, "No field name presented")
        
        if propClass == PropertyClass.AirflowContext:
            self.airflow_context.put_attribute(field_name, field_value)
        elif propClass == PropertyClass.Environment:
            self.environment_settings[field_name] = field_value
        else:
            ActivityLog.log_warning("{} PropertyClass does not support put".format(str(propClass)))

    def __find_child_xcom_target(self, field_name: str, sub_field_name: typing.Optional[str], data: dict = None) -> typing.Any:
        """
        Find a setting by name in the xcom data that was passed with the context
        object.

        Searches for all first party properties on any xcom object in the settings. If
        a sub_field_name is passed, the field_name property is exepcted to be the parent
        property and the sub_field_name property is returned instead.

        Parameters:
        field_name: Name of property to find
        sub_field_name: The sub property on the property identified by field_name, if present
        data: Dictionary of key/value data to search for properties.

        Returns:
        sub_field_name == None
            Return the value first occurance where key in a dictionary == field_name

        sub_field_name is not None
            Find the object identified by teh first instance of field_name, then search in that
            object for a value identified by the key sub_field_name

        Throws:
        None
        """
        return_value = None
        if data is None:
            data = self.xcom_target

        if data is not None:  # pylint: disable=len-as-condition

            if field_name in data:
                return_value = data[field_name]

            if not return_value:
                for key in data:
                    if isinstance(data[key], dict):
                        return_value = self.__find_child_xcom_target(field_name, sub_field_name, data[key])
                        if return_value:
                            break

            if sub_field_name and return_value and isinstance(return_value, dict):
                # If looking for a sub field, we now have the main dictionary, repeat to get sub value
                return_value = self.__find_child_xcom_target(sub_field_name, None, return_value)

        return return_value

    def xcom_persist_save(self, persisted_task:str, data:typing.Any) -> str:
        """
        Passing data in XCOM on Airflow is limited. The docs say small data, others say 64K. 
        When the payload becomes too large, the DAG will persist the data to:
            Constants.ENVIRONMENT.TEMP_DIRECTORY/Constants.XCOM_PERSIST.XCOM_PERSIST_PATH

        Because multiple instances of the DAG can be running at any one time, the file 
        being persisted has the run_id of this instance appended to it to avoid conflicts. 

        Parameters
        persisted_task: must be a member of the constants class XCOMPersistanceConstants
        data: Content to dump to a file. If a dict or list, it's JSON dumped, otherwise it goes as is

        Returns:
        The full path of the file generated.

        Throws:
        ValueError: If persisted_task is not a member of  XCOMPersistanceConstants
        """
        return_path = None

        if DagContext._is_valid_persistance(Constants.XCOM_DATA, persisted_task):
            # Do something
            file_name = persisted_task
            if self.run_id:
                file_name = "{}_{}".format(self.run_id, persisted_task)

            xcom_directory = self._create_xcom_path(Constants.XCOM_PERSIST.XCOM_PERSIST_PATH)
            persist_path = os.path.join(xcom_directory, file_name)
            
            output_data = data
            if isinstance(output_data, list) or isinstance(output_data,dict):
                output_data = json.dumps(output_data, indent=4)

            with open(persist_path, "w") as persisted_data:
                persisted_data.writelines(output_data)

            return_path = persist_path
        else:
            raise ValueError("Requested task not in XCOM PERSIST values: {}".format(persisted_task))

        return return_path

    def xcom_persist_load(self, task_id:str) -> typing.Any:
        """
        Loads the data from an XCOM field. If the data is a file (the only value), the 
        return value is the contents of that file. If not, the return value is the value
        that the XCOM system has. 

        Parameters:
        task_id: The actual Airflow task name to find a value for 

        Returns:
        Value contained or value in a file
        """
        return_data = None
        
        if Constants.AIRFLOW_CTX.TASK_INSTANCE in self.context:        
            raw_data = self.context[Constants.AIRFLOW_CTX.TASK_INSTANCE].xcom_pull(task_ids=task_id)
            try:
                # Is it a file path?
                if os.path.exists(raw_data):
                    file_content = None
                    with open(raw_data, "r") as xcom_data:
                        file_content = xcom_data.readlines()
                        file_content = "\n".join(file_content)
                        # Just in case it fails.....
                        raw_data = file_content

                    if file_content:
                        return_data = json.loads(file_content)

                else:
                    return_data = json.loads(raw_data)
            except Exception as ex:  # pylint: disable=broad-except, unused-variable
                # Not a JSON object and not a file
                return_data = raw_data

        return return_data

    def xcom_persist_clear(self, clear_all:bool = False) -> int:
        """
        Clear out all of the XCOM data files that were stored in 
            Constants.ENVIRONMENT.TEMP_DIRECTORY/Constants.XCOM_PERSIST.XCOM_PERSIST_PATH
        """
        return_count = 0

        xcom_directory = self._create_xcom_path(Constants.XCOM_PERSIST.XCOM_PERSIST_PATH)

        if os.path.exists(xcom_directory):
            if clear_all:
                shutil.rmtree(xcom_directory)
            elif self.run_id:
                owned_files = []
                for root, dirs, files in os.walk("."):
                    for name in files:
                        print(name)
                        if name.startswith(self.run_id):
                            owned_files.append(os.path.join(xcom_directory, name))

                for owned in owned_files:
                    # Sanity check
                    if os.path.exists(owned):
                        return_count += 1
                        os.remove(owned)

        return return_count

    def summarize(self):
        """
        Print a summary of the different settings that are contained within this
        instance.
        """
        print("Execution Configuration from DAG:")
        print(self.airflow_context.to_json())

        print("Environment Settings:")
        pprint(self.environment_settings)

        print("XCOM Passed Data:")
        pprint(self.xcom_target)

    def _create_xcom_path(self, path:str) -> str:
        """
        Create the XCOM directory at:
        Constants.ENVIRONMENT.TEMP_DIRECTORY/Constants.XCOM_PERSIST.XCOM_PERSIST_PATH

        Return full path for further processing.
        """
        temp_directory = self.get_value(PropertyClass.Environment, Constants.ENVIRONMENT.TEMP_DIRECTORY)
        xcom_directory = os.path.join(temp_directory, path)

        if not os.path.exists(xcom_directory):
            os.makedirs(xcom_directory)

        return xcom_directory

    @staticmethod
    def _except_on_missing_key(configuration_type: str, key_name: str):
        """
        Utilized by each of the find* calls above when
        1. The key is not found
        2. Raising an exception was reqested

        Parameters:
        configuration_type: Differentiator name for setting fields
        key_name: Property not found

        Returns:
        None

        Throws:
        KeyErryr - ALWAYS
        """
        message = "Configuration - {} - missing value for {}".format(configuration_type, key_name)
        raise KeyError(message)

    @staticmethod
    def _is_valid_persistance(constants_class, setting:str ):
        """
        constants_class: One of the classes in constants.py to find a setting in
        setting: Setting to find to see if it's a valid choice.
        """
        return_value = False
        for prop in dir(constants_class):
            val = getattr(constants_class, prop)
            if isinstance(val, str):
                if setting == val:
                    return_value = True
                    break

        return return_value
