# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import json
import typing
from dagcontext.configurations.constants import Constants
from dagcontext.generic.activelog import ActivityLog

class AirflowContextConfiguration:
    """
    Generic configuraton object that loads the "params" from the
    task context sent to non virtualenv python tasks. The params become
    first class properties on the class itself.
    """
    def __init__(self, context: dict):
        """
        Recieves the context object passed to tasks by Airflow. Main goal is to
        pull the parameters that were passed to the DAG.

        Context should be a dictionary and at most have sub dictionaries of a single leve.

        So, flatten them out and the rest of the code doesn't need to change.

        Version 1.10.12
            Configuration exists in  context[Constants.AIRFLOW_CTX.TASK_DAGRUN].conf

        Version 2.2.1
            Configuration exists in context[Constants.AIRFLOW_CTX.TASK_PARAMS]
        """
        # Load params, but in 1.10.12 it will not have the execution config
        if Constants.AIRFLOW_CTX.TASK_PARAMS in context:
            ActivityLog.log_info("LOADING PARAMETERS: {}".format(Constants.AIRFLOW_CTX.TASK_PARAMS))
            # When triggered through the Airflow 2.x UI with a payload, this 
            # is enough to get what we are looking for. 
            self._load_setting(context[Constants.AIRFLOW_CTX.TASK_PARAMS])

        # Pick something that SHOULD be there, in 1.12 it won't be....
        if not self.has_attribute(Constants.AIRFLOW_EX_CTX.SYSTEM_PARTITION_ID):
            if Constants.AIRFLOW_CTX.TASK_DAGRUN in context:
                ActivityLog.log_info("LOADING DAGRUN: {}".format(Constants.AIRFLOW_CTX.TASK_DAGRUN))
                # When triggered through the Airflow 1.x UI with a payload, this is
                # enough to get what we are looking for. 
                run_configuration = context[Constants.AIRFLOW_CTX.TASK_DAGRUN].conf
                self._load_setting(run_configuration)
 
                if Constants.AIRFLOW_CTX.TASK_DAGRUN_EXECUTION_CONTEXT in context[Constants.AIRFLOW_CTX.TASK_DAGRUN].conf:
                    ActivityLog.log_info("LOADING EXECUTION CTX: {}".format(Constants.AIRFLOW_CTX.TASK_DAGRUN_EXECUTION_CONTEXT))
                    # When triggered through the OAK API, we need to be more specific about where to load
                    # the information, so this works to get the payload. 
                    self._load_setting(context[Constants.AIRFLOW_CTX.TASK_DAGRUN].conf[Constants.AIRFLOW_CTX.TASK_DAGRUN_EXECUTION_CONTEXT])

    def _load_setting(self, settings: dict) -> None:
        """
        Sets attributes on this instance of the class based on the content of the incoming
        dict. Sub keys that are dicts have thier values raised up to first class attributes
        of the class.

        Parameters:
        settings: Dictionary of values to add as attributes to this class instance

        Returns:
        None
        """
        for param in settings:
            current_value = settings[param]
            if isinstance(current_value, dict):
                for sub_param in current_value:
                    setattr(self, sub_param, current_value[sub_param])
            else:
                setattr(self, param, current_value)

    def has_attribute(self, attribute_name: str) -> bool:
        """
        Determine if the property is in this object

        Parameters:
        attribute_name: Property name to find

        Returns:
        bool

        Throws:
        None
        """
        return attribute_name in self.__dict__

    def get_attribute(self, attribute_name: str) -> typing.Any:
        """
        Get the value for the given attribute.

        Parameters:
        attribute_name: Property name to find

        Returns:
        The value of the property

        Throws:
        KeyError property not present
        """
        return self.__dict__[attribute_name]

    def put_attribute(self, attribute_name: str, value) -> None:
        """
        Get the value for the given attribute.

        Parameters:
        attribute_name: Property name to find

        Returns:
        The value of the property

        Throws:
        KeyError property not present
        """
        self.__dict__[attribute_name] = value

    def to_json(self, additional: dict = None) -> typing.Optional[str]:
        """
        Translate this object to a dictionary with optional additional
        properties.

        Parameters:
        additiona: Additional properties to append to the return dict.

        Returns:
        String representation of JSON dictionary

        Throws:
        None
        """
        return_value = None

        output = self.__dict__.copy()
        if additional:
            output.update(additional)

        if output and len(output):  # pylint: disable=len-as-condition
            return_value = json.dumps(output)

        return return_value
