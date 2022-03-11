# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import os
import typing
from dagcontext.airflowutil.ivarloader import IVariableLoader
from dagcontext.configurations.constants import Constants


class EnvironmentConfiguration:
    """
    Wrapper class for the sideloaded configuration which contains all of the deployment details
    that will be needed to process. This must be in a JSON file sitting next to the main dag file.
    """

    def __init__(self, 
        environment_variables:typing.List[str] = None, 
        variable_loader: IVariableLoader = None, 
        airflow_variables: typing.List[str] = None):
        """
        Sideload configuration is either from a sideloaded JSON file or
        the Airflow variables.

        The JSON file must be in the form of a dictionary.

        They are stored in the intenal config_object

        Parameters:
        directory: System path holding the sideload config
        config_file: The actual sideload config file, does not have to exist
        airflow_variable_loader: Class instance with single static call .load(variable:str)
        airflow_variables: Variables to read from Airflow

        Returns:
        None

        Throws:
        FileNotFoundError if blob is not present.
        """
        self.variable_loader = variable_loader
        self.airflow_variables = airflow_variables
        self.environment_variables = environment_variables
        self.config_object = {}

        if self.environment_variables is not None and len(self.environment_variables):  # pylint: disable=len-as-condition
            for env_var in self.environment_variables:
                if env_var in os.environ:
                    self.config_object[env_var] = os.environ[env_var]
                else:
                    self.config_object[env_var] = None

        if self.airflow_variables is not None and len(self.airflow_variables):  # pylint: disable=len-as-condition
            if self.variable_loader:
                for airf_var in self.airflow_variables:
                    self.config_object[airf_var] = self.variable_loader.load(airf_var)

    def update_config(self, field_name: str, field_value: typing.Any):
        """
        Updates the internal configuration settings with the incoming data.

        Parameters:
        field_name: Name of new or existing property
        field_value: Value to set to property

        Returns:
        None

        Throws:
        ValueError if field name is None
        """
        if field_name is None:
            raise ValueError("field_name cannot be None")

        if self.config_object is not None:
            self.config_object[field_name] = field_value

    def get_config(self, optionals: dict = None) -> dict:
        """
        Transform this internal data to a dictionary to pass along to
        downstream tasks

        Parameters:
        optionals: Additional properties to add to the return dictionary.

        Returns:
        A dictionary to pass to downstream tasks in form
        {Constants.SIDELOAD.SIDELOAD_SETTINGS : dict:properties}

        Throws:
        None
        """
        return_data = {Constants.ENVIRONMENT.ENVIRONMENT_SETTINGS: None}
        if self.config_object is not None:
            return_data[Constants.ENVIRONMENT.ENVIRONMENT_SETTINGS] = self.config_object.copy()
            if optionals and isinstance(optionals, dict):
                return_data.update(optionals)

        return return_data
