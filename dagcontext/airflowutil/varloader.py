# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

import json
from dagcontext.airflowutil.ivarloader import IVariableLoader

from airflow.models import Variable 


class AirflowVarialbeLoader(IVariableLoader):
    """
    Airflow variable loader, wrapped to make mocking easier
    """

    def load(self, variable:str) -> str: # pylint: disable=no-self-use
        """
        Load a specific variable value from the Airflow variables
        """
        value = Variable.get(variable)
        if isinstance(value, str):
            try:
                value = json.loads(value)
            except Exception as ex:
                # It's OK as only one will actually be a JSON object
                pass

        return value
