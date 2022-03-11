# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

from abc import ABC, abstractmethod


class IVariableLoader(ABC):
    """
    Interface to mask airflow imports, suitable for unit testing
    """

    @abstractmethod
    def load(self, variable:str) -> str:
        """
        Load a specific variable value from the system (typically Airflow)
        """
