# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

from enum import Enum
from abc import ABC, abstractmethod

class IdentitySelector(Enum):
    DefaultCredential = "DefaultAzureCredential"
    AzureCli = "AzureCliCredential"
    OakSystem = "OakSystemCredential"

class IIdentityProvider(ABC):
    DEFAULT_TOKEN_ENDPOINT = "https://management.core.windows.net/"

    def __init__(self, provider:IdentitySelector):
        self.provider:IdentitySelector = provider

    @abstractmethod
    def get_token() -> str:
        pass
