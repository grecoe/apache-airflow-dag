# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:

from dagcontext.authentication.identityprovider import IIdentityProvider, IdentitySelector
from azure.identity import (
    AzureCliCredential, 
    DefaultAzureCredential
)

class AzureIdentity(IIdentityProvider):
    def __init__(self):
        super().__init__(IdentitySelector.AzureCli)

    def get_token(self) -> str:
        cred = AzureCliCredential()
        token_obj = cred.get_token(IIdentityProvider.DEFAULT_TOKEN_ENDPOINT)
        return token_obj.token

class DefaultIdentity(IIdentityProvider):
    def __init__(self):
        super().__init__(IdentitySelector.DefaultCredential)

    def get_token(self) -> str:
        cred = DefaultAzureCredential()
        token_obj = cred.get_token(IIdentityProvider.DEFAULT_TOKEN_ENDPOINT)
        return token_obj.token
