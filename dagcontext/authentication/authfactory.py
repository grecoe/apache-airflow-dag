# Copyright (c) Microsoft Corporation. All rights reserved.
#
# Licensed under Microsoft Incubation License Agreement:


import typing
from dagcontext.generic.activelog import ActivityLog
from dagcontext.authentication.identityprovider import IIdentityProvider, IdentitySelector
from dagcontext.authentication.systemauth import SystemIdentity
from dagcontext.authentication.azureauth import (
    AzureIdentity,
    DefaultIdentity
)

class AuthFactory:
    @staticmethod
    def load_authentication(system_endpoint, system_header) -> typing.Dict[IdentitySelector, str]:
        auth_collection = {}

        providers:typing.List[IIdentityProvider] = [
            AzureIdentity(),
            DefaultIdentity(),
            SystemIdentity(system_endpoint, system_header)
        ]
        
        for id_provider in providers:
            auth_collection[id_provider.provider] = AuthFactory.__get_token(id_provider) 

        return auth_collection

    @staticmethod
    def __get_token(provider:IIdentityProvider) -> str:
        token = None
        try:
            token = provider.get_token()
        except Exception as ex:
            ActivityLog.log_warning("Unable to acquire token for {}".format(provider.provider), ex)
        return token