
import json
import requests
from dagcontext.configurations.constants import Constants
from dagcontext.authentication.identityprovider import IIdentityProvider, IdentitySelector


class SystemIdentity(IIdentityProvider):
    def __init__(self, endpoint, header):
        super().__init__(IdentitySelector.OakSystem)
        self.endpoint = endpoint
        self.header = header

        if self.endpoint is None:
            self.endpoint = Constants.OAK_IDENTITY.OAK_DEFAULT_IDENTITY_ENDPOINT

    def get_token(self) -> str:

        url = Constants.OAK_IDENTITY.OAK_SYSTEM_IDENTITY_ENDPOINT_URL.format(
            identity_endpoint = self.endpoint
        )
        headers = {
            'Metadata': 'true'
        }

        if self.header: 
            headers['X-IDENTITY-HEADER'] = self.header

        return_token = None
        response = requests.get(url=url, headers=headers)
        if response.status_code != 200:
            raise Exception("Failed to get MSI token on endpoint {}".format(url))
        else:
            data_msi = json.loads(response.text)
            return_token = data_msi["access_token"]

        return return_token