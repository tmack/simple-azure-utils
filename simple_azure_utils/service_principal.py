import json
import os
import logging
from dataclasses import dataclass


@dataclass
class ServicePrincipal:
    """Class for loading Azure Service Principals"""
    tenant_id: str
    client_id: str
    client_secret: str

    def __init__(self, tenant_id=None, client_id=None, client_secret=None, settings_file_path=None):
        # load the service principal setting
        self.tenant_id = self.load_setting('AZ_TENANT_ID', tenant_id, settings_file_path)
        self.client_id = self.load_setting('AZ_CLIENT_ID', client_id, settings_file_path)
        self.client_secret = self.load_setting('AZ_CLIENT_SECRET', client_secret, settings_file_path)

        # if 'AZURE' is used instead of 'AZ'
        self.tenant_id = self.load_setting('AZURE_TENANT_ID', self.tenant_id, settings_file_path)
        self.client_id = self.load_setting('AZURE_CLIENT_ID', self.client_id, settings_file_path)
        self.client_secret = self.load_setting('AZURE_CLIENT_SECRET', self.client_secret, settings_file_path)

    # load service principal settings into env variables, this makes it easier to work with the Azure authentication
    def load_service_principal_into_environment(self):
        if self.tenant_id is not None:
            os.environ['AZ_TENANT_ID'] = self.tenant_id
        if self.client_id is not None:
            os.environ['AZ_CLIENT_ID'] = self.client_id
        if self.client_secret is not None:
            os.environ['AZ_CLIENT_SECRET'] = self.client_secret

        # if 'AZURE' is used instead of 'AZ
        if self.tenant_id is not None:
            os.environ['AZURE_TENANT_ID'] = self.tenant_id
        if self.client_id is not None:
            os.environ['AZURE_CLIENT_ID'] = self.client_id
        if self.client_id is not None:
            os.environ['AZURE_CLIENT_SECRET'] = self.client_secret

    def load_setting(self, secret_name, default_value, settings_file_path=None):
        # if default value is provided, just use that
        if default_value is not None:
            return default_value
        # if a default value is not provided, try to load from file
        if settings_file_path is not None:
            secret_value = self.load_secret_from_file(secret_name, settings_file_path)
            if secret_value is not None:
                return secret_value
        # if the file doesn't have a secret check the environment
        if os.getenv(secret_name) is not None:
            return os.environ[secret_name]

    @staticmethod
    def load_secret_from_file(secret_name, settings_file_path):
        if os.path.exists(settings_file_path) is False:
            logging.debug(f'settings_file_path: {settings_file_path} set but file does not exist')
            return None
        with open(settings_file_path, 'r') as f:
            raw_settings = f.read()
            settings = json.loads(raw_settings)
            if secret_name not in settings:
                logging.warning(f'secret_name: {secret_name} not found in file: {settings_file_path}')
                return None
            return settings[secret_name]

    def get_tenant_id_client_id_and_secret(self):
        return self.tenant_id, self.client_id, self.client_secret
