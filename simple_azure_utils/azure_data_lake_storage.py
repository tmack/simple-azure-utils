import logging
import os
import re
from pathlib import Path

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

from .service_principal import ServicePrincipal


class AzureStorageAccount:

    def __init__(self, storage_account_name=None, settings_path=None):
        self.storage_account_name = storage_account_name
        self.settings_path = settings_path

    def get_storage_account_name(self, default_value):
        if default_value is not None:
            return default_value
        else:
            return self.storage_account_name

    def get_settings_path(self, default_value):
        if default_value is not None:
            return default_value
        else:
            return self.settings_path

    def upload_data(self, data, upload_path, overwrite=False, storage_account_name=None, settings_path=None):
        container_name = self._get_container_name(upload_path)
        upload_path = self._update_path_from_container_name(container_name, upload_path)
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=upload_path)
        upload_results = blob_client.upload_blob(data, overwrite=overwrite)
        return upload_results

    def upload_file(self, file_path, upload_path, overwrite=False, storage_account_name=None, settings_path=None):
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        container_name = self._get_container_name(upload_path)
        upload_path = self.get_blob_path(upload_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=upload_path)
        with open(file_path, 'rb') as f:
            file_content = f.read()
            upload_results = blob_client.upload_blob(file_content, overwrite=overwrite)
        return upload_results

    def download(self, file_path, download_path, storage_account_name=None, settings_path=None):
        container_name = self._get_container_name(file_path)
        blob_path = self.get_blob_path(file_path)
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=blob_path)
        with open(download_path, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())
        return download_path

    def download_files(self, file_paths, storage_account_name=None, settings_path=None, download_folder='/adls',
                       remove_spaces_from_download_path=False):
        logging.info(f'Downloading files from storage_account_name: {storage_account_name}')
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        download_paths = []
        for file_path in file_paths:
            container_name = self._get_container_name(file_path)
            blob_path = self.get_blob_path(file_path)
            logging.info(f'Downloading blob_path: {blob_path} from container_name: {container_name}')
            blob_client = data_lake_client.get_blob_client(container=container_name, blob=blob_path)
            download_location = self._create_download_dir(blob_path, container_name, download_folder,
                                                          remove_spaces_from_download_path=remove_spaces_from_download_path)

            download_paths.append(download_location)
            with open(download_location, "wb") as my_blob:
                download_stream = blob_client.download_blob()
                my_blob.write(download_stream.readall())
        return download_paths

    def upload_files(self, files_to_upload, storage_account_name=None, settings_path=None, overwrite=False):
        """
        :param files_to_upload: [{'localPath': 'downloads/test.txt', 'uploadPath': 'containername/test-uploads/test.txt'}..]
        :param storage_account_name:
        :param settings_path:
        :param overwrite:
        :return:
        """
        logging.info(f'Uploading files from storage_account_name: {storage_account_name}')
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        upload_results = []
        for file_to_upload in files_to_upload:
            local_path = file_to_upload['localPath']
            upload_path = file_to_upload['uploadPath']
            container_name = self._get_container_name(upload_path)
            blob_path = self.get_blob_path(upload_path)
            logging.info(f'Uploading file: {local_path} to blob_path: {blob_path} in container_name: {container_name}')
            blob_client = data_lake_client.get_blob_client(container=container_name, blob=blob_path)
            with open(local_path, 'rb') as f:
                file_content = f.read()
                upload_result = blob_client.upload_blob(file_content, overwrite=overwrite)
                upload_result['local_path'] = local_path
                upload_result['upload_path'] = upload_path
                upload_results.append(upload_result)
        return upload_results

    @staticmethod
    def _create_download_dir(blob_path, container_name, download_folder, remove_spaces_from_download_path=False):
        download_location = f'{download_folder}/{container_name}/{blob_path}'
        if remove_spaces_from_download_path:
            download_location = download_location.replace(' ', '-')
        download_dir = os.path.dirname(download_location)
        path = Path(download_dir)
        path.mkdir(parents=True, exist_ok=True)
        logging.info(f'Created download path {download_location}')
        return download_location

    def list(self, directory='', storage_account_name=None, settings_path=None):
        container_name = self._get_container_name(directory)
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        container_client = data_lake_client.get_container_client(container_name)
        blob_path = self.get_blob_path(directory)
        blobs = container_client.list_blobs(name_starts_with=blob_path)
        return blobs

    def get_file_info(self, file_path, storage_account_name=None, settings_path=None):
        container_name = self._get_container_name(file_path)
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        file_path = self._update_path_from_container_name(container_name, file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=file_path)
        blob_info = blob_client.get_blob_properties()
        return blob_info

    def delete_file(self, file_path, storage_account_name=None, settings_path=None):
        container_name = self._get_container_name(file_path)
        data_lake_client = self._get_storage_account_client(storage_account_name, settings_path)
        file_path = self._update_path_from_container_name(container_name, file_path)
        blob_client = data_lake_client.get_blob_client(container=container_name, blob=file_path)
        delete_results = blob_client.delete_blob()
        return delete_results

    def get_blob_path(self, directory):
        container_name = self._get_container_name(directory)
        path = directory[len(container_name) + 1:]
        blob_path = path
        return blob_path

    @staticmethod
    def _get_container_name(upload_path, container_name='default'):
        container_name = AzureStorageAccount._get_container_name_from_env(container_name)
        container_name = AzureStorageAccount._get_container_name_from_string(upload_path, container_name)
        max_container_name_length = 63
        if len(container_name) > max_container_name_length:
            logging.warning('The container name may be too long, look for errors from Azure')
        return container_name

    @staticmethod
    def _update_path_from_container_name(container_name, upload_path):
        if upload_path.startswith(f'{container_name}:'):
            upload_path = upload_path[len(f'{container_name}:'):]
        if upload_path.startswith(f'{container_name}/'):
            upload_path = upload_path[len(f'{container_name}/'):]
        return upload_path

    @staticmethod
    def _get_container_name_from_env(container_name, container_env_key='DataLakeContainerName'):
        if container_env_key in os.environ:
            container_regex = r'(^[a-z0-9]+)'
            container_name_raw = os.environ[container_env_key]
            matches = re.findall(container_regex, container_name_raw)
            container_name_raw_is_valid = True if len(matches) == 1 else False
            if container_name_raw_is_valid:
                if len(container_name_raw) == len(matches[0]):
                    container_name = container_name_raw
        return container_name

    @staticmethod
    def _get_container_name_from_string(string, container_name=None):
        container_delimiter_regex = r'(\A[a-z]+[\-a-z|\d]+)(?::|/)'
        matches = re.findall(container_delimiter_regex, string)
        upload_path_has_container_name = True if len(matches) == 1 else False
        if upload_path_has_container_name:
            container_name = matches[0]
        return container_name

    def _get_storage_account_client(self, storage_account_name, settings_path):
        try:
            logging.info(f'Getting Storage Account Client for {storage_account_name}')
            storage_account_name = self.get_storage_account_name(default_value=storage_account_name)
            settings_path = self.get_settings_path(default_value=settings_path)
            service_principal = ServicePrincipal(settings_file_path=settings_path)
            service_principal.load_service_principal_into_environment()
            credential = DefaultAzureCredential()
            account_url = f"https://{storage_account_name}.blob.core.windows.net/"
            client = BlobServiceClient(account_url=account_url, credential=credential)
            return client
        except Exception as e:
            logging.exception('Unable to get data lake client {')
            raise Exception(e)
