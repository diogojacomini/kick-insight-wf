from shared.variables import CONNECTION_STRING
from shared.camp_bra import CampeonatoBrasileiro
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging
import json
from pandas import read_csv
import io

class AzureStore():

    def __init__(self) -> None:
        self.CONNECTION_STRING = CONNECTION_STRING
        self.connect()

    def connect(self):
        self.client = BlobServiceClient.from_connection_string(self.CONNECTION_STRING)

    def get_container(self, container_name):
        return self.client.get_container_client(container_name)

    def get_file(self, container_name, filename):
        return self.client.get_blob_client(container=container_name, blob=filename)

    def upload_lake(self, data, filename, container_name, overwrite=True):
        """
        Faz o upload de dados para um contêiner no Azure Blob Storage.

        Args:
            data: Os dados a serem carregados, que podem ser um DataFrame do Pandas ou um dicionário.
            filename (str): O nome do arquivo no qual os dados serão salvos no Blob Storage.
            container_name (str): O nome do contêiner no Azure Blob Storage.
            overwrite (bool, optional): Se True, o arquivo será substituído se já existir no Blob Storage. Padrão é True.

        Raises:
            ValueError: Se o tipo de arquivo não é suportado (apenas '.csv' e '.json' são suportados).
        
        Exemplo: upload_lake(dataset, 'example.csv', 'my-container')
        """
        logging.info(f"upload_lake: Uploading {filename} to container {container_name}, overwrite={overwrite}")
        blob_client = self.get_container(container_name).get_blob_client(filename)

        if filename.endswith('.csv'):
            file_content = data.to_csv(index=False, encoding='utf-8').encode('utf-8')

        elif filename.endswith('.json'):
            file_content = json.dumps(data)
        elif filename.endswith('.ipynb'):
            file_content = data
        else:
            logging.info("upload_lake: !Warning! tipo do arquivo .{0} ".format(filename.split(".")[-1]))
            raise ValueError("Unsupported file type. Only '.csv' and '.json' are supported.")

        blob_client.upload_blob(file_content, overwrite=overwrite)
        logging.info("upload_lake: Upload successful")


    def read_lake(self, container_name, filename):
        """
        Lê um arquivo de um contêiner no Azure Blob Storage.

        Args:
            container_name (str): O nome do contêiner no Azure Blob Storage.
            filename (str): O nome do arquivo a ser lido.

        Returns:
            DataFrame or dict or None: Os dados lidos do arquivo, dependendo do tipo do arquivo.
                                    Retorna None em caso de erro.
        """
        file_client = self.get_file(container_name, filename)
        try:
            data = file_client.download_blob()
            content = data.readall()

            if filename.endswith('.csv'):
                return read_csv(io.StringIO(content.decode('utf-8')))

            elif filename.endswith('.json'):
                return json.loads(content)
            
            elif filename.endswith('.ipynb'):
                logging.warning("read_lake:ipynb")
                return content#data.readall()#.decode('utf-8') 
            else:
                logging.warning("read_lake: Warning! Tipo de arquivo diferente de csv ou json.")
                logging.warning("Se novo tipo de arquivo for adicionado, é preciso atualizar a function: read_lake")

        except Exception as error_read_lake:
            logging.error('read_lake: error_read_lake: {0}'.format(str(error_read_lake)))
            return None