import logging
from azure.functions import HttpRequest, HttpResponse
from shared.connection_az import AzureStore
from datetime import datetime
import random
from shared.functions import get_xy, get_current_date
from sklearn.ensemble import RandomForestClassifier
from time import sleep
from conf.variables import CONNECTION_STRING
from azure.storage.blob import BlobServiceClient

def main(req: HttpRequest) -> HttpResponse:
    az = AzureStore()
    filename_control = f'ml_{get_current_date()}_prlm.json'
    parameters = az.read_lake(container_name='controle', filename=filename_control)

    if 'ml' in parameters.get('exec'):        
        data = az.read_lake(parameters.get('container_name'), 'curated/campeonato_brasileiro.csv')

        current_season_data = data.query(f"temporada == {datetime.now().year}")
        dataset = data.query(f"temporada != {datetime.now().year}")

        # Divindo os dados de treino e teste por temporada aleatoriamente
        temporadas = dataset.temporada.drop_duplicates().to_list()
        random.seed(127)
        random.shuffle(temporadas)

        seasons_test = temporadas[:int(0.3 * len(temporadas))]  # 30% teste
        seasons_train = temporadas[len(seasons_test):]  # 70% treino

        # get_xy - Separa o DataFrame em matrizes X e y com base nos valores de temporada, features e target especificados.
        X_train, y_train = get_xy(data, seasons_train, parameters.get('features'), parameters.get('target'))
        X_test, y_test = get_xy(data, seasons_test, parameters.get('features'), parameters.get('target'))

        # Criar o modelo Random Forest com os melhores parâmetros
        best_model = RandomForestClassifier(**parameters.get('ml_params'))
        best_model.fit(X_train, y_train)
        
        # Conectar ao Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client('campeonato-brasileiro')
        sleep(40)

        # Salvar o modelo
        with open('model.pkl', 'rb') as model_file:
            container_client.upload_blob(name='model.pkl', data=model_file)
        
        az.upload_lake(X_test, f'ml/tb_x_test_{datetime.now().year}.csv', parameters.get('container_name'))
        az.upload_lake(y_test, f'ml/tb_y_test_{datetime.now().year}.csv', parameters.get('container_name'))
    
    return HttpResponse("OK")

