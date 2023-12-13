import logging
from azure.functions import HttpRequest, HttpResponse
from shared.connection_az import AzureStore
import pandas as pd
from shared.functions import get_current_date
from sklearn.metrics import accuracy_score, confusion_matrix, classification_report
import numpy as np
from io import StringIO
from datetime import datetime
from conf.variables import CONNECTION_STRING
import pickle
from azure.storage.blob import BlobServiceClient


def main(req: HttpRequest) -> HttpResponse:
    az = AzureStore()
    filename_control = f'ml_{get_current_date()}_prlm.json'
    parameters = az.read_lake(container_name='controle', filename=filename_control)

    if 'ml' in parameters.get('exec'):        
        # Conectar ao Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client('campeonato-brasileiro')

        # Recuperar o modelo
        blob_client = container_client.get_blob_client('model.pkl')
        model_blob = blob_client.download_blob()
        model_content = model_blob.readall()

        # Carregar o modelo
        model = pickle.loads(model_content)
        
        X_test = az.read_lake(container_name=parameters.get('container_name'), filename=f'ml/tb_x_test_{datetime.now().year}.csv')
        y_test = az.read_lake(container_name=parameters.get('container_name'), filename=f'ml/tb_y_test_{datetime.now().year}.csv')

        # Fazer as previsões
        predictions = model.predict(X_test)

        # Calcular a acurácia do modelo
        accuracy = accuracy_score(y_test, predictions)
        logging.warning(f'Acurácia do modelo: {accuracy}')
        #df_accuracy = pd.DataFrame({'accuracy': accuracy})

        df_report = pd.read_fwf(StringIO(classification_report(y_test, predictions)), index_col=0)

        # Matrix de confusao
        matriz_confusao = confusion_matrix(y_test, predictions)
        total_por_classe = matriz_confusao.sum(axis=1)

        divisor = np.where(total_por_classe[:, np.newaxis] == 0, 1, total_por_classe[:, np.newaxis])
        if (total_por_classe == 0).any():
            # Tomar uma ação adequada, como atribuir um valor padrão
            matriz_confusao_percentual = np.where(divisor == 0, 0, matriz_confusao / divisor)
        else:
            matriz_confusao_percentual = matriz_confusao / total_por_classe[:, np.newaxis]

        df_confusao = pd.DataFrame(matriz_confusao)
        df_confusao_percentual = pd.DataFrame(matriz_confusao_percentual)

        importancias = model.feature_importances_
        nomes_recursos = np.array(parameters.get('features'))
        df_importance = pd.DataFrame({'feature': nomes_recursos, 'importance': importancias})
        df_importance = df_importance.sort_values(by='importance', ascending=False)

        # LOAD
        az.upload_lake(df_report, f'testing/report_{get_current_date()}.csv', parameters.get('container_name'))
        az.upload_lake(df_confusao, f'testing/tb_confusao_{get_current_date()}.csv', parameters.get('container_name'))
        az.upload_lake(df_confusao_percentual, f'testing/tb_confusao_percentual_{get_current_date()}.csv', parameters.get('container_name'))
        az.upload_lake(df_importance, f'testing/tb_importance_{get_current_date()}.csv', parameters.get('container_name'))
        #az.upload_lake(df_accuracy, f'testing/tb_accuracy_{get_current_date()}.csv', parameters.get('container_name'))

    return HttpResponse("OK")

