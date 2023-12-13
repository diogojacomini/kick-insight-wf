import logging
from azure.functions import HttpRequest, HttpResponse
from shared.connection_az import AzureStore
import pandas as pd
from datetime import datetime
from shared.functions import get_current_date
from conf.variables import CONNECTION_STRING
import pickle
from azure.storage.blob import BlobServiceClient


def main(req: HttpRequest) -> HttpResponse:
    az = AzureStore()
    filename_control = f'ml_{get_current_date()}_prlm.json'
    parameters = az.read_lake(container_name='controle', filename=filename_control)

    if 'pr' in parameters.get('exec'):
        logging.warning("PR")  
        # Conectar ao Azure Blob Storage
        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STRING)
        container_client = blob_service_client.get_container_client('campeonato-brasileiro')

        # Recuperar o modelo
        blob_client = container_client.get_blob_client('model.pkl')
        model_blob = blob_client.download_blob()
        model_content = model_blob.readall()

        # Carregar o modelo
        model = pickle.loads(model_content)
        
        df = az.read_lake(parameters.get('container_name'), 'curated/campeonato_brasileiro.csv')

        temp_ctr = df.query(f"temporada == {datetime.now().year}")
        max_round = temp_ctr['rodada'].max()
        temp_ctr = temp_ctr.query(f"rodada == {max_round}")
        
        # Predict
        df_result = temp_ctr.copy()
        pred = model.predict(temp_ctr[parameters.get('features')]).copy()
        df_result['posision_ml'] = pred

        pred_proba = model.predict_proba(temp_ctr[parameters.get('features')]).copy()
        df_proba = pd.DataFrame(pred_proba, columns = range(0, pred_proba.shape[1]))

        df_pred = pd.merge(temp_ctr.reset_index(drop=True), df_proba, left_index=True, right_index=True)
        df_pred['nao_campeao'] = df_pred.iloc[:, 13:-1].apply(pd.to_numeric, errors='coerce').sum(axis=1)

        df_pred['campeao_normalizado'] = (df_pred[0] / df_pred[0].sum()) * 100
        df_pred.rename(columns={0:'campeao'}, inplace=True)

        df_pred = df_pred[['time', 'posicao', 'rodada', 'temporada', 'campeao', 'nao_campeao', 'campeao_normalizado']]

        # Load
        az.upload_lake(df_pred, f'results/tb_prediction_{max_round}_{get_current_date()}.csv', parameters.get('container_name'))

    return HttpResponse("OK")

