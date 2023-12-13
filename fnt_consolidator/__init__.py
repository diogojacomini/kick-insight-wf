from azure.functions import HttpRequest, HttpResponse
import logging
from shared.connection_az import AzureStore
from pandas import concat 
from datetime import datetime
from shared.functions import get_current_date

def main(req: HttpRequest) -> HttpResponse:
    az = AzureStore()
    filename_control = f'ml_{get_current_date()}_prlm.json'
    parameters = az.read_lake(container_name='controle', filename=filename_control)

    # input
    files_input = az.list_files(container_name='campeonato-brasileiro', layer='results')
    dataset = concat(files_input)
    
    # save
    az.upload_lake(dataset, f'curated/tb_prediction_all_{datetime.now().year}.csv', parameters.get('container_name'))

    return HttpResponse("OK")