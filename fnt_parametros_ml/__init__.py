import logging
from azure.functions import HttpRequest, HttpResponse
from shared.functions import check_prlm_sistema, get_current_date
from conf.error_code import get_error
from conf.parameters import get_params
from shared.connection_az import AzureStore

def main(req: HttpRequest) -> HttpResponse:
    sistema = req.headers.get('sistema'.upper())
    exections = req.headers.get('exec'.upper())
    logging.warning(sistema)

    az = AzureStore()

    valid, message = check_prlm_sistema(sistema, 'SISTEMA')
    if valid is not True:
        return HttpResponse(message, status_code=get_error('er_parameters').get('cod'))
    valid, message = check_prlm_sistema(exections, 'EXEC')
    if valid is not True:
        return HttpResponse(message, status_code=get_error('er_parameters').get('cod'))
    
    params = {'exec': exections.split(',')}
    params.update(get_params(sistema))

    try:
        az.upload_lake(
            data=params,
            filename=f'ml_{get_current_date()}_prlm.json',
            container_name='controle',
            overwrite=True
        )
        return HttpResponse("OK")
    
    except Exception as error_fun:
        logging.error(f"main (error_fun): Erro ao fazer upload do JSON de controle: {str(error_fun)}")
        return HttpResponse(status_code=get_error('status_incompleto_func').get('cod'))
