from conf.variables import get_vars
from datetime import datetime

def get_xy(dataset, values, features, target):
    """
    Separa o DataFrame em matrizes X e y com base nos valores de temporada, recursos (features) e alvo (target) especificados.
    
    Args:
        dataset (pandas.DataFrame): DataFrame contendo os dados.
        values (list): Lista dos valores de temporada a serem considerados.
        features (list): Lista das colunas de recursos (features) a serem incluídas na matriz X.
        target (str): Nome da coluna alvo (target) a ser incluída na matriz y.
    
    Returns:
        tuple: Tupla contendo as matrizes X e y.
    """
    mask = dataset['temporada'].isin(values)
    X = dataset.loc[mask, features].copy()
    y = dataset.loc[mask, target].copy()
    return X, y

def check_prlm_sistema(value, parametro):
    """
    Verifica se o parâmetro 'sistema' é válido.

    Args:
        sistema (str): O sistema a ser verificado.

    Returns:
        bool: True se o sistema for válido, caso contrário, retorna uma mensagem de erro.
    """
    if not value:
        return False, f"check_prlm_sistema: Parâmetro '{parametro}' é obrigatório!"

    if value not in get_vars(parametro):
        return False, f"check_prlm_sistema: Parâmetro '{parametro}' é inválido. Favor " \
                       "configurar com um dos seguintes valores: {0}".format(', '.join(get_vars(parametro)))

    return True, None

def get_current_date() -> int:
    """Retorna a data atual no formato 'AAAAMMDD'."""
    return datetime.now().strftime("%Y%m%d")