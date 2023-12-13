import logging
from azure.functions import HttpRequest, HttpResponse
from shared.connection_az import AzureStore
from shared.camp_bra import CampeonatoBrasileiro
import pandas as pd
from datetime import datetime

def main(req: HttpRequest) -> HttpResponse:
    az = AzureStore()
    cb = CampeonatoBrasileiro()
    container_name = 'campeonato-brasileiro'

    # Process
    df = az.read_lake(container_name, 'curated/campeonato_brasileiro.csv')
    df = cb.run(df)

    # Create analytics table
    df_ultima_rodada = df[df['rodada'] == df['ultima_rodada']]

    # Primeira Validação
    df_check = df[['temporada', 'rodada']].drop_duplicates().groupby('temporada', as_index=False)['rodada'].count()

    anos_completos = pd.DataFrame({'temporada': range(2003, datetime.now().year+1)})

    df_check = pd.merge(anos_completos, df_check, on='temporada', how='left')
    df_check['rodada'].fillna(0, inplace=True)

    # Segunda validação
    df_check_02 = df[['temporada', 'time']].drop_duplicates().groupby('temporada', as_index=False)['time'].count()

    # Counts dos times
    counts = df_ultima_rodada['time'].value_counts().sort_values(ascending=True).to_frame().reset_index().rename(columns={'count':'qtd'}).copy()

    # Estatísticas dos campeões por temporada
    estatisticas_campeoes = df_ultima_rodada.query('campeao == 1').groupby('temporada').agg({'pontos': ['max'], 'gols': 'sum'})

    # Dados agrupados
    df_grouped = df_ultima_rodada[df_ultima_rodada['temporada'] != 2023].groupby(['time', 'temporada']).agg({'pontos': 'sum'}).reset_index()

    # Load
    az.upload_lake(df, 'analytics/tb_sys_campeonato_completo.csv', container_name)
    az.upload_lake(df_ultima_rodada, 'analytics/tb_sys_ultima_rodada.csv', container_name)
    az.upload_lake(df_check, 'analytics/tb_sys_validacao_rodada.csv', container_name)
    az.upload_lake(df_check_02, 'analytics/tb_sys_validacao_time.csv', container_name)
    az.upload_lake(counts, 'analytics/tb_sys_counts.csv', container_name)
    az.upload_lake(estatisticas_campeoes, 'analytics/tb_sys_estatisticas_campeoes.csv', container_name)
    az.upload_lake(df_grouped, 'analytics/tb_sys_grouped.csv', container_name)

    return HttpResponse("OK")
