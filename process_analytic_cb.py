import matplotlib.pyplot as plt
from datetime import datetime
import pandas as pd
import requests


class CampeonatoBrasileiro():

    def run(self, dataset):
        df = self.get_ultima_rodada(dataset)
        df = self.get_campeao(df)
        df = self.get_rebaixamento(df)
        return df

    def get_ultima_rodada(self, dataset):
        """
        Obtém a última rodada de cada temporada e adiciona ao dataframe.
        
        Args:
            df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
        
        Returns:
            pandas.DataFrame: DataFrame com a coluna 'ultima_rodada' adicionada.
        """
        df_max_rodada = dataset[['temporada', 'rodada']].drop_duplicates().groupby('temporada', as_index=False)['rodada'].max()
        df_max_rodada.rename(columns={'rodada': 'ultima_rodada'}, inplace=True)

        return pd.merge(dataset, df_max_rodada, how='left', on='temporada')

    def get_campeao(self, dataset):
        """
        Obtém as informações do time campeão de cada temporada e adiciona ao dataframe.
        
        Args:
            df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
        
        Returns:
            pandas.DataFrame: DataFrame com a coluna 'campeao' adicionada.
        """
        df_campeao = dataset[dataset['rodada'] == dataset['ultima_rodada']].query('posicao == 1')[['time', 'temporada', 'posicao']]
        df_campeao.rename(columns={'posicao': 'campeao'}, inplace=True)
        df_campeao = df_campeao[df_campeao['temporada'] != datetime.now().year]

        df_campeao.groupby('time')['campeao'].sum().sort_values(ascending=False)

        df_prt = pd.merge(dataset, df_campeao, how='left', on=['time', 'temporada'])
        df_prt.fillna({'campeao': 0}, inplace=True)

        return df_prt

    def get_rebaixamento(self, dataset):
        """
        Obtém as informações dos times rebaixados em cada temporada e adiciona ao dataframe.
        
        Args:
            df (pandas.DataFrame): DataFrame contendo os dados do campeonato.
        
        Returns:
            pandas.DataFrame: DataFrame com as colunas 'z4_1_anos', 'z4_2_anos' e 'z4_3_anos' adicionadas.
        """
        for i in range(1, 4):
            df_z4 = dataset[dataset['rodada'] == dataset['ultima_rodada']].groupby('temporada').tail(4)[['time', 'temporada']]
            df_z4['temporada'] = df_z4['temporada'] + (1 + i)
            df_z4[f'z4_{i}_anos'] = 1
            df_prt = pd.merge(dataset, df_z4, how='left', on=['time', 'temporada'])
            df_prt.fillna({f'z4_{i}_anos': 0}, inplace=True)
        
        return df_prt


if __name__ == '__main__':
    response = requests.get('https://fnt-campeonato-bra.azurewebsites.net/api/getwf?code=Cw5nnWHZlyyzpHzgIUiX48NiEX3z-rpr0tJpSAxXYDokAzFu4welWQ==')
    df = pd.DataFrame(response.json())

    cb = CampeonatoBrasileiro()
    df = cb.run(df)
    
    dataset = df[['temporada', 'rodada']].drop_duplicates()
    df_ultima_rodada = df[df['rodada'] == df['ultima_rodada']]

    # Primeira Validação
    df_check = dataset[['temporada', 'rodada']].drop_duplicates().groupby('temporada', as_index=False)['rodada'].count()

    anos_completos = pd.DataFrame({'temporada': range(2003, datetime.now().year+1)})

    df_check = pd.merge(anos_completos, df_check, on='temporada', how='left')
    df_check['rodada'].fillna(0, inplace=True)

    # Segunda validação
    df_check_02 = dataset[['temporada', 'time']].drop_duplicates().groupby('temporada', as_index=False)['time'].count()

    # Times
    times = df_ultima_rodada.time.drop_duplicates().sort_values().to_list()

    # Counts dos times
    counts = df_ultima_rodada['time'].value_counts().sort_values(ascending=True).to_frame().reset_index().rename(columns={'count':'qtd'}).copy()

    # Estatísticas dos campeões por temporada
    estatisticas_campeoes = df_ultima_rodada.query('campeao == 1').groupby('temporada').agg({'pontos': ['max'], 'gols': 'sum'})

    # Extraindo os valores para o gráfico
    temporadas = estatisticas_campeoes.index
    pontos_maximos = estatisticas_campeoes[('pontos', 'max')]
    gols_totais = estatisticas_campeoes[('gols', 'sum')]

    # Salve
