import pandas as pd
from datetime import datetime

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
            dataset = pd.merge(dataset, df_z4, how='left', on=['time', 'temporada'])
            dataset.fillna({f'z4_{i}_anos': 0}, inplace=True)
        
        return dataset
