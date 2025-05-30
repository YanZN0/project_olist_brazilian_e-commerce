import pandas as pd
import os



def extracting_and_reading_csv(caminho):
    '''
    Essa função tem como objetivo ler arquivos CSV do caminho recebido,
    trabalha com multiplos CSVs fazendo uma separação individual através de um dict de dataframes.
    '''
    dataframes = {}             
    for arquivos in os.listdir(caminho):
        if arquivos.endswith('.csv'):
            nome = arquivos.replace('.csv', '') 
            caminho_completo = os.path.join(caminho, arquivos)
            extract = pd.read_csv(caminho_completo)
            dataframes[nome] = extract
    return dataframes





