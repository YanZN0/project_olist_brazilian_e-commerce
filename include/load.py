import pandas as pd
import os



def extract_csv(caminho):
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

def rename_colums(dict_dfs):

    if 'olist_customers_dataset' in dict_dfs:
        dict_dfs['olist_customers_dataset'] = dict_dfs['olist_customers_dataset'].rename(columns={
            '"customer_id"': 'customer_id',
            '"customer_zip_code_prefix"': 'customer_zip_code',
            '"customer_city"':  'customer_city',
            '"customer_state"': 'customer_state',
        })
        dict_dfs['olist_customers_dataset'] = dict_dfs['olist_customers_dataset'].drop(columns=['"customer_unique_id"'])
    




