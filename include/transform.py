import pandas as pd
import numpy as np
import pyarrow as pa

def cleaning_and_transformation_raw_data(dict_dfs):
    """
    Essa função recebe nossas tabelas CSVs com dados brutos, em formato de um dicionário de Dataframes.
    Executamos as principais transformações e limpezas dos dados brutos, com o objetivo de gerar
    tabelas com clareza e fácil interpretação visando fornecer uma estrutura aprimorada para ánalises.
    """
# Abaixo começa as transformações em nomes de colunas, essas transformações foram feitas com o objetivo de gerar mais clareza, interpretação e nomes que geram mais informação sobre os dados que a coluna contém.

    if 'olist_customers_dataset' in dict_dfs:
        dict_dfs['olist_customers_dataset'] = dict_dfs['olist_customers_dataset'].rename(columns={

            '"customer_id"':                'customer_id',
            '"customer_zip_code_prefix"':   'customer_zip_code',
            '"customer_city"':              'customer_city',
            '"customer_state"':             'customer_state'
        })

        # Coluna excluida "customer_unique_id", a tabela contém presente duas colunas com o mesmo objetivo de conter o ID único do cliente, a exclusão foi feita para melhorar a clareza da tabela e retirar valores considerados inválidos.
        dict_dfs['olist_customers_dataset'] = dict_dfs['olist_customers_dataset'].drop(columns=["customer_unique_id"])

    if 'olist_geolocation_dataset' in dict_dfs:
        dict_dfs['olist_geolocation_dataset'] = dict_dfs['olist_geolocation_dataset'].rename(columns={

            '"geolocation_zip_code_prefix"':    'geolocation_zip_code_prefix',
            '"geolocation_lat"':                'geolocation_latitude',
            '"geolocation_lng"':                'geolocation_longitude',
            '"geolocation_city"':               'geolocation_city',
            '"geolocation_state"':              'geolocation_state'
        })

    if 'olist_order_payments_dataset' in dict_dfs:
        dict_dfs['olist_order_payments_dataset'] = dict_dfs['olist_order_payments_dataset'].rename(columns={
            
            '"order_id"':                   'order_id',
            '"payment_sequential"':         'payment_sequential',
            '"payment_type"':               'payment_type',
            '"payment_installments"':       'payment_installments',
            '"payment_value"':              'payment_value'   
        })

    if 'olist_order_reviews_dataset' in dict_dfs:
        dict_dfs['olist_order_reviews_dataset'] = dict_dfs['olist_order_reviews_dataset'].rename(columns={
            
            '"review_id"':                          'review_id',
            '"order_id"':                           'order_id',
            '"review_score"':                       'review_score',
            '"review_comment_title"':               'review_comment_tittle',
            '"review_comment_message"':             'review_message',
            '"review_creation_date"':               'review_date',
            '"review_answer_timestamp"':            'review_answer_timestamp'
        })

    if 'olist_orders_dataset' in dict_dfs:
        dict_dfs['olist_orders_dataset'] = dict_dfs['olist_orders_dataset'].rename(columns={
            
            '"order_id"':                               'order_id',
            '"customer_id"':                            'customer_id',
            '"order_status"':                           'order_status',
            '"order_purchase_timestamp"':               'order_purchase_timestamp',
            '"order_approved_at"':                      'order_approved_at',
            '"order_delivered_carrier_date"':           'order_delivered_carrier_date',
            '"order_delivered_customer_date"':          'order_delivered_customer_date',
            '"order_estimated_delivery_date"':          'order_estimated_delivery_date'
        })

    if 'olist_products_dataset' in dict_dfs:
        dict_dfs['olist_products_dataset'] = dict_dfs['olist_products_dataset'].rename(columns={
            
            '"product_id"':                             'product_id',
            '"product_category_name"':                  'product_category_name',
            '"product_weight_g"':                       'product_weight_g',
            '"product_length_cm"':                      'product_length_cm',
            '"product_height_cm"':                      'product_height_cm',
            '"product_width_cm"':                       'product_width_cm'
        })

        # Tabelas excluidas através do comando .drop(), exclusão foram feitas com o objetivo de excluir colunas que não agregam valor a tabela ou valor a ánalises avançadas.
        dict_dfs['olist_products_dataset'] = dict_dfs['olist_products_dataset'].drop(columns=["product_description_lenght", "product_name_lenght", "product_photos_qty"])

    if 'olist_sellers_dataset' in dict_dfs:
        dict_dfs['olist_sellers_dataset'] = dict_dfs['olist_sellers_dataset'].rename(columns={
            
            '"seller_id"':                     'seller_id',
            '"seller_zip_code_prefix"':        'seller_zip_code',
            '"seller_city"':                   'seller_city',
            '"seller_state"':                  'seller_state'
        })

    if 'product_category_name_translation' in dict_dfs:
        dict_dfs['product_category_name_translation'] = dict_dfs['product_category_name_translation'].rename(columns={
            
            '"product_category_name"': 'product_category_name'
        })

        # Coluna excluida, tomei está decisão pois os dados presentes em outras tabelas apenas usam "product_category_name", a outra coluna em sí não agregava com dados importantes nenhuma tabela.
        dict_dfs['product_category_name_translation'] = dict_dfs['product_category_name_translation'].drop(columns=["product_category_name_english"])

    for name, df in dict_dfs.items():   # Obtendo todos os Dataframes do dicionário através de um loop FOR com chave = name [nome do dataframe dentro do dicionário]; valor = Dataframe [Tabelas CSV sendo Dataframes]. Podendo trabalhar com eles tanto de forma invidual ou de forma total.
        
        for col in df.columns:  # Aqui obtenho todas as colunas de todos os Dataframes, sendo as tabelas que estamos transformando.

            if df[col].dtypes == 'float64':     # Esse IF foi criado para ser um filtro, nesse código de filtro tenho objetivo de lidar com valores NaN mas em colunas do tipo float. Com o comando fillna() que sobrescreve valores NaN, eu fiz com que os dados inválidos que fossem NaN dentro da coluna virassem o valor 0.               
                df[col] = df[col].fillna(0) 
                

# Esses dois IFs são um tipo de filtro, eles lindam com valores diferentes, acontece que primeiro eu fiz com que todos os valores NaN fossem "MISSING_VALUE", mas colunas de tipo float ou int gera erro e a transformação é abortada. A solução encontrada foram os IFs trabalhando com tipos diferentes de colunas que, cada valor inválido e sobrescrito para valor que a coluna pode suportar.


            if df[col].dtypes == 'object':
                df[col] = df[col].fillna('MISSING_VALUE')   # Esse outro filtro foi para dados inválidos NaN mas que são strings, para lidar com esses valores inválidos e não excluir linhas que podem ser importantes, minha solução foi com o comando fillna() novamente sobrescrever esses valores NaN para "MISSING_VALUE".
            
            
        # Inplace=True. Essa solução evita que eu tenha que criar dicionários vázios para armazenar novas atualizações do dataframe
        
        invalid_values = df.apply(lambda x: x.replace('""', '', inplace=True))   # Removendo "aspas" de dados das colunas, evitando que dados inválidos no futuro sejam carregados no banco de dados.
        

# Logo após todas as transformações com objetivos de deixar tabelas mais claras, interpretáveis e tabelas limpas, eu tive a decisão de essas tabelas depois das transformações dos dados salvar em arquivos .parquet proporcionando melhor desempenho ao processamento de grandes conjuntos de dados, otimização, e redução de custo de armazenamento.

        if name == 'olist_customers_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

# Para fazer o salvamento de cada tabela sendo individualmente e não sendo o dicionário de dataframes inteiro, eu utilizei comandos IF. Para cada nome de Dataframe (Tabela), salvar em .parquet na pasta transformed_dataset.


        if name == 'olist_geolocation_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'olist_order_payments_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'olist_order_reviews_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'olist_orders_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'olist_products_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'olist_sellers_dataset' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)

        if name == 'product_category_name_translation' in dict_dfs:
            df.to_parquet(f"/usr/local/airflow/include/transformed_datasets/{name}.parquet", index=False)


    return dict_dfs





    
    



        

