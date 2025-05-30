import pandas as pd
import numpy as np

def rename_colums(dict_dfs):
    """
    Essa função contém o objetivo de receber nossas tabelas CSV, em formato dataframe,
    e renomear colunas para nomes geram mais clareza, interpretação.
    """

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

    return dict_dfs

def correcting_NaN_values(dict_dfs):

    for name, df in dict_dfs.items():
        
        values_NaN = df.replace(np.nan, 'INVALID_VALUE', inplace=True)

    return dict_dfs

    
    



        

