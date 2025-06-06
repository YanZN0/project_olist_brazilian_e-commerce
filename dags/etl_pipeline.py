from airflow.decorators import task, dag
from include.extract import extracting_and_reading_csv
from include.transform import cleaning_and_transformation_raw_data
from datetime import datetime


@dag(
        dag_id="etl_pipeline",
        description="De arquivos CSV como fonte de dados de tabelas brutas, para tabelas mais claras e desenvolvidas, carregadas em um banco de dados PostgreSQL",
        schedule_interval = None,
        start_date=datetime(2025, 5, 31),
        catchup=False
)

def pipeline_etl():
    
    @task(task_id='extrair_dados_CSV')
    def task_extracting_and_reading_csv():
        return extracting_and_reading_csv() 
    
    @task(task_id='transformar_e_limpar_dados_brutos')
    def task_cleaning_and_transformation_raw_data(dict_df):
        return cleaning_and_transformation_raw_data(dict_df)
    
    t1 = task_extracting_and_reading_csv()
    t2 = task_cleaning_and_transformation_raw_data(t1)

    t1 >> t2
    
pipeline_etl()


    