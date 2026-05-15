from airflow import DAG
from airflow.operators.empty import EmptyOperator
from datetime import datetime

# 1. Define the DAG settings (Schedule @hourly, etc.)
with DAG(
    dag_id='news_pipeline_demo',
    start_date=datetime(2024, 1, 1),
    schedule_interval='@hourly',
    catchup=False,
    tags=['project_emsi']
) as dag:

    # 2. Create the boxes (Tasks)
    start = EmptyOperator(task_id='start')
    
    scrape = EmptyOperator(task_id='scrape_articles')
    
    bronze_silver = EmptyOperator(task_id='clean_bronze_to_silver')
    
    gold = EmptyOperator(task_id='aggregate_to_gold')
    
    warehouse = EmptyOperator(task_id='load_to_postgres_warehouse')
    
    end = EmptyOperator(task_id='end')

    # 3. Connect the boxes with arrows (Dependencies)
    start >> scrape >> bronze_silver >> gold >> warehouse >> end