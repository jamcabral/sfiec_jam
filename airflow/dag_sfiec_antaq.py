from nis import cat
from airflow import DAG 
from datetime import datetime
from airflow.operators.python import PythonOperator
from pendulum import time
from airflow.operators.email_operator import EmailOperator

# Quem são operadores:
# Representam as tasks de cada DAG. como no exemplo operator Python, mas poderiamos verificar se os arquivos foram baixados atraves do operator bash.

# Os sensores são para monitorar as taks internas ou externas para poder executar a proxima task.

#Hooks provem interface para conexão com o mundo externo exemplo AWS, Azure, DataBricks


enviar_email = EmailOperator(
        task_id='enviar_email',
        to='to@gmail.com',
        subject='Airflow Alert',
        html_content=""" <h3>A Dag SFIEC ANTAQ Falhou</h3> """,
        dag=dag
)


with DAG('sfiec', 
dag_id = 'exemplo_dag',
start_date = datetime(2021,4,1), 
schedule_interval = time(7,7,7), catchup=False,
on_failure_callback=enviar_email) as dag:
    
    capturar_dados = PythonOperator(
        task_id = 'capturar_dados',
        python_callable= capturar_dados
    )

    copiar_dados_prata = PythonOperator(
        task_id = 'copiar_dados_prata',
        python_callable= copiar_dados_prata
    )

    unzip_dados = PythonOperator(
        task_id = 'unzip_dados',
        python_callable= unzip_dados
    )

    delete_dados = PythonOperator(
        task_id = 'delete_dados',
        python_callable= delete_dados
    )

    transformacao_dados = PythonOperator(
        task_id = 'transformacao_dados',
        python_callable= transformacao_dados
    )

    copiar_dados_ouro = PythonOperator(
        task_id = 'copiar_dados_ouro',
        python_callable= copiar_dados_ouro
    )
    

    capturar_dados >> copiar_dados_prata >> unzip_dados >> delete_dados >> transformacao_dados >> copiar_dados_ouro