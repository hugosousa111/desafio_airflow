import sqlite3
import pandas as pd
from pandasql import sqldf

from datetime import datetime, timedelta
from textwrap import dedent
from airflow.operators.python import PythonOperator
from airflow import DAG
from airflow.models import Variable

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


## Do not change the code below this line ---------------------!!#
def export_final_answer():
    import base64

    # Import count
    with open('files_data_pipeline/count.txt') as f:
        count = f.readlines()[0]

    my_email = Variable.get("my_email")
    message = my_email+count
    message_bytes = message.encode('ascii')
    base64_bytes = base64.b64encode(message_bytes)
    base64_message = base64_bytes.decode('ascii')

    with open("files_data_pipeline/final_output.txt","w") as f:
        f.write(base64_message)
    return None
## Do not change the code above this line-----------------------##

def write_simple_log(name_method, message):
    """Write in file simple_log.log, the 
    method_name and the message sucess, 
    or, if fail, the error message."""
    with open('logs/simple_log.log', mode='a') as f:
        now = datetime.now()
        f.write('\n\n')
        f.write(f'\n{"-"*30}')
        f.write(f'\n{str(now)}')
        f.write(f'\n{name_method}')
        f.write(f'\n{message}')
        f.write(f'\n{"-"*30}')

def read_database_and_export_csv():
    """Read table 'Order', and write 
    a csv file 'files_data_pipeline/output_orders.csv'."""
    try: 
        conn = sqlite3.connect(
            "data/Northwind_small.sqlite",
            isolation_level=None,
            detect_types=sqlite3.PARSE_COLNAMES
            )
        df_order = pd.read_sql_query("SELECT * FROM 'Order';", conn)
        conn.close()

        df_order.to_csv('files_data_pipeline/output_orders.csv', index=False)

        message = 'success'
    except Exception as e:
        message = 'error: ' + str(e)
    finally:
        write_simple_log('read_database_and_export_csv', message)

def read_database_join_csv_and_export_txt():
    """Read table 'OrderDetail' and join 
    with file 'files_data_pipeline/output_orders.csv'. Calc 
    'Quantity'('ShipCity' = Rio de Janeiro). 
    Write in the file 'files_data_pipeline/count.txt'.

    In SQL: 
    SELECT sum(od.Quantity) AS total
    FROM "Order" o 
    LEFT JOIN OrderDetail od ON o.id = od.OrderId 
    WHERE o.ShipCity = 'Rio de Janeiro'
    GROUP BY o.ShipCity
    """
    try: 
        conn = sqlite3.connect(
            "data/Northwind_small.sqlite",
            isolation_level=None,
            detect_types=sqlite3.PARSE_COLNAMES
            )
        df_order_detail = pd.read_sql_query("SELECT * FROM OrderDetail;", conn)
        conn.close()

        df_order = pd.read_csv('files_data_pipeline/output_orders.csv')

        query = '''
            SELECT sum(od.Quantity) AS total
            FROM df_order o 
            LEFT JOIN df_order_detail od ON o.id = od.OrderId 
            WHERE o.ShipCity = 'Rio de Janeiro'
            GROUP BY o.ShipCity
            '''
        count = sqldf(query).values[0][0]

        with open('files_data_pipeline/count.txt', mode='w') as f:
            f.write(f'{str(count)}')

        message = 'success'
    except Exception as e:
        message = 'error: ' + str(e)
    finally:
        write_simple_log('read_database_join_csv_and_export_txt', message)

with DAG(
    'DesafioAirflow',
    default_args=default_args,
    description='Desafio de Airflow da Indicium',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['example'],
) as dag:
    dag.doc_md = """
        Esse é o desafio de Airflow da Indicium.
        ** Hugo Silveira Sousa **
    """
   
    export_final_output = PythonOperator(
        task_id='export_final_output',
        python_callable=export_final_answer,
        provide_context=True
    )

    export_final_output.doc_md = dedent(
        """\
    #### Task Documentation
    
    Essa task lê o arquivo `files_data_pipeline/count.txt` e salva o valor encontrado no arquivo.
    Depois, pega o email cadastrado como variável no airflow. 
    Soma o valor count com o email. 
    Codifica o resultado com ascii (b64). 
    Por fim, salva o valor codificado no arquivo `files_data_pipeline/final_output.txt`. 
    """
    )

    task1 = PythonOperator(
        task_id='task1',
        python_callable=read_database_and_export_csv,
        provide_context=True
    )

    task1.doc_md = dedent(
        """\
    #### Task Documentation

    Essa task se conecta no banco `data/Northwind_small.sqlite`. 
    Seleciona a tabela 'Order'. 
    Salva o resultado no arquivo `files_data_pipeline/output_orders.csv`. 
    """
    )

    task2 = PythonOperator(
        task_id='task2',
        python_callable=read_database_join_csv_and_export_txt,
        provide_context=True
    )
    
    task2.doc_md = dedent(
        """\
    #### Task Documentation
        
    Essa task se conecta ao banco `data/Northwind_small.sqlite`. 
    Seleciona a tabela 'OrderDetail'. 
    Carrega o arquivo `files_data_pipeline/output_orders.csv`.
    Realiza um join, e soma a quantidade de pedidos enviados para a cidade 'Rio de janeiro'.
    Por fim, a soma é salva no arquivo `files_data_pipeline/count.txt`.
    """
    )

task1 >> task2 >> export_final_output