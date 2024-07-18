from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys
import os
from airflow.utils.task_group import TaskGroup
from airflow.hooks.mysql_hook import MySqlHook

# Add the scripts directory to the system path
sys.path.append(os.path.join(os.path.dirname(__file__), '../scripts'))

# Import functions from your scripts
from models.raw_models.raw_load_accounts  import create_tables as subtask_1_1, tuncate_table as subtask_1_2, insert_table as subtask_1_3
from models.raw_models.raw_load_atms  import create_tables as subtask_2_1, tuncate_table as subtask_2_2, insert_table as subtask_2_3
from models.raw_models.raw_load_bill_payments  import create_tables as subtask_3_1, tuncate_table as subtask_3_2, insert_table as subtask_3_3
from models.raw_models.raw_load_branches  import create_tables as subtask_4_1, tuncate_table as subtask_4_2, insert_table as subtask_4_3
from models.raw_models.raw_load_cards  import create_tables as subtask_5_1, tuncate_table as subtask_5_2, insert_table as subtask_5_3
from models.raw_models.raw_load_cheques  import create_tables as subtask_6_1, tuncate_table as subtask_6_2, insert_table as subtask_6_3
from models.raw_models.raw_load_credit_scores  import create_tables as subtask_7_1, tuncate_table as subtask_7_2, insert_table as subtask_7_3
from models.raw_models.raw_load_customer_support  import create_tables as subtask_8_1, tuncate_table as subtask_8_2, insert_table as subtask_8_3
from models.raw_models.raw_load_customers  import create_tables as subtask_9_1, tuncate_table as subtask_9_2, insert_table as subtask_9_3
from models.raw_models.raw_load_employees  import create_tables as subtask_10_1, tuncate_table as subtask_10_2, insert_table as subtask_10_3
from models.raw_models.raw_load_fixed_deposits  import create_tables as subtask_11_1, tuncate_table as subtask_11_2, insert_table as subtask_11_3
from models.raw_models.raw_load_insurance  import create_tables as subtask_12_1, tuncate_table as subtask_12_2, insert_table as subtask_12_3
from models.raw_models.raw_load_investments import create_tables as subtask_13_1, tuncate_table as subtask_13_2, insert_table as subtask_13_3
from models.raw_models.raw_load_loans import create_tables as subtask_14_1, tuncate_table as subtask_14_2, insert_table as subtask_14_3
from models.raw_models.raw_load_mortgage_applications  import create_tables as subtask_15_1, tuncate_table as subtask_15_2, insert_table as subtask_15_3
from models.raw_models.raw_load_online_banking  import create_tables as subtask_16_1, tuncate_table as subtask_16_2, insert_table as subtask_16_3
from models.raw_models.raw_load_recurring_deposits  import create_tables as subtask_17_1, tuncate_table as subtask_17_2, insert_table as subtask_17_3
from models.raw_models.raw_load_savings_goals  import create_tables as subtask_18_1, tuncate_table as subtask_18_2, insert_table as subtask_18_3
from models.raw_models.raw_load_service_charges  import create_tables as subtask_19_1, tuncate_table as subtask_19_2, insert_table as subtask_19_3
from models.raw_models.raw_load_transactions  import create_tables as subtask_20_1, tuncate_table as subtask_20_2, insert_table as subtask_20_3

# Define default args
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
}
mysql_hook = MySqlHook(mysql_conn_id='mysql_default')
conn = mysql_hook.get_conn()
cursor=conn.cursor()
# Define the DAG
with DAG(
    'parent_dag',
    default_args=default_args,
    description='A DAG to run transformation scripts with subtasks',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    




    with TaskGroup('raw_load_accounts') as script_1_tasks1:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_1_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_1_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_1_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_atms') as script_1_tasks2:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_2_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_2_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_2_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_bill_payments') as script_1_tasks3:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_3_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_3_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_3_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_branches') as script_1_tasks4:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_4_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_4_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_4_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_cards') as script_1_tasks5:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_5_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_5_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_5_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_cheques') as script_1_tasks6:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_6_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_6_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_6_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_credit_scores') as script_1_tasks7:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_7_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_7_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_7_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_customer_support') as script_1_tasks8:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_8_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_8_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_8_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_customers') as script_1_tasks9:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_9_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_9_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_9_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_employees') as script_1_tasks10:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_10_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_10_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_10_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_fixed_deposits') as script_1_tasks11:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_11_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_11_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_11_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_insurance') as script_1_tasks12:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_12_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_12_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_12_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_investments') as script_1_tasks13:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_13_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_13_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_13_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_loans') as script_1_tasks14:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_14_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_14_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_14_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_mortgage_applications') as script_1_tasks15:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_15_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_15_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_15_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_online_banking') as script_1_tasks16:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_16_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_16_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_16_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_recurring_deposits') as script_1_tasks17:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_17_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_17_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_17_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_savings_goals') as script_1_tasks18:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_18_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_18_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_18_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_service_charges') as script_1_tasks19:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_19_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_19_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_19_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3
    with TaskGroup('raw_load_transactions') as script_1_tasks20:
        task1_subtask1 = PythonOperator(
            task_id='create_table',
            python_callable=subtask_20_1,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask2 = PythonOperator(
            task_id='tuncate_table',
            python_callable=subtask_20_2,
            op_kwargs={'cursor': cursor },
        )
        
        task1_subtask3 = PythonOperator(
            task_id='insert_table',
            python_callable=subtask_20_3,
            op_kwargs={'cursor': cursor },
        )
        
        # Set dependencies within TaskGroup
        task1_subtask1 >> task1_subtask2 >> task1_subtask3

    script_1_tasks1   >>script_1_tasks2   >>script_1_tasks3   >>script_1_tasks4   >>script_1_tasks5   >>script_1_tasks6   >>script_1_tasks7   >>script_1_tasks8   >>script_1_tasks9   >>script_1_tasks10   >>script_1_tasks11   >>script_1_tasks12   >>script_1_tasks13   >>script_1_tasks14   >>script_1_tasks15   >>script_1_tasks16   >>script_1_tasks17   >>script_1_tasks18   >>script_1_tasks19   >>script_1_tasks20