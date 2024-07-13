from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2023, 1, 1),  # Adjust the start date as per your requirement
}

# List of child DAGs to trigger sequentially
child_dags = [
    'raw_load_accounts',
    'raw_load_atms',
    'raw_load_bill_payments',
    'raw_load_branches',
    'raw_load_cards',
    'raw_load_cheques',
    'raw_load_credit_scores',
    'raw_load_customers',
    'raw_load_customer_support',
    'raw_load_employees',
    'raw_load_fixed_deposits',
    'raw_load_investments',
    'raw_load_insurance',
    'raw_load_loans',
    'raw_load_mortgage_applications',
    'raw_load_online_banking',
    'raw_load_recurring_deposits',
    'raw_load_savings_goals',
    'raw_load_service_charges',
    'raw_load_transactions',
]

# Define the parent DAG
with DAG('parent_dag',
         default_args=default_args,
         schedule_interval=None,  # This parent DAG won't be scheduled, it will be triggered by its child DAGs
         catchup=False) as dag:

    # Define a list to hold the TriggerDagRunOperator tasks
    trigger_tasks = []

    # Create a TriggerDagRunOperator for each child DAG
    for child_dag_id in child_dags:
        trigger_task = TriggerDagRunOperator(
            task_id=f'trigger_{child_dag_id}',
            trigger_dag_id=child_dag_id,
            wait_for_completion=True,  # Wait for the child DAG to complete before proceeding
        )
        trigger_tasks.append(trigger_task)

    # Set the order of tasks in the DAG (linear dependency)
    for i in range(len(trigger_tasks) - 1):
        trigger_tasks[i] >> trigger_tasks[i + 1]

    # Set the parent DAG's task dependencies
    dag   # Start the sequence with the first child DAG

    # Import each child DAG as a sub-DAG within the parent DAG
    for child_dag_id in child_dags:
        globals()[child_dag_id] = DAG(
            dag_id=child_dag_id,
            default_args=default_args,
            schedule_interval=None,  # Disable scheduling for this example
            catchup=False,  # Ensure tasks run only once
        )

        # Optionally, you can set up dependencies between child DAGs or define additional tasks

        # Link the child DAGs to the parent DAG
        dag 
