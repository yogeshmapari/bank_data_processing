from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.task_group import TaskGroup

# Define a function to import tasks from child DAGs
def import_tasks_from_child_dag(parent_dag, child_dag_id, task_id_prefix):
    # Import the child DAG module
    import importlib.util
    spec = importlib.util.spec_from_file_location(child_dag_id, f"/path/to/{child_dag_id}.py")
    child_dag_module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(child_dag_module)

    # Retrieve the tasks from the child DAG
    tasks = child_dag_module.tasks

    # Add tasks to the parent DAG
    for task_id, task_instance in tasks.items():
        parent_dag.add_task(task_instance)

# Default arguments for DAGs
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
    dag  # Start the sequence with the first child DAG

    # Import tasks from each child DAG as sub-tasks within the parent DAG
    for child_dag_id in child_dags:
        # Use a TaskGroup to group tasks from each child DAG under a common group
        with TaskGroup(group_id=f'task_group_{child_dag_id}', dag=dag) as tg:
            # Import tasks from the child DAG
            import_tasks_from_child_dag(tg, child_dag_id, task_id_prefix=f'{child_dag_id}_')

            # Optionally, define dependencies or additional logic within each TaskGroup

        # Optionally, define dependencies between child DAGs or perform additional operations

