from datetime import datetime, timezone
from airflow.decorators import dag
from airflow.operators.bash import BashOperator

# dag variables
DESCRIPTION = "used to start the daily blood donation pipeline job"
TAGS = ["project"]
START_DATE = datetime(2024, 1, 1, tzinfo=timezone.utc)
SCHEDULE = "55 0 * * *"
CATCHUP = False

# bash commands
execute_script = """
bash /home/ubuntu/blood-donation-pipeline/bash-start-script.sh
"""

@dag(
    description=DESCRIPTION,
    tags=TAGS,
    start_date=START_DATE,
    schedule=SCHEDULE,
    catchup=CATCHUP
)
def blood_donation_pipeline():
    start_script = BashOperator(
        task_id = "start_script",
        bash_command = execute_script
    )
    
    start_script

blood_donation_pipeline()