
import subprocess
import os
DBT_PROJECT_DIR = "/opt/airflow/dbt/flight_project"
DBT_PROFILES_DIR = "/home/airflow/.dbt"

def run_dbt(command):
    env = {
        "DBT_LOG_PATH": "STDOUT",       
        "DBT_TARGET_PATH": "/tmp",    
    }

    process = subprocess.Popen(
        ["dbt", command, "--project-dir", DBT_PROJECT_DIR, "--profiles-dir", DBT_PROFILES_DIR],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        env={**env, **os.environ}  
    )

    stdout, stderr = process.communicate()
    print(stdout)
    print(stderr)

def dbt_run():
    run_dbt("run")

def dbt_test():
    run_dbt("test")