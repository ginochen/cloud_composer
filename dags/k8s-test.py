import airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import date, timedelta
from airflow.models import Variable

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': airflow.utils.dates.days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
     
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
}

dag = DAG(
    'hp_opt_dag', default_args=default_args,
    schedule_interval="@once")

train = KubernetesPodOperator(
    task_id="training",
    name="airflowjob-{}-model-training-{}".format(env, uuid),
    # The namespace to run within Kubernetes, default namespace is
    # `default`. There is the potential for the resource starvation of
    # Airflow workers and scheduler within the Cloud Composer environment,
    # the recommended solution is to increase the amount of nodes in order
    # to satisfy the computing requirements. Alternatively, launching pods
    # into a custom namespace will stop fighting over resources.
    namespace="default",
    image="training_image/url:latest",
    cmds=[
        "python",
        "training/run.py",
        "--env",
        env,
        "--uuid",
        uuid,
    ],
#    in_cluster=IN_CLUSTER, # This will tell your task to look inside the cluster for the Kubernetes config. In this setup, your workers are tied to a role with the right privileges in the cluster
#    cluster_context=CLUSTER_CONTEXT, # if in_cluster==True, then won't use cluster_contex
    in_cluster=True
    get_logs=True,
    startup_timeout_seconds=60 * 5,
    is_delete_operator_pod=True, # This will delete completed pods in the namespace as they finish, keeping Airflow below its resource quotas
    resources=Resources(request_cpu="100m", request_memory="1Gi"),
    image_pull_policy="Always",
    node_selectors={
        "cloud.google.com/gke-nodepool": "n1-standard-1-low-cost-pool"
        #"cloud.google.com/gke-nodepool": "n1-standard-8-high-cost-pool"
    },
    tolerations=[
        {
            "key": "node-pool",
            "operator": "Equal",
            "value": "n1-standard-1-low-cost-pool",
            "effect": "NoExecute",
        }
    ],
    retries=1,
    retry_delay=timedelta(minutes=5),
    xcom_push=True,
    dag=dag,
)

train #-> serve
