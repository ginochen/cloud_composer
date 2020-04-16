import airflow
from airflow import DAG
#from airflow.operators.python_operator import PythonOperator
#from airflow.operators.bash_operator import BashOperator
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.contrib.kubernetes.secret import Secret
from airflow.contrib.kubernetes.volume import Volume
from airflow.contrib.kubernetes.volume_mount import VolumeMount
from airflow.contrib.kubernetes.pod import Port

from datetime import date, timedelta
from airflow.models import Variable
# install pypi packages from CLI
# gcloud composer environments update ENVIRONMENT-NAME --update-pypi-packages-from-file requirements.txt --location LOCATION

# To mount the data for model training:
# 1. create a persistent disk on GCE
# gcloud compute disks create [DISK_NAME]  --size [DISK_SIZE] --type [DISK_TYPE]
# gcloud compute disks create test-disk --size 10G --type pd-standard

# 2. follow the steps (http://space.af/blog/2018/09/30/gke-airflow-cloud-composer-and-persistent-volumes/)
#    to create a persistent volume claim from the persistent volume for k8s cluster using the created GCE persistent disk

# 3. Mount persistent volumes from cluster into the Pod
#volume_mount = VolumeMount('test',
#                           mount_path='/tmp',
#                           sub_path=None,
#                           read_only=False)
#volume_config = {
#            'persistentVolumeClaim':
#                {
#                    'claimName': 'test'
#                }
#        }
#volume = Volume(name='test', configs=volume_config)

# In this demo we'll skip the data mounting and just focus on Pod utilization
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
    name="airflowjob-model-training",
    # The namespace to run within Kubernetes, default namespace is
    # `default`. There is the potential for the resource starvation of
    # Airflow workers and scheduler within the Cloud Composer environment,
    # the recommended solution is to increase the amount of nodes in order
    # to satisfy the computing requirements. Alternatively, launching pods
    # into a custom namespace will stop fighting over resources.
    namespace="default",
    image="train_image:latest",
    cmds=[
        "python",
        "/model/train.py",
    ],
    # tells if the target cluster is the same as the one where airflow is running. 
#    in_cluster=IN_CLUSTER,
    # We are using here a dynamic parameter IN_CLUSTER which takes different values depending on the environment where airflow is run. 
    # For local test, we will deploy the pods in our airflow dev cluster, so the value will be False and CLUSTER_CONTEXT will be equal to our dev cluster context.  
#    cluster_context=CLUSTER_CONTEXT, 
    get_logs=True,
    startup_timeout_seconds=60 * 5,
    # This will delete completed pods in the namespace as they finish, keeping Airflow below its resource quotas
    is_delete_operator_pod=True, 
    resources=Resources(request_cpu="100m", request_memory="1Gi"),
    # Determines when to pull a fresh image, if 'IfNotPresent' will cause
    # the Kubelet to skip pulling an image if it already exists. If you
    # want to always pull a new image, set it to 'Always'.
    image_pull_policy="Always",
    # tells on which node the pod should be run on. We specify here nodes from a specific pool.
    node_selectors={ 
        "cloud.google.com/gke-nodepool": "n1-standard-1-high-cost-GPU-pool"
        #"cloud.google.com/gke-nodepool": "n1-standard-8-high-cost-pool"
    },
    # sets the required tolerations. Here we are providing the associated toleration for the node pool taint.
    tolerations=[ 
        {
            "key": "node-pool",
            "operator": "Equal",
            "value": "n1-standard-8-high-cost-GPU-pool",
            "effect": "NoExecute",
        }
    ],
    retries=1,
    retry_delay=timedelta(minutes=5),
    volumes=[volume],
    volume_mounts=[volume_mount],
    # Enable the pod to send a result to the airflow worker. 
    # We need to write the data we want to return in a json file located in : /airflow/xcom/return.json. 
    # Under the hood, KubernetesPodOperator mount a volume and use a sidecar container which will 
    # read the file and output it on the stdout, which is then captured and parsed by the worker.
    xcom_push=True, 
    dag=dag,
)

port = Port('http', 8001)

serve = KubernetesPodOperator(
    task_id="serving",
    name="airflowjob-model-serving",
    namespace="default",
    image="serve_image:latest",
    cmds=[
        "mlflow", "models", "serve", "-m", "/mlruns/0/your_uuid/artifacts/model", "-h", "0.0.0.0", "-p", "8001"
    ],
    get_logs=True,
    startup_timeout_seconds=60 * 5,
    is_delete_operator_pod=True, 
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
    ports=[port],
    xcom_push=True, 
    dag=dag,
)


# specify the upstream downstream DAG relationship
train >> serve
