# cloud_composer
This project is a demo for using Airflow on Google Cloud Composer to manage the ML pipeline with resource autoscalability.

# Initiate node pools 
Follow the basic GKE cluster setup as: ![this figure](https://miro.medium.com/max/1400/1*tHeyUE-dArS2X3seVXEz2w.png)


Additional node pool `n1-standard-1-lowmem-pool` is created for model serving or other low resource purpose.

In order for Composer to use these node pools, we need to first go to GKE engine in the console 

`"Create cluster" -> "Add node pool" -> "Enable autoscaling" -> set "Minimum number of nodes" to 0 -> "Create" cluster`

The reason to set minimum to 0 is when GPU not used the node will shut down.

We can also set up the GKE config directly using the [code](./gke-setup/gke-node-pool-example.sh).


After all node pools are set, go back to Composer to finish setting up the Airflow node pool

Once set up, you can check the Log and DAG directory by clicking on the interface ![interface](./fig/composer_0.png)

Ok I'm calling the project `composer-test`, so the URI for the DAGs are under `gs://us-central1-composer-test-61840402-bucket/dags/` 

Log in to enable `gsutil` by:

`gcloud auth login`

Upload the Dags by `gsutil rsync` or follow the [blog](https://engineering.adwerx.com/sync-a-github-repo-to-your-gcp-composer-airflow-dags-folder-2b87eb065915) 
to set up `cloudbuild.yaml` file to sync the git repo directly with Composer.
