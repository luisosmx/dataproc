# Dataproc, Spark, Hadoop and GCP

## Create dataproc cluster

```bash
gcloud dataproc clusters create cluster-cf6f --bucket dataproc-test-381100 --region us-central1 --zone us-central1-f --single-node --master-machine-type n1-standard-2 --master-boot-disk-size 30 --image-version 2.0-debian10 --max-idle 3600s --scopes 'https://www.googleapis.com/auth/cloud-platform' --project airflow-gke-381100
```

## Create job 

```bash
POST /v1/projects/airflow-gke-381100/regions/us-central1/jobs:submit/
{
  "projectId": "airflow-gke-381100",
  "job": {
    "placement": {
      "clusterName": "cluster-cf6f"
    },
    "statusHistory": [],
    "reference": {
      "jobId": "job-e38cfa7f",
      "projectId": "airflow-gke-381100"
    },
    "pysparkJob": {
      "mainPythonFileUri": "gs://dataproc-test-381100/main_poc.py",
      "properties": {},
      "jarFileUris": [
        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
      ]
    }
  }
}
```