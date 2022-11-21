# Serverless Datalake Example/Framework: The Best Practice of Serverless Datalake Enginerring on AWS Glue/Athena/MWAA(Airflow)/S3

---

## Update Nodes

1. Original TLC CSV files on S3 are replaced by parquet files. CSV files are moved to a backup folder, so `feeder.sh` is changed. @2022-11-04
2. A new branch: datahub is ready, add features to support metadata management tool: datahub. @2022-12-21

---

## 1. Build

To build this project, you need have JDK & Maven on your local, and you should also have an AWS account with Admin role.

1. Check out project. **Note that If you need run and demo datahub integration feature, please check out datahub branch.**
2. Update src/main/profiles/prd.properties, change replace all "<...>" values against your environment.
3. Run maven command under project root dir:

```bash
mvn clean package
```


4. Get serverless-datalake-example-1.0.zip file under target folder.

## 2. Install

You have 2 ways to get installer package, one is building from source codes just as step above, the other one is downloading directly:

```bash
wget https://github.com/bluishglc/serverless-datalake-example/releases/download/v1.0/serverless-datalake-example-1.0-master.zip -O serverless-datalake-example-1.0.zip
```

**Note that If you need run and demo datahub integration feature, please download from datahub release: **

```bash
wget https://github.com/bluishglc/serverless-datalake-example/releases/download/v1.0/serverless-datalake-example-1.0-datahub.zip -O serverless-datalake-example-1.0.zip
```

unzip package:

```bash
unzip serverless-datalake-example-1.0.zip
```

then run install command:

```bash
./serverless-datalake-example-1.0/bin/install.sh \
    --region <your-aws-region> \
    --app-bucket <your-app-bucket-name> \
    --data-bucket <your-data-bucket-name> \
    --airflow-dags-home s3://<your-airflow-dags-path> \
    --access-key-id '<your-access-key-id>' \
    --secret-access-key '<your-secret-access-key>' \
    --nyc-tlc-access-key-id '<your-global-account-access-key-id>' \
    --nyc-tlc-secret-access-key '<your-global-account-secret-access-key>'
```

Note: the parameters of cli will overwrite values in prd/dev properties files.

## 3. Init

This step will create crawlers, jobs, databases and tables.

```bash
sdl.sh init
```

## 4. Run

There are 2 ways to run, one is by airflow, the other is by cli. for airflow, you must have a running airflow environment, and have a configured ssh connection name `ssh_to_client` which can connect to current node via ssh, then copy `wfl/sdl_monthly_build.py` to the dag folder of airflow or assign path to --airflow-dags-home in install command, if all done, you will see a dag named `sdl-monthly-build`, then you can start it from airflow console page. Or you can run this project via cli immediately as following:

```bash
./serverless-datalake-example-1.0/bin/sdl.sh build --year 2020 --month 01
```
This command will run a full batch of data in 2020/01.

### 5. Known Issues

When 1.0 relase, it works well on cn and us regions, however, recently, if run on us-east-1 region, there will be an error when run jobs:

```log
Exception in User Class: java.io.IOException : com.amazon.ws.emr.hadoop.fs.shaded.com.amazonaws.services.s3.model.AmazonS3Exception: Access Denied (Service: Amazon S3; Status Code: 403; Error Code: AccessDenied ....
```

It is not caused by IAM or S3 policies, I also checked VPC S3 endpoint, no any problems, so by now, we have not find root cause.


## 6. Notes

### 6.1 @2022-11-04

csv https download link is unavailable from May 13, 2022, they are changed to parquet files. so following cli does not work anymore.
```bash
wget "https://nyc-tlc.s3.amazonaws.com/trip data/${category}_tripdata_${YEAR}-${MONTH}.csv" -P "/tmp/nyc-tlc/"
```
although csv backup files are provided as public bucket, cli as following works in aws global regions:
```bash
aws s3 cp "s3://nyc-tlc/csv_backup/${category}_tripdata_${YEAR}-${MONTH}.csv" "/tmp/nyc-tlc/"
```
however, this public bucket does NOT support anonymous access, so for a china region account, it is still inaccessible.
at beginning, we created a github repo to store csv files, and download files as following:
```bash
wget --tries=10 --timeout=10 "https://github.com/bluishglc/nyc-tlc-data/releases/download/v1.0/${category}_tripdata_${YEAR}-${MONTH}.csv.gz" -P "/tmp/nyc-tlc/"
gzip -d "/tmp/nyc-tlc/${category}_tripdata_${YEAR}-${MONTH}.csv.gz"
````
however, the network from China to Github is very unstable, downloads often failed, so we have to add 2 parameters:

- --nyc-tlc-access-key-id
- --nyc-tlc-secret-access-key

They are actually AWS global S3 account's AKSK, with this AKSK, the cli can download CSV files from US region to local, then upload to China S3 bucket.

