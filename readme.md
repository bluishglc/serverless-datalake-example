# Serverless Datalake Example/Framework: The Best Practice of Serverless Datalake Enginerring on AWS Glue/Athena/MWAA(Airflow)/S3

## 1. Build

To build this project, you need have JDK & Maven on your local, and you should also have an AWS account with Admin role.

1. Check out project.
2. Update src/main/profiles/prd.properties, change replace all "<...>" values against your environment.
3. Run maven command under project root dir:

```bash
mvn clean package
```

4. Get serverless-datalake-example-1.0.zip file under target folder.

## 2. Install

You have 2 ways to get installer package, one is building from source codes just as step above, the other one is downloading directly:

```bash
wget https://github.com/bluishglc/serverless-datalake-example/releases/download/v1.1/serverless-datalake-example-1.0.zip
```

then unzip package and run install command:

```bash
./serverless-datalake-example-1.0/bin/install.sh --region <your-aws-region> --app-bucket <your-app-bucket-name> --data-bucket <your-data-bucket-name> --airflow-dags-home s3://<your-airflow-dags-path> --access-key-id '<your-access-key-id>' --access-key '<your-access-key>'
```

Note: the parameters of cli will overwrite values in prd/dev properties files.


## 3. Run

There are 2 ways to run, one is by airflow, the other is by cli. for airflow, you must have a running airflow environment, and have a configured ssh connection name `ssh_to_client` which can connect to current node via ssh, then copy `wfl/sdl_monthly_build.py` to the dag folder of airflow or assign path to --airflow-dags-home in install command, if all done, you will see a dag named `sdl-monthly-build`, then you can start it from airflow console page. Or you can run this project via cli immediately as following:

```bash
./serverless-datalake-example-1.0/bin/sdl.sh build --year 2020 --month 01
```
This command will run a full batch of data in 2020/01.
