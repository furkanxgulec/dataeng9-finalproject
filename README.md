# Setting up Airflow, Minio, Spark, and Sqoop

## Pre-installation Steps for Airflow (including database setup and volumes creation)
```command
spark, airflow, minioya ait kullanıcı şifreleri .env  dosylarının içindedir
örnek /minio/.env
````
```bash
docker-compose up -d --build airflow-init
```

This command initializes the Airflow environment, including database setup and volume creation.

### Troubleshooting: "ValueError: Unable to configure handler 'processor'" in Airflow

If you encounter an error in the Airflow service related to configuring the 'processor' handler, follow these steps:

```bash
cd airflow/
sudo chown 50000:0 dags logs plugins
cd ..
docker-compose up -d
```

This command changes the ownership of specific directories within the Airflow directory to resolve the issue.

## HiveServer2 and HiveMetaStore to Launch

Note: This step is required for hive operations If you are not hive, you do not need to start it.

```bash
docker exec -it sqoop bash
chmod +x ./sqoop/hive_start.sh
sed -i -e 's/\r$//' ./sqoop/hive_start.sh
./sqoop/hive_start.sh
```
