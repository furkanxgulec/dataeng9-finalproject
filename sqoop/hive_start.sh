#!/bin/bash

# Start Hive Metastore and run it in the background
nohup hive --service metastore > metastore.log 2>&1 &
echo "Hive Metastore has been started."

# Start HiveServer2 and run it in the background
nohup hive --service hiveserver2 > hiveserver2.log 2>&1 &
echo "HiveServer2 has been started."

# The script ends here
echo "Hive services are starting. Please wait for 60 seconds..."
sleep 60

