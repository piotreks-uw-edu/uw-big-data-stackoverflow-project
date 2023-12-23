### the first step - configure token
databricks configure --token
here:
https://adb-6199578147525915.15.azuredatabricks.net/
dapie0c8515fe78ee9ff4b71ab818266b177

### copy
databricks fs cp -r /data dbfs:/fall_2023_users/piotreks/csv/ --overwrite

### list folder
databricks fs ls dbfs:/fall_2023_users/piotreks/

### remove the folder
databricks fs rm -r dbfs:/fall_2023_users/piotreks/csv