YOUR_SPARK_HOME/bin/spark-submit \
  --class org.example.Main \
  --master local[4] \
  /opt/spark/apps/org/example/apachesparkapp/1.0-SNAPSHOT/apachesparkapp-1.0-20241015.004822-1.jar


  /opt/bitnami/spark/bin/spark-submit \
    --class "org.example.Main" \
    /opt/spark/apps/org/example/apachesparkapp/1.0-SNAPSHOT/apachesparkapp-1.0-20241015.211223-2.jar \
    /opt/spark/data/Crime_Data_from_2020_to_Present.csv