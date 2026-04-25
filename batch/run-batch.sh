#!/bin/bash
while true; do
  echo "[BATCH] Ejecutando: $(date)"
  /opt/spark/bin/spark-submit \
    --master local[2] \
    --class BatchAggregations \
    --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 \
    /app/app.jar
  echo "[BATCH] Listo. Próxima ejecución en 2 minutos."
  sleep 120
done