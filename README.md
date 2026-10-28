# Tarea 3 – Procesamiento de datos con Apache Spark
**Estudiante:** Ricardo Andrés Díaz Díaz  
**Curso:** Big Data – UNAD  
**Entrega:** 29 de octubre de 2026

## Contenido
- `batch_analisis.py`: carga el CSV desde HDFS, muestra esquema, filas, estadísticas y filtra `VALOR > 5000`.
- `kafka_producer.py`: genera datos simulados de sensores y los publica en el topic `sensor_data`.
- `spark_streaming_consumer.py`: consume `sensor_data` con Spark Structured Streaming y calcula promedios por sensor en ventanas de un minuto.
- `requirements.txt`: dependencia Python del productor.

## Requisitos previos
- Hadoop/HDFS activo y el archivo ubicado en `hdfs://localhost:9000/Tarea3/rows.csv`.
- Spark 3.5.8 instalado.
- Kafka 3.9.2 instalado y escuchando en `localhost:9092`.
- Topic Kafka `sensor_data` creado.
- `kafka-python` instalado en el Python que ejecuta el productor.

## Ejecución batch
Con Hadoop/HDFS activo:
```bash
spark-submit batch_analisis.py
```

## Ejecución en tiempo real
1. Iniciar ZooKeeper y Kafka según la configuración local.
2. En una terminal, ejecutar el productor:
   ```bash
   python3 kafka_producer.py
   ```
3. En otra terminal, ejecutar el consumidor:
   ```bash
   spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
   ```
4. Detener cada programa con `Ctrl+C`.

## Resultados batch observados
- Registros: 8.351.
- Promedio de `VALOR`: aproximadamente 2.370,93.
- Mínimo: 620,62.
- Máximo: 5.061,21.
- Registros con `VALOR > 5000`: 5.

Los resultados corresponden a la ejecución observada en la máquina virtual. El CSV contiene `VALOR`, `UNIDAD`, `VIGENCIADESDE` y `VIGENCIAHASTA`.

## Enlaces pendientes
- Video batch: PEGAR ENLACE
- Video Kafka + Spark Streaming: PEGAR ENLACE
- Presentación online: PEGAR ENLACE
- Repositorio GitHub: PEGAR ENLACE

Antes de publicar, comprueba que no incluyas contraseñas, claves privadas ni credenciales. El CSV no está incluido en este paquete.
