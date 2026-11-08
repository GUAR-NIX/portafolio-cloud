"""Sesión de Spark con Delta Lake, S3A (MinIO) y estado en RocksDB."""

from __future__ import annotations

import os

from pyspark.sql import SparkSession

from streaming_gh.config import S3_ACCESS_KEY, S3_ENDPOINT, S3_SECRET_KEY


def crear_sesion(nombre: str = "streaming-gh", s3: bool = True, master: str | None = None) -> SparkSession:
    constructor = (
        SparkSession.builder.appName(nombre)
        .master(master or os.environ.get("SPARK_MASTER", "local[4]"))
        .config("spark.driver.memory", os.environ.get("SPARK_DRIVER_MEMORY", "2g"))
        # Fuera del heap: buffers directos (Netty, Parquet, S3A) acotados; por defecto pueden llegar al tamaño del heap
        .config("spark.driver.extraJavaOptions", "-XX:MaxDirectMemorySize=512m -XX:+UseG1GC")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        .config("spark.sql.session.timeZone", "UTC")
        # Pocas particiones de shuffle: con 4 núcleos, 200 (el valor por defecto) son 200 tareas y 200 almacenes de
        # estado por micro-lote, casi todos vacíos.
        .config("spark.sql.shuffle.partitions", os.environ.get("SPARK_SHUFFLE_PARTITIONS", "4"))
        .config("spark.sql.streaming.stateStore.providerClass",
                "org.apache.spark.sql.execution.streaming.state.RocksDBStateStoreProvider")
        .config("spark.sql.streaming.stateStore.rocksdb.changelogCheckpointing.enabled", "true")
        # RocksDB vive fuera del heap de la JVM: sin tope, crece hasta que Docker mata el contenedor por memoria
        .config("spark.sql.streaming.stateStore.rocksdb.boundedMemoryUsage", "true")
        .config("spark.sql.streaming.stateStore.rocksdb.maxMemoryUsageMB", os.environ.get("ROCKSDB_MAX_MB", "512"))
        .config("spark.databricks.delta.schema.autoMerge.enabled", "false")
        .config("spark.ui.showConsoleProgress", "false")
    )
    if os.environ.get("SPARK_PAQUETES"):  # fuera de la imagen (p. ej. un venv con pyspark): bajar los conectores
        constructor = constructor.config("spark.jars.packages", os.environ["SPARK_PAQUETES"])
    if s3:
        constructor = (
            constructor.config("spark.hadoop.fs.s3a.endpoint", S3_ENDPOINT)
            .config("spark.hadoop.fs.s3a.access.key", S3_ACCESS_KEY)
            .config("spark.hadoop.fs.s3a.secret.key", S3_SECRET_KEY)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                    "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
            .config("spark.hadoop.fs.s3a.fast.upload", "true")
        )
    spark = constructor.getOrCreate()
    spark.sparkContext.setLogLevel(os.environ.get("SPARK_LOG_LEVEL", "WARN"))
    return spark
