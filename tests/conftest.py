"""
Fixture pytest partagée : une SparkSession unique pour tous les tests.
"""

import os
import sys
import warnings

# Désactiver les avertissements
warnings.filterwarnings("ignore")

# Configurer les variables d'environnement AVANT l'import de PySpark
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["PYARROW_IGNORE_TIMEZONE"] = "1"

import pytest
from pyspark.sql import SparkSession


@pytest.fixture(scope="session")
def spark():
    """Crée une SparkSession avec configuration minimale pour Windows."""
    spark_session = (SparkSession.builder
             .master("local")
             .appName("TP3-tests")
             .config("spark.driver.host", "127.0.0.1")
             .config("spark.driver.bindAddress", "127.0.0.1")
             .config("spark.driver.memory", "1g")
             .config("spark.sql.shuffle.partitions", "1")
             .config("spark.sql.codegen.wholeStage", "false")
             .config("spark.python.worker.reuse", "false")
             .config("spark.sql.execution.arrow.pyspark.enabled", "false")
             .config("spark.sql.pyspark.udf.sqlType", "true")
             .getOrCreate())
    yield spark_session
    spark_session.stop()