#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function,division
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DoubleType
import pyspark.sql.functions as F
import sys
#
# Script para extraer información de los ficheros cite75_99.txt y apat63_99.txt.
# a) A partir del fichero cite75_99.txt obtener el número de citas de cada patente.
#    Debes obtener un DataFrame de la siguiente forma:
#   +--------+------+
#   |NPatente|ncitas|
#   +--------+------+
#   | 3060453|  3   |
#   | 3390168|  6   |
#   | 3626542| 18   |
#   | 3611507|  5   |
#   | 3000113|  4   |
#
# b) A partir del fichero apat63_99.txt, crear un DataFrame que contenga el número de
# patente, el país y el año, descartando el resto de campos del fichero.
# Ese DataFrame debe tener la siguiente forma:
#
#   +--------+----+----+
#   |NPatente|Pais|Anho|
#   +--------+----+----+
#   | 3070801| BE| 1963|
#   | 3070802| US| 1963|
#   | 3070803| US| 1963|
#   | 3070804| US| 1963|
#   | 3070805| US| 1963|
#
# Ejecutar en local con:
# spark-submit --master local[*] --driver-memory 4g p1.py cite75_99.txt apat63_99.txt dfCitas.parquet dfInfo.parquet
# Ejecución en un cluster YARN:
# spark-submit --master yarn --num-executors 8 --driver-memory 4g p1.py path_a_cite75_99.txt_en_HDFS path_a_apat63_99.txt_en_HDFS dfCitas.parquet dfInfo.parquet

# This is not a class, I hate OOP, this is just a wardrove for functions...
class DFInterface:
    _cite_schema = StructType([
        StructField("CITING", IntegerType(), True),
        StructField("CITED", IntegerType(), True)
    ])

    _apat_schema = StructType([
        StructField("PATENT", IntegerType(), True),
        StructField("GYEAR", IntegerType(), True),
        StructField("GDATE", IntegerType(), True),
        StructField("APPYEAR", IntegerType(), True),
        StructField("COUNTRY", StringType(), True),
        StructField("POSTATE", StringType(), True),
        StructField("ASSIGNEE", IntegerType(), True),
        StructField("ASSCODE", IntegerType(), True),
        StructField("CLAIMS", IntegerType(), True),
        StructField("NCLASS", IntegerType(), True),
        StructField("CAT", IntegerType(), True),
        StructField("SUBCAT", IntegerType(), True),
        StructField("CMADE", IntegerType(), True),
        StructField("CRECEIVE", IntegerType(), True),
        StructField("RATIOCIT", DoubleType(), True),
        StructField("GENERAL", DoubleType(), True),
        StructField("ORIGINAL", DoubleType(), True),
        StructField("FWDAPLAG", DoubleType(), True),
        StructField("BCKGTLAG", DoubleType(), True),
        StructField("SELFCTUB", DoubleType(), True),
        StructField("SELFCTLB", DoubleType(), True),
        StructField("SECDUPBD", DoubleType(), True),
        StructField("SECDLWBD", DoubleType(), True)
    ])

    _country_schema = StructType([
        StructField("CODE", StringType(), True),
        StructField("NAME", StringType(), True)
    ])

    def __init__(self, spark, path, schema, header="true"):
        raise NotImplementedError("Loader is an abstract class")
    
    @staticmethod
    def load(spark, path, schema, header="true"):
        df = spark.read.format("csv")\
            .option("header", header)\
            .option("mode", "FAILFAST")\
            .option("sep", ",")\
            .option("inferSchema", "false")\
            .option("nullValue", "")\
            .option("lineSep", "\n")\
            .option("escape", "\"")\
            .schema(schema)\
            .load(path)
        
        # We will use primitive DFs multiple times, so we can cache them
        df.cache()
        return df

    @staticmethod
    def cite_from_file(spark, path, schema=_cite_schema):
        return DFInterface.load(spark, path, schema)
    
    @staticmethod
    def apat_from_file(spark, path, schema=_apat_schema):
        return DFInterface.load(spark, path, schema)
    
    @staticmethod
    def country_from_file(spark, path, schema=_country_schema):
        return DFInterface.load(spark, path, schema, header="false")
    
    @staticmethod
    def save_df(df, path):
        """Guarda un DataFrame en formato parquet con compresión gzip"""
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("compression", "gzip")\
            .save(path)

def ejercicio_uno_a(spark, cite):
    # Cargamos el DataFrame de citas
    dfCite = DFInterface.cite_from_file(spark, cite)

    # Agrupamos por la patente citada y contamos el número de citas
    dfCitas = dfCite.groupBy("CITED")\
        .count()\
        .withColumnRenamed("CITED", "NPatente")\
        .withColumnRenamed("count", "ncitas")
    
    return dfCitas

def ejercicio_uno_b(spark, apat):
    # Cargamos el DataFrame de información de las patentes
    dfApat = DFInterface.apat_from_file(spark, apat)

    # Seleccionamos las columnas que nos interesan
    dfInfo = dfApat.select("PATENT", "COUNTRY", "GYEAR")\
        .withColumnRenamed("PATENT", "NPatente")\
        .withColumnRenamed("COUNTRY", "Pais")\
        .withColumnRenamed("GYEAR", "Anho")
    
    return dfInfo

def main():
    # Comprueba el número de argumentos
    # sys.argv[1] es el primer argumento, sys.argv[2] el segundo, etc.
    if len(sys.argv) != 5:
        print("Usar: p1.py cite75_99.txt apat63_99.txt dfCitas.parquet dfInfo.parquet")
        exit(-1)
    
    cite_file = sys.argv[1]
    apat_file = sys.argv[2]
    dfCitas_file = sys.argv[3]
    dfInfo_file = sys.argv[4]

    # Asumimos que se nos van a dar los parametros master, driver-memory y num-executors
    spark = SparkSession\
        .builder\
        .appName("Practica Spark de Xabier Iglesias, ejercicio 1")\
        .getOrCreate()

    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")

    # Ejecutamos los ejercicios
    dfCitas = ejercicio_uno_a(spark, cite_file)
    dfInfo = ejercicio_uno_b(spark, apat_file)
    #Imprimimos las primeras filas de los DataFrames
    print("Citas de patentes")
    dfCitas.show()
    print("Información de patentes")
    dfInfo.show()
    # Guardamos los DataFrames en formato parquet
    print("Guardando los DataFrames en formato parquet")
    DFInterface.save_df(dfCitas, dfCitas_file)
    DFInterface.save_df(dfInfo, dfInfo_file)
    # Imprimimos el numero de particiones de cada DataFrame
    print("Número de particiones de dfCitas: ", dfCitas.rdd.getNumPartitions())
    print("Número de particiones de dfInfo: ", dfInfo.rdd.getNumPartitions())
  
    # Paramos la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()