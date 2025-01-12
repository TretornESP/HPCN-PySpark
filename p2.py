#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function,division
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.types import DoubleType, LongType
import pyspark.sql.functions as F
import sys
import csv
#
#Script que, a partir de los datos en Parquet de la práctica anterior,
#obtenga para cada país y para cada año el total de patentes, el total de
# citas obtenidas por todas las patentes, la media de citas y el máximo número de citas.
# Obtener solo aquellos casos en los que existan valores en ambos ficheros (inner join).
# Adicionalmente, cada país tiene que aparecer con su nombre completo, obtenido del
# fichero country_codes.txt. El DataFrame generado tiene que ser como este:
#
#+-------------------+----+-----------+----------+------------------+--------+
#|Pais               |Anho|NumPatentes|TotalCitas|MediaCitas        |MaxCitas|
#+-------------------+----+-----------+----------+------------------+--------+
#|Algeria            |1963|2          |7         |3.5               |4       |
#|Algeria            |1968|1          |2         |2.0               |2       |
#|Algeria            |1970|1          |2         |2.0               |2       |
#|Algeria            |1972|1          |1         |1.0               |1       |
#|Algeria            |1977|1          |2         |2.0               |2       |
#|Andorra            |1987|1          |3         |3.0               |3       |
#|Andorra            |1993|1          |1         |1.0               |1       |
#|Andorra            |1998|1          |1         |1.0               |1       |
#|Antigua and Barbuda|1978|1          |6         |6.0               |6       |
#|Antigua and Barbuda|1979|1          |14        |14.0              |14      |
#|Antigua and Barbuda|1991|1          |8         |8.0               |8       |
#|Antigua and Barbuda|1994|1          |19        |19.0              |19      |
#|Antigua and Barbuda|1995|2          |12        |6.0               |11      |
#|Antigua and Barbuda|1996|2          |3         |1.5               |2       |
#|Argentina          |1963|14         |35        |2.5               |7       |
#|Argentina          |1964|20         |60        |3.0               |8       |
#|Argentina          |1965|10         |35        |3.5               |10      |
#|Argentina          |1966|16         |44        |2.75              |9       |
#|Argentina          |1967|13         |60        |4.615384615384615 |14      |
#
# Ejecutar en local con:
# spark-submit --master local[*] --driver-memory 4g p2.py dirNcitas dirInfo path_a_country_codes_en_local.txt p2out
# Ejecución en un cluster YARN:
# spark-submit --master yarn --num-executors 8 --driver-memory 4g --queue urgent p2.py dirNcitas dirInfo path_a_country_codes_en_local.txt p2out

class DFInterface:
    _patentes_schema = StructType([
        StructField("Npatente", IntegerType(), True),
        #Esto casca si usamos int...
        StructField("Ncitas", LongType(), True)
    ])

    _datos_patentes_schema = StructType([
        StructField("Npatente", IntegerType(), True),
        StructField("Pais", StringType(), True),
        StructField("Anho", IntegerType(), True),
    ])

    _country_schema = StructType([
        StructField("Code", StringType(), True),
        StructField("Name", StringType(), True)
    ])

    def __init__(self, spark, path, schema, header="true"):
        raise NotImplementedError("Loader is an abstract class")
    
    @staticmethod
    def load(spark, path, schema, mode="parquet"):
        df = spark.read.format(mode).option("mode", "FAILFAST").schema(schema).load(path)
        
        # We will use primitive DFs multiple times, so we can cache them
        df.cache()
        return df

    @staticmethod
    def patentes_from_parquet(spark, path, schema=_patentes_schema):
        return DFInterface.load(spark, path, schema)
    
    @staticmethod
    def datos_patentes_from_parquet(spark, path, schema=_datos_patentes_schema):
        return DFInterface.load(spark, path, schema)
    
    @staticmethod
    def country_from_file(sc, path, schema=_country_schema):
        with open(path, 'r') as f:
            d = dict(filter(None, csv.reader(f, delimiter='\t')))
        return sc.broadcast(d)
    
    @staticmethod
    def save_df(df, path):
        """Guarda un DataFrame en formato parquet con compresión gzip"""
        df.write.format("parquet")\
            .mode("overwrite")\
            .option("compression", "gzip")\
            .save(path)
    
    @staticmethod
    def save_csv(df, path):
        """Guarda un DataFrame en formato csv, sin comprimir y con cabeceras"""
        df.write.format("csv").mode("overwrite").option("header", "true").save(path)

def ejercicio_dos(spark, patentes, datos_patentes, country_codes):
    #replace de los códigos de país por el nombre del país
    datos_patentes = datos_patentes.replace(to_replace=country_codes.value, subset=["PAIS"])

    # Join de los datos de patentes con los datos de patentes, el inner es redundante,
    # lo ponemos por claridad
    df = patentes.join(datos_patentes, "Npatente", "inner")

    # Agrupamos por país y año
    df = df.groupBy("Pais", "Anho")
    # Agregamos las columnas de estadísticas
    df = df.agg(
        F.count("Npatente").alias("NumPatentes"),
        F.sum("Ncitas").alias("TotalCitas"),
        F.avg("Ncitas").alias("MediaCitas"),
        F.max("Ncitas").alias("MaxCitas")
    )
    # Ordenamos por país y año
    df = df.orderBy("Pais", "Anho")
    
    return df

def main():
    # Comprueba el número de argumentos
    # sys.argv[1] es el primer argumento, sys.argv[2] el segundo, etc.
    if len(sys.argv) != 5:
        print("Usar: p2.py dirNcitas dirInfo path_a_country_codes_en_local.txt p2out")
        exit(-1)
    
    dirNcitas_file = sys.argv[1]
    dirInfo_file = sys.argv[2]
    country_code_file = sys.argv[3]
    outfile = sys.argv[4]

    # Asumimos que se nos van a dar los parametros master, driver-memory y num-executors
    spark = SparkSession\
        .builder\
        .appName("Practica Spark de Xabier Iglesias, ejercicio 2")\
        .getOrCreate()

    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")
    sc = spark.sparkContext

    # Cargamos los DataFrames de patentes y datos de patentes
    dfNcitas = DFInterface.patentes_from_parquet(spark, dirNcitas_file)
    dfInfo = DFInterface.datos_patentes_from_parquet(spark, dirInfo_file)
    country_codes = DFInterface.country_from_file(sc, country_code_file)

    # Ejecutamos la función del ejercicio
    df = ejercicio_dos(spark, dfNcitas, dfInfo, country_codes)

    # Mostramos el resultado
    df.show()

    #Guardamos el resultado
    DFInterface.save_csv(df, outfile)
    
    # Paramos la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()