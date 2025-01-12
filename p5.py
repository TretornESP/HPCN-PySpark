#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function,division
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType
from pyspark.sql.types import DoubleType, LongType
from pyspark.sql.window import Window
import pyspark.sql.functions as F
import sys
import csv
#
# Obtener a partir del fichero Parquet con la información de (Npatente, Pais y Año)
# un DataFrame que nos muestre el número de patentes asociadas a cada país por cada
# década (entendemos por década los años del 0 al 9, es decir de 1970 a 1979 es una década).
# Adicionalmente, debe mostrar el aumento o disminución del número de patentes para
# cada país y década con respecto al la década anterior.
#
# El DataFrame generado tiene que ser como este:
#
# La ejecución y salida del script debe ser como sigue:
#
#+----+------+---------+----+
#|Pais|Decada|NPatentes|Dif |
#+----+------+---------+----+
#|AD  |1980  |1        |0   |
#|AD  |1990  |5        |4   |
#|AE  |1980  |7        |0   |
#|AE  |1990  |11       |4   |
#|AG  |1970  |2        |0   |
#|AG  |1990  |7        |5   |
#|AI  |1990  |1        |0   |
#|AL  |1990  |1        |0   |
#|AM  |1990  |2        |0   |
#|AN  |1970  |1        |0   |
#|AN  |1980  |2        |1   |
#|AN  |1990  |5        |3   |
#|AR  |1960  |135      |0   |
#|AR  |1970  |239      |104 |
#|AR  |1980  |184      |-55 |
#|AR  |1990  |292      |108 |
#
# Ejecutar en local con:
# spark-submit --master local[*] --driver-memory 4g p5.py dfInfo.parquet p5out
# Ejecución en un cluster YARN:
# spark-submit --master yarn --num-executors 8 --driver-memory 4g --queue urgent p5.py dfInfo.parquet p5out

class DFInterface:
    _datos_patentes_schema = StructType([
        StructField("NPATENTE", IntegerType(), True),
        StructField("Pais", StringType(), True),
        StructField("ANHO", IntegerType(), True),
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
    def datos_patentes_from_parquet(spark, path, schema=_datos_patentes_schema):
        return DFInterface.load(spark, path, schema)
            
    @staticmethod
    def save_csv(df, path):
        """Guarda un DataFrame en formato csv, sin comprimir y con cabeceras"""
        df.write.format("csv").mode("overwrite").option("header", "true").save(path)

def ejercicio_cinco(dfInfo):
    # Creamos una columna con la década
    df = dfInfo.withColumn("Decada", F.floor(dfInfo["ANHO"]/10)*10)

    # Agrupamos por país y década y contamos el número de patentes
    df = df.groupBy("Pais", "Decada").count().withColumnRenamed("count", "NPatentes")

    # Creamos una ventana para poder comparar con la década anterior
    window = Window.partitionBy("Pais").orderBy("Decada")

    # Añadimos una columna con el número de patentes de la década anterior
    df = df.withColumn("Anterior", F.lag("NPatentes", 1).over(window))

    # Calculamos la diferencia con la década anterior
    df = df.withColumn("Dif", F.when(F.isnull(df["Anterior"]), 0).otherwise(df["NPatentes"] - df["Anterior"]))

    # Nos quedamos con las columnas que nos interesan
    df = df.select("Pais", "Decada", "NPatentes", "Dif")

    return df

def main():
    # Comprueba el número de argumentos
    # sys.argv[1] es el primer argumento, sys.argv[2] el segundo, etc.
    if len(sys.argv) != 3:
        print("Usar: p5.py dirInfo_file outfile")
        exit(-1)
    
    dirInfo_file = sys.argv[1]
    outfile = sys.argv[2]

    # Asumimos que se nos van a dar los parametros master, driver-memory y num-executors
    spark = SparkSession\
        .builder\
        .appName("Practica Spark de Xabier Iglesias, ejercicio 5")\
        .getOrCreate()

    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")
    sc = spark.sparkContext

    # Cargamos los DataFrames de patentes y datos de patentes
    dfInfo = DFInterface.datos_patentes_from_parquet(spark, dirInfo_file)

    # Ejecutamos la función del ejercicio
    df = ejercicio_cinco(dfInfo)

    # Mostramos el dataframe completo
    df.show()

    #Guardamos el resultado
    DFInterface.save_csv(df, outfile)
    
    # Paramos la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()