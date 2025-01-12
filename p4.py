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
# Obtener a partir de los fichero Parquet creados en la practica 1 un DataFrame
# que proporcione, para un grupo de países especificado, las patentes ordenadas
# por número de citas, de mayor a menor, junto con una columna que indique el
# rango (posición de la patente en esa país/año según las citas obtenidas.
#
# La ejecución y salida del script debe ser como sigue:
#
#....
#+----+----+--------+------+-----+
#|Pais|Anho|Npatente|Ncitas|Rango|
#+----+----+--------+------+-----+
#|ES  |1963|3093080 |20    |1    |
#|ES  |1963|3099309 |19    |2    |
#|ES  |1963|3081560 |9     |3    |
#|ES  |1963|3071439 |9     |3    |
#|ES  |1963|3074559 |6     |4    |
#|ES  |1963|3114233 |5     |5    |
#|ES  |1963|3094845 |4     |6    |
#|ES  |1963|3106762 |3     |7    |
#|ES  |1963|3088009 |3     |7    |
#|ES  |1963|3087842 |2     |8    |
#|ES  |1963|3078145 |2     |8    |
#|ES  |1963|3094806 |2     |8    |
#|ES  |1963|3073124 |2     |8    |
#|ES  |1963|3112201 |2     |8    |
#|ES  |1963|3102971 |1     |9    |
#|ES  |1963|3112703 |1     |9    |
#|ES  |1963|3095297 |1     |9    |
#|ES  |1964|3129307 |11    |1    |
#|ES  |1964|3133001 |10    |2    |
#|ES  |1964|3161239 |8     |3    |
#.................................
#|FR  |1963|3111006 |35    |1    |
#|FR  |1963|3083101 |22    |2    |
#|FR  |1963|3077496 |16    |3    |
#|FR  |1963|3072512 |15    |4    |
#|FR  |1963|3090203 |15    |4    |
#|FR  |1963|3086777 |14    |5    |
#|FR  |1963|3074344 |13    |6    |
#|FR  |1963|3096621 |13    |6    |
#|FR  |1963|3089153 |13    |6    |
#.................................
#
# Ejecutar en local con:
# spark-submit --master local[*] --driver-memory 4g p4.py dfCitas.parquet dfInfo.parquet FR,ES outdir
# Ejecución en un cluster YARN:
# spark-submit --master yarn --num-executors 8 --driver-memory 4g --queue urgent p4.py dfCitas.parquet dfInfo.parquet FR,ES outdir

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
    def save_csv(df, path):
        """Guarda un DataFrame en formato csv, sin comprimir y con cabeceras"""
        df.write.format("csv").mode("overwrite").option("header", "true").save(path)

def ejercicio_cuatro(dfNcitas, dfInfo, country_list):
    # Hacemos un join de los dos DataFrames, nos quedamos con el NPATENTE de dfNcitas
    df = dfNcitas.join(dfInfo, dfNcitas.Npatente == dfInfo.Npatente)

    # Nos quedamos solo con las columnas que nos interesan, referenciamos dfInfo.NPATENTE
    df = df.select(dfInfo.Pais, dfInfo.Anho, dfInfo.Npatente, dfNcitas.Ncitas)

    # Creamos una ventana para cada país y año
    window = Window.partitionBy("Pais", "Anho").orderBy(F.desc("Ncitas"))

    # Filtramos por los países que nos interesan
    df = df.filter(df.Pais.isin(country_list))

    # Añadimos una columna con el rango
    df = df.withColumn("Rango", F.rank().over(window))

    # Ordenamos por país, año(ascedente) y número de citas(descendente)
    df = df.orderBy("Pais", "Anho", F.desc("Ncitas"))

    return df

def main():
    # Comprueba el número de argumentos
    # sys.argv[1] es el primer argumento, sys.argv[2] el segundo, etc.
    if len(sys.argv) != 5:
        print("Usar: p4.py dirNcitas dirInfo C1,C2,C3,... p2out")
        exit(-1)
    
    dirNcitas_file = sys.argv[1]
    dirInfo_file = sys.argv[2]
    country_list = sys.argv[3].split(",")
    outfile = sys.argv[4]

    # Asumimos que se nos van a dar los parametros master, driver-memory y num-executors
    spark = SparkSession\
        .builder\
        .appName("Practica Spark de Xabier Iglesias, ejercicio 4")\
        .getOrCreate()

    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")
    sc = spark.sparkContext

    # Cargamos los DataFrames de patentes y datos de patentes
    dfNcitas = DFInterface.patentes_from_parquet(spark, dirNcitas_file)
    dfInfo = DFInterface.datos_patentes_from_parquet(spark, dirInfo_file)

    # Ejecutamos la función del ejercicio
    df = ejercicio_cuatro(dfNcitas, dfInfo, country_list)

    # Mostramos el dataframe completo
    df.show()

    #Guardamos el resultado
    DFInterface.save_csv(df, outfile)
    
    # Paramos la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()