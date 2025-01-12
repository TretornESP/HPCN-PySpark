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
#A partir del fichero apat63_99.txt obtener el número de patentes por país y año
#usando RDDs (no uséis DataFrames). El RDD obtenido debe ser un RDD clave/valor,
#en el cual la clave es un país y el valor una lista de tuplas, en la que cada
#tupla esté formada por un año y el número de patentes de ese país en ese año.
# 
#Adicionalmente, el RDD creado debería estar ordenado por el código del país
#y, para cada país, la lista de valores ordenados por año.
#
#Ejemplo de par clave/valor de salida
#
# (u'PA', [(u'1963', 2), (u'1964', 2), (u'1965', 1), (u'1966', 1), (u'1970', 1),
# (u'1971', 1), (u'1972', 6), (u'1974', 3), (u'1975', 5), (u'1976', 3),
# (u'1977', 2), (u'1978', 2), (u'1980', 2), (u'1982', 1), (u'1983', 1),
# (u'1985', 2), (u'1986', 1), (u'1987', 2), (u'1988', 1), (u'1990', 1),
# (u'1991', 2), (u'1993', 1), (u'1995', 1), (u'1996', 1), (u'1999', 1)])
#
# Ejecutar en local con:
# spark-submit --master local[*] --driver-memory 4g p3.py path_to_apat63_99.txt p3out
# Ejecución en un cluster YARN:
# spark-submit --master yarn --num-executors 8 --driver-memory 4g --queue urgent p3.py path_to_apat63_99.txt p3out

class RDDInterface:
    def __init__(self, spark):
        raise NotImplementedError("Loader is an abstract class")
        
    @staticmethod
    def apat_from_file(sc, path):
        #leemos el fichero con 8 particiones

        #Aqui la documentacion nos dice que usemos textFile(path, N)
        #para el numero de particiones, sin embargo esto es una sugerencia.

        #La forma de hacerle un enforcement, es con repartition
        rdd = sc.textFile(path).map(lambda x: x.split(","))
        return rdd.repartition(8)
    
    @staticmethod
    #No se especifica el formato de salida, por lo que asumimos que es texto
    def save_as_text(rdd, path):
        rdd.saveAsTextFile(path)
            
def ejercicio_3(rdd):
    
    # Eliminamos las comillas de los campos
    rdd = rdd.map(lambda x: [y.replace('"', '') for y in x])

    # Por cada país y año, contamos el número de patentes
    rdd = rdd.map(lambda x: ((x[4], x[1]), 1))\
        .reduceByKey(lambda x, y: x + y)\
        .map(lambda x: (x[0][0], (x[0][1], x[1])))\
        .groupByKey()\
        .sortByKey()\
        .mapValues(lambda x: sorted(x, key=lambda y: y[0]))
    
    # Explicacion paso a paso del bloque anterior:
    # 1. Para cada patente (lambda sobre x), obtenemos una tupla
    #    con las columnas 4 y 1 (COUNTRY y GYEAR) como clave y 1 como valor
    #    es como un contador, vamos
    # 2. Sumamos los valores para cada clave, como todos son 1, estamos contando el total de patentes por país y año
    # 3. Cambiamos la clave a solo el país, y la tupla a una tupla de año y número de patentes
    # 4. Agrupamos por clave, que es el país
    # 5. Ordenamos por clave (el país)
    # 6. Ordenamos los valores por año
    
    # Imprimimos los primeros 10 elementos
    print(rdd.take(10))

    return rdd

def main():
    # Comprueba el número de argumentos
    # sys.argv[1] es el primer argumento, sys.argv[2] el segundo, etc.
    if len(sys.argv) != 3:
        print("Usar: p3.py path_to_apat63_99.txt p3out")
        exit(-1)
    
    apat_file = sys.argv[1]
    dirOutput = sys.argv[2]

    # Asumimos que se nos van a dar los parametros master, driver-memory y num-executors
    spark = SparkSession\
        .builder\
        .appName("Practica Spark de Xabier Iglesias, ejercicio 3")\
        .getOrCreate()

    # Cambio la verbosidad para reducir el número de
    # mensajes por pantalla
    spark.sparkContext.setLogLevel("FATAL")
    sc = spark.sparkContext
    
    # Cargamos el RDD de información de las patentes
    raw_rdd = RDDInterface.apat_from_file(sc, apat_file)

    # Ejecutamos el ejercicio
    rdd = ejercicio_3(raw_rdd)

    # Imprimimos el resultado
    print(rdd.collect())

    # Guardamos el resultado
    RDDInterface.save_as_text(rdd, dirOutput)

    # Paramos la sesión de Spark
    spark.stop()

if __name__ == "__main__":
    main()