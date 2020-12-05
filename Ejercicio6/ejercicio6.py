from pyspark import SparkConf, SparkContext
import sys

arg1 = sys.argv[1] 
arg2 = sys.argv[2]

conf = SparkConf().setMaster("local").setAppName("Ejercicio6")
sc = SparkContext(conf = conf)

#Input-Levantando el dataSet
text_file = sc.textFile("/user/hduser/TP-2/Datos")

#Dataset
dataset = text_file.map(lambda linea: linea.split("\t"))

#Operar
rdd_lat = dataset.map(lambda linea: (linea[1],1))
resumen_lat = rdd_lat.reduceByKey(lambda x,y: x+y)
rdd_lon = dataset.map(lambda linea: (linea[2],1))
resumen_lon = rdd_lon.reduceByKey(lambda x,y: x+y)

#Impresion
resumen_lat.takeOrdered(1, key = lambda x:-x[1])
resumen_lon.takeOrdered(1, key = lambda x:-x[1])