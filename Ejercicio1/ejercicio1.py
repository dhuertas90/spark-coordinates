from pyspark import SparkConf, SparkContext
import sys

arg1 = sys.argv[1] 
arg2 = sys.argv[2]

conf = SparkConf().setMaster("local").setAppName("Ejercicio1")
sc = SparkContext(conf = conf)


def condicion(l):
	linea = l[4]
	linea = linea.strip("u")
	return (len(linea) > 0)

#Input-Levantando el dataSet
text_file = sc.textFile(arg1)

#Operar
rdd_lineas = text_file.map(lambda linea: linea.split("\t"))
data_set = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_group = data_set.map(lambda linea: (linea[0], 1))
resumen = rdd_group.reduceByKey(lambda x,y: x + y )

#Output-Salida en archivo
resumen.saveAsTextFile(arg2)