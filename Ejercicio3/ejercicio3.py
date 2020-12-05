from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Ejercicio3")
sc = SparkContext(conf = conf)


def condicion(l):
	linea = l[4]
	linea = linea.strip("u")
	return (len(linea) > 0)

#Input-Levantando el dataSet
text_file = sc.textFile("/user/hduser/TP-2/Datos")

#Operar
rdd_lineas = text_file.map(lambda linea: linea.split("\t"))
data_set = rdd_lineas.filter(lambda linea: condicion(linea))
rdd_destinos = rdd_filtrado.map(lambda linea: ((linea[1] , linea[2]), 1))
rdd_resumen = rdd_destinos.reduceByKey(lambda x,y: x + y )
resumen = rdd_resumen.sortBy(lambda linea: linea[1], ascending=False)
resumen.top(10)