from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Ejercicio4")
sc = SparkContext(conf = conf)

ini = 90
fin = 5000

def condicion(l):
	t_s = l[3]
	return (ini <= int(t_s) <= fin)

#Input-Levantando el dataSet
text_file = sc.textFile("/user/hduser/TP-2/Datos")

#Operar
rdd_lineas = text_file.map(lambda linea: linea.split("\t"))
rdd_filtrado = data_set.filter(lambda linea: condicion(linea))
rdd_filtrado.count()