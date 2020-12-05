from pyspark import SparkConf, SparkContext

conf = SparkConf().setMaster("local").setAppName("Ejercicio5")
sc = SparkContext(conf = conf)

duracion = 3000
def condicion(l, ini, fin):
	t_s = l[3]
	return (ini <= int(t_s) <= fin)

#Input-Levantando el dataSet
text_file = sc.textFile("/user/hduser/TP-2/Datos")

#Dataset
dataset = text_file.map(lambda linea: linea.split("\t"))

#Obtener maximo
rdd_timestamps = dataset.map(lambda linea: linea[3])
maximo = rdd_timestamps.reduce(lambda x,y: max(int(x), int(y)))

#Operar
ini = 0
fin = duracion
while fin <= maximo:
	rdd_franja = dataset.filter(lambda linea: condicion(linea,ini,fin))
	count = rdd_franja.count()
	resumen = rdd_franja.map(lambda linea: ((ini,fin), count))
	franja_count = resumen.reduceByKey(lambda x,y: x)
	franja_count = resumen.takeSample(True,1)
	print(franja_count)
	ini = fin
	fin = fin + duracion

	if fin > maximo:
		fin = maximo
	if fin == maximo:
		break