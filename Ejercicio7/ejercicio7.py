from pyspark import SparkConf, SparkContext
import math
import sys

conf = SparkConf().setMaster("local").setAppName("Ejercicio7")
sc = SparkContext(conf = conf)

arg1 = sys.argv[1]

def distancia(key, l):
	x1 = int(l[0])
	y1 = int(l[1])
	x2 = int(l[3])
	y2 = int(l[4])
	resta1 = x2-x1
	resta2 = y2-y1
	suma = (resta1**2) + (resta2**2)
	res = math.sqrt(suma)
	return (key, (l[0], l[1], l[2], l[3], l[4], res))

def promedio(t1, t2):
	x = [ int(t1[0]), int(t2[0]) ]
	y = [ int(t1[1]), int(t2[1]) ]
	promLat = sum(x)/2
	promLong = sum(y)/2
	return (promLat, promLong)

def minimo(t1, t2):
	if t1[5] < t2[5]:
		return (t1[0], t1[1], t1[2], t1[3], t1[4], t1[5])
	else:
		return (t2[0], t2[1], t2[2], t2[3], t2[4], t2[5])

#Input-Levantando el dataSet
text_file = sc.textFile(arg1)

#Dataset
dataset = text_file.map(lambda linea: linea.split("\t"))
rdd_dataset=dataset.map(lambda linea: ((linea[0], linea[3]), linea[1], linea[2]) )

#Centroides
lista_centroides=[ ('1', (5,20)), ('2', (20,40)), ('3', (40,55)), ('4', (55,70)), ('5', (70,100)) ]
rdd_centroides = sc.parallelize(lista_centroides)
k = 5
for x in range(1,k):
	#producto cartesiano: agrego cada centroide y su posicion para cada linea
	rdd_posicion = rdd_dataset.cartesian(rdd_centroides)
	#creamos la siguiente tupla con el fin de organizar los datos: key=(id_v,t_s)	value=(lat,lon,id_c,lat_c,lon_c)
	rdd_posicion_centroide = rdd_posicion.map(lambda l: ((l[0][0]), (l[0][1], l[0][2], l[1][0], l[1][1][0], l[1][1][1])) )
	#agregamos para cada tupla la distancia hacia el centroide
	rdd_distancias = rdd_posicion_centroide.map(lambda l: distancia(l[0],l[1]) )
	#nos quedamos para cada key=(id_v, t_s) el que tenga la distancia menor, asociando asi cada tupla con su centroide
	rdd_centroides_asociados = rdd_distancias.reduceByKey(lambda t1,t2: minimo(t1,t2) )
	#creamos la siguiente tupla con el fin de organizar y descartar datos: key=id_c value=(lat,lon)
	rdd_clusters = rdd_centroides_asociados.map(lambda l: (l[1][2], (l[1][0], l[1][1])) )
	#cantidad por centroides
	cantidades_por_centroides = rdd_clusters.countByKey().items()
	print(cantidades_por_centroides)
	#nuevos centroides: calculamos para cada centroide su promedio de lat y long. Se ordena
	rdd_centroides = rdd_clusters.reduceByKey(lambda t1, t2: promedio(t1,t2)).sortByKey()