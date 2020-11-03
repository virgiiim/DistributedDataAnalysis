# apertuna connessione per eseguire il file da shell
from pyspark import SparkContext
sc=SparkContext(appName="", master="local[*]") 
sc.setLogLevel("ERROR")
from pyspark.sql import SparkSession
spark = SparkSession.builder.appName("").config("","").getOrCreate()

# librerie utili
import pyspark.sql.functions as F
from pyspark.sql import Row
from pyspark.sql.functions import col
from datetime import datetime
from math import sqrt


####################################################################
####################################################################
# ANALISI PRELIMINARI: CONTROLLARE CHE PLACE COINCIDE CON COORDINATES, lo facciamo solo per un dataset

cnt = sc.accumulator(0)
# funzione che aggiunge uno al contatore ogni volta che c'è corrispondenza tra place e coordinates
# da applicare ad ogni riga del dataset
def coincide(tweet):
    global cnt
    x_list = []
    y_list = []
    for coord in tweet.place:
        x_list.append(coord[0])
        y_list.append(coord[1])
    x_max = max(x_list)
    y_max = max(y_list)
    x_min = min(x_list)
    y_min = min(y_list)
    x = tweet.coordinates[0]
    y = tweet.coordinates[1]
    if x >= x_min and x <= x_max and y >= y_min and y <= y_max:
        cnt += 1
        
df_prova = spark.read.json("georef-tweets-20151121.json")
df_prova = df_prova.select([df_prova.place.bounding_box.coordinates[0].alias("place"), df_prova.coordinates.coordinates.alias("coordinates")]).dropna()
df_prova.foreach(coincide)
n_tot = df_prova.count()
#print('Il numero totale di coincidenze è: ', cnt, ' su ', n_tot)

# OUTPUT:
# Il numero totale di coincidenze è:  81572  su  81844

# FINE ANALISI PRELIMINARI
################################################################
################################################################


# funzioni utili
def media(lista):
   n = len(lista)
   somma=0
   for x in lista:
	   somma +=x
   return float(somma)/n
# funzione che calcola il 'baricentro' di una lista di punti (punti inteso come coordinate)
def centroide(coord_list):
	x_list=[]
	y_list=[]
	for coord in coord_list:
		x_list.append(coord[0])
		y_list.append(coord[1])
	x = media(x_list)
	y = media(y_list)
	return (x,y)
# distanza euclidea tra due punti del piano
def distanza(coord1, coord2):
	return sqrt((coord1[0]-coord2[0])**2 + (coord1[1]-coord2[1])**2)
# per ogni punto di una lista di punti, distanza tra il punto e il punto successivo nella lista
def dist_tot(lista):
	dist=0.0
	for i in range(len(lista)-1):
		dist += distanza(lista[i], lista[i+1])
	return dist
#numero di spostamenti 
def spost(lista):
	count=0
	for x in range(len(lista)-1):
		if lista[x]!=lista[x+1]:
			count +=1
	return count
# data la stringa come nella colonna 'created_at', restituisce il datetime 
def tempo(stringa):
	time_split = stringa.split()
	result = time_split[0]+" "+time_split[1]+" "+time_split[2]+" "+time_split[3]+" "+time_split[5]
	date_result= datetime.strptime(result, '%a %b %d %H:%M:%S %Y')
	return date_result
# in input un dataframe, output = rdd con chiave id_utente e valore lista dei 'baricentri' dei place visitati in ordine cronologico
def lista_spost(df):
    df = df.select(["user.id", "created_at", df.place.bounding_box.coordinates[0].alias("coordinates")]).dropna()
    df = df.rdd
    df = df.map(lambda x: (x.id, [(tempo(x.created_at), centroide(x.coordinates))]))
    df_place = df.reduceByKey(lambda x,y: x+y)
    df_place = df_place.map(lambda x: (x[0], sorted(x[1])))
    df_place = df_place.map(lambda x: (x[0], [coppia[1] for coppia in x[1]]))	
    return df_place 

# creazione lista di tutti i nostri dataframe
df_list = [] 
df_list.append(spark.read.json("georef-tweets-20151121.json"))
df_list.append(spark.read.json("georef.json"))
df_list.append(spark.read.json("georef-tweets-20151123.json"))
df_list.append(spark.read.json("georef-tweets-20151124.json"))
df_list.append(spark.read.json("georef-tweets-20151125.json"))
df_list.append(spark.read.json("georef-tweets-20151126.json"))
df_list.append(spark.read.json("georef-tweets-20151127.json"))
df_list.append(spark.read.json("georef-tweets-20151128.json"))
df_list.append(spark.read.json("georef-tweets-20151129.json"))


###################################################################################################
###################################################################################################
#########################         ANALISI HASHTAG       ###########################################

# creazione dataframe (hashtag, frequenza)
rdd_list = []
for i in range(len(df_list)):
	tags = df_list[i].select("entities.hashtags.text")
   # RDD composto da tutti gli hashtag in minuscolo 
	tags = tags.rdd.flatMap(lambda row: [word.lower() for word in row.text])
   # RDD di (hashtag, frequenza)
	tags = tags.map(lambda row: (row,1)).reduceByKey(lambda x,y: x+y)
	rdd_list.append(tags)

new_rdd = sc.union(rdd_list)
new_rdd = new_rdd.reduceByKey(lambda x,y: x+y)
df_hash_freq = new_rdd.toDF(['tag', 'counts'])
df_hash_freq.show()
# con questo dataset abbiamo creato il wordcloud


# creazione dataset (hashtag, lista di frequenze per ogni giorno)
def esempio(row):
   n_giorni = 8
   valuelist = [0]*n_giorni
   valuelist[row[1][1]] = row[1][0]
   return (row[0], valuelist)

def list_union(list1, list2):
	return [list1[i] + list2[i] for i in range(len(list1))]

def filtering(row):
	count_tot = 0
	for i in range(len(row[1])):
		count_tot += row[1][i]
	return count_tot > 500

rdd_list = []
for i in range(len(df_list)):
	df = df_list[i].select( "entities.hashtags.text")
	tags = df.rdd.flatMap(lambda row: [word.lower() for word in row.text])
	tags = tags.map(lambda row: (row,(1, i))).reduceByKey(lambda x,y: (x[0]+y[0], x[1]))
	tags = tags.map(esempio)
   # tags è un RDD con chiave l'hashtag e valore una lista di 0, ad eccezione della posizione relativa al giorno preso 
   # in considerazione, in cui c'è la frequenza dell'hashtag
	rdd_list.append(tags)

new_rdd = sc.union(rdd_list)
new_rdd = new_rdd.reduceByKey(list_union)
new_rdd = new_rdd.filter(filtering)
df_hash_list = new_rdd.toDF(['tag', 'counts_list'])


# calcolo della varianza delle liste delle frequenze, maggiore è la varianza, maggiore è la probabilità che quell'hashtag rappresenti 
# un evento accaduto nei nostri nove giorni
rdd_lista_freq = df_hash_list.rdd.map(lambda row: (row.tag, [int(num) for num in row.counts_list]))

def calcolo_media(row):
    list_sum = 0
    for num in row[1]:
        list_sum += num
    return (row[0], row[1], list_sum/8)

#Calcolo della media
rdd_lista_freq = rdd_lista_freq.map(calcolo_media)

def calcolo_varianza(row):
    somma_scarti = 0
    for i in range(len(row[1])):
        somma_scarti += (row[1][i]-row[2])**2
    return (row[0], row[1], row[2], somma_scarti/len(row[1]))

rdd_lista_freq = rdd_lista_freq.map(calcolo_varianza)

df_hash_freq = rdd_lista_freq.toDF(['tag', 'counts_list', 'media', 'varianza'])
df_hash_freq.orderBy('varianza', ascending=False).show()


###################################################################################################
###################################################################################################
#######################         DATA PREPARATION          #########################################
###################################################################################################
###################################################################################################

# creazione dataframe con attributi id, spostamenti, distanza percorsa e posti visitati
lista_rdd = []
for df in df_list:
	lista_rdd.append(lista_spost(df))
unione_rdd = sc.union(lista_rdd)
data_spost = unione_rdd.reduceByKey(lambda x, y: x+y)   # -->RDD di tutti i dataset con (id_utente, lista_spost)
# trasformazione da RDD di coppie in RDD di Row, quindi in DataFrame
data_spost = data_spost.map(lambda x: Row(id = x[0], distanza = dist_tot(x[1]), posti = len(set(x[1])), spostamenti = spost(x[1]))).toDF()
data_spost.show()


# creazione dataframe con attributi id e numero medio di link per tweet
lista_rdd = []
for df in df_list:
	data = df.select("user.id", "entities.urls.url").dropna()
	lista_rdd.append(data.rdd.map(lambda row:(row.id, (len(row.url),1))))  

unione_rdd = sc.union(lista_rdd)
data_url = unione_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1])) # -->RDD di tutti i dataset con (id_utente, n_url_tot, n_tweet)
# trasformazione da RDD a DataFrame con attributi id e url per tweet
data_url = data_url.map(lambda row: Row(id=row[0], url_per_tweet=float(row[1][0]/row[1][1]))).toDF()


# creazione dataframe con attributi id e numero medio di hashtag per tweet, come sopra
lista_rdd = []
for df in df_list:
	data = df.select("user.id", "entities.hashtags.text").dropna()
	lista_rdd.append(data.rdd.map(lambda row:(row.id, (len(row.text),1))))

unione_rdd = sc.union(lista_rdd)
data_hashtag = unione_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
data_hashtag = data_hashtag.map(lambda row: Row(id=row[0], hash_per_tweet=float(row[1][0]/row[1][1]))).toDF()


# creazione dataframe con attributi id e numero medio di foto per tweet, come sopra
def my_len(row):
    try:
        lenght = len(row.extended_entities.media)
    except:
        lenght = 0
    return lenght
            
lista_rdd = []
for df in df_list:
   data = df.select("user.id", "extended_entities")
   lista_rdd.append(data.rdd.map(lambda row: (row.id, (my_len(row), 1))).filter(lambda row: row[0] != None))

unione_rdd = sc.union(lista_rdd)
data_media = unione_rdd.reduceByKey(lambda x, y: (x[0]+y[0], x[1]+y[1]))
data_media = data_media.map(lambda row: Row(id=row[0], media_per_tweet=float(row[1][0]/row[1][1]))).toDF()


# creazione dataframe degli user con una selezione di attributi
First = True
for df in df_list:
   if First:
       df_tot = df.select("user.id", "user.friends_count", "user.followers_count", "user.favourites_count", "user.listed_count", "user.statuses_count", "user.verified", "user.default_profile_image")
       First = False
   else:
       df = df.select("user.id", "user.friends_count", "user.followers_count", "user.favourites_count", "user.listed_count", "user.statuses_count", "user.verified", "user.default_profile_image")
       df_tot = df_tot.unionAll(df)
# A questo punto abbiamo un dataframe che rappresenta l'unione di tutti gli utenti presenti nei nostri 9 file, cioè con gli user ripetuti
# NB: Non possiamo semplicemente eliminare i duplicati perché gli attributi sono quasi tutti 'variabili', ad es tra un tweet e l'altro può cambiare il numero di follower
# Dobbiamo fare un raggruppamento per id e per gli attributi variabili abbiamo deciso di calcolarci il massimo
data_sel_attr = df_tot.groupBy("id").agg(F.max("favourites_count").alias("favourites_cnt"), F.max("friends_count").alias("friends_cnt"), F.max("followers_count").alias("followers_cnt"), F.max("listed_count").alias("listed_cnt"), F.max("statuses_count").alias("status_cnt"), F.max("default_profile_image").alias("image_prof"), F.max("verified").alias("verified"))

# Adesso dobbiamo fare il join di tutti questi dataframe
data_finale_utente = data_url.join(data_hashtag, on='id')
data_finale_utente = data_finale_utente.join(data_media, on='id')
data_finale_utente = data_finale_utente.join(data_sel_attr, on='id')

data_finale_utente.show()


###################################################################################################
###################################################################################################
###########################         CLUSTERING        #############################################
###################################################################################################
###################################################################################################

from numpy import array
from pyspark.mllib.clustering import KMeans

# clustering sugli attributi creati relativi alle posizioni, scartando coloro che non si muovono
cluster_spost = data_spost.select("distanza","spostamenti","posti").filter(col('distanza') > 0)
# KMeans vuole un RDD di array di float
cluster_spost = cluster_spost.rdd.map(lambda line: array([float(x) for x in line]))
# tutti e tre gli attributi sembrano avere una distribuzione esponenziale, quindi ne facciamo il logaritmo
from numpy import log
cluster_spost = cluster_spost.map(lambda line: log(line))
# tramite il grafico dell'SSE abbiamo notato che il k ideale è 3
cluster=KMeans.train(cluster_spost, 3)
center_list=cluster.clusterCenters
from numpy import exp
center_list = [exp(centr) for centr in center_list]
print(center_list)

# creazione del dataset (id_utente, cluster_spost)
def my_predict(record):
    if record.distanza == 0:
        tipo_cluster = 4
    else:
        tipo_cluster = cluster.predict(log(array([record.distanza, record.spostamenti, record.posti])))
    return Row(id = record.id, cluster_spost=tipo_cluster)

data_cluster_spost = data_spost.rdd.map(my_predict).toDF()
data_cluster_spost.groupby('cluster_spost').count().show()


# clustering sugli attributi relativi all'utente twitter, senza tenere in considerazione le distanze
data = data_finale_utente.dropna()
# conversione in float
data = data.rdd.map(lambda row: (row.id, float(row.friends_cnt), float(row.followers_cnt), float(row.favourites_cnt), float(row.listed_cnt), float(row.status_cnt), float(row.verified), float(row.image_prof), float(row.media_per_tweet), float(row.url_per_tweet), float(row.hash_per_tweet), float(row.followers_cnt) - float(row.friends_cnt)))
data_cl = data.map(lambda row: (row[1], row[2], row[3], row[4], row[5], row[6], row[7], row[8], row[9], row[10], row[11]))
# conversione in array
data_cl = data_cl.map(lambda line: array(line))
# calcolo media e std delle colonne per la normalizzazione
mean_list= array([data_cl.map(lambda row: row[i]).mean() for i in range(11)])
std_list= array([data_cl.map(lambda row: row[i]).stdev() for i in range(11)])
# normalizzazione
data_cl = data_cl.map(lambda row: (row-mean_list)/std_list)
# k = 6 ottenuto tramite studio del grafico dell'SSE
cluster=KMeans.train(data_cl, 6)
center_list=cluster.clusterCenters
center_list = [centr * std_list + mean_list for centr in center_list]
print(center_list)

# creazione del dataset (id_utente, cluster_utente)
def pred(record):
    dist_min = float('inf')
    cluster = 0
    for i in range(len(center_list)):
        dist = 0
        for j in range(1, len(record)):
            dist += (record[j] - center_list[i][j-1])**2
        if dist < dist_min:
            dist_min = dist
            cluster = i
    return (record[0], cluster)

data_cluster_utente = data.map(pred)
data_cluster_utente = data_cluster_utente.toDF(['id', 'cluster_label'])
data_cluster_utente.groupby('cluster_label').count().show()

# dataset con id, cluster spostamento e cluster utente, per vedere quanto è rappresentativo il clustering sugli spostamenti rispetto al clustering utenti
data_cluster_totale = data_cluster_utente.join(data_cluster_spost, on='id')


# creazione del dataset (id_utente, polarity, objectivity)
from textblob import TextBlob

rdd_list = []
# creazione RDD di (id, lista testi tweet)
for i in range(len(df_list)):
	id_text = df_list[i].select("user.id", "text")
	id_text = id_text.rdd.map(lambda row:(row.id, [row.text]))
	id_text = id_text.reduceByKey(lambda x,y: x+y)
	rdd_list.append(id_text)

new_rdd = sc.union(rdd_list)
new_rdd = new_rdd.reduceByKey(lambda x,y: x+y)
new_rdd = new_rdd.map(lambda row: Row(id = row[0], text_list = row[1]))

# sentiment analysis sui tweet della lista, per ogni utente, restituisce la polarity e la objectivity medie
def sentiment(row):
	n_tweet = len(row.text_list)
	pol = 0
	subj = 0
	for tweet in row.text_list:
		blob = TextBlob(tweet)
		try:
			blob = blob.translate(to='en')
		except:
			blob = blob
		sentiment = blob.sentiment
		pol += sentiment.polarity
		subj += sentiment.subjectivity
	pol = pol / n_tweet
	subj = subj / n_tweet
	return Row(id = row.id, polarity = pol, subjectivity = subj)

data_polarity = new_rdd.map(sentiment)
data_polarity = data_polarity.toDF()
data_polarity.show()


# join dei tre dataset precedenti
data_analisi = data_cluster_totale.join(data_polarity, on = 'id')
data_analisi.show()


###################################################################################################
# stampa dei risultati finali

# rollup dei cluster, ci dice come sono suddivisi i cluster degli utenti nei cluster degli spostamenti 
data_analisi.rollup('cluster_spost', 'cluster_label').count().orderBy('cluster_spost', 'cluster_label').show()

# print di polarity e subjectivity medie per cluster_spost
print(data_analisi.groupBy('cluster_spost').agg(F.avg('polarity').alias('polarity'), F.avg('subjectivity').alias('subjectivity').collect()))

# print di polarity e subjectivity medie per cluster_label (utente)
print(data_analisi.groupBy('cluster_label').agg(F.avg('polarity').alias('polarity'), F.avg('subjectivity').alias('subjectivity').collect()))