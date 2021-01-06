from graphframes import *
from functools import reduce
from pyspark import *
from pyspark.sql import *
from operator import add
import time
import sys

spark = SparkSession.builder.appName('fun').getOrCreate() 

from pyspark.sql.types import Row
import uuid
from pyspark.sql.functions import monotonically_increasing_id
from uuid import UUID

def parse_vert(x, delimit):
    hyper_return =  x.value.split(delimit)
    try:
        hyper_return.remove("")
    except:
        pass
    for v in hyper_return:
        yield v,"vertex"

def parse_edge(x, delimit):
    hyper_return =  x.value.split(delimit)
    try:
        hyper_return.remove("")
    except:
        pass
    for i in range(len(hyper_return)):
        for j in range(i+1,len(hyper_return)):
            yield hyper_return[i],hyper_return[j]
            yield hyper_return[j],hyper_return[i]

def init_graph(graph_file):
    vertices = spark.read.text(graph_file).rdd.flatMap(lambda x: parse_vert(x, " "))#.toDF(["id","type"])
    edges = spark.read.text(graph_file).rdd.flatMap(lambda x: parse_edge(x, " "))#.toDF(["id","type"])
    df_vert = vertices.map(lambda x:(x[0],x[1])).toDF(["id","type"]).dropDuplicates()
    df_edge = edges.map(lambda x:(x[0],x[1])).toDF(["src","dst"]).dropDuplicates()
    #print("vertices collect",vertices.collect())
    #print("edges collect", edges.collect()) 
    df_vert.show()
    df_edge.show()
    new_g = GraphFrame(df_vert, df_edge)
    return new_g

start_time = time.time()
new_g = init_graph(sys.argv[1])
end_time = time.time()
exec_start = time.time()
results2 = new_g.pageRank(resetProbability=0.15, maxIter=sys.argv[2])
exec_end = time.time()

print(results2.vertices.show())
results2.vertices.write.csv(sys.argv[1] + "clique.csv")

print("--- Execution Time: %s seconds ---" % (end_time - start_time))
print("--- Partition Time: %s seconds ---" % (exec_end - exec_start))
