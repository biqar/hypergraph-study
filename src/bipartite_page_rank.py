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

def f(x, uuid):
    d = {}
    elements = x.value.split(" ")
    for i in range(len(elements)):
        yield elements[i],uuid
        yield uuid, elements[i]
    #for i in range(len(x)):
    #    d["abc"] = x[i]
    #return d

def make_edge_df(edge):
    d = {}
    d["src"] = edge[0]
    d["dst"] = edge[1]
    return d

def make_vertex_df(edge):
    yield edge.src,"vertex"
    yield edge.dst, "vertex"
    
        
def hyp_df(x, delimit):
    hyper_return =  x.value.split(delimit)
    try:
        hyper_return.remove("")
    except:
        pass
    return (hyper_return),1

def parse_hyp(x):
    hypid = "hyp" + str(x[len(x)-1])
    for i in range(len(x[0])):
        yield x[0][i],hypid
        yield hypid,x[0][i]

def parse_vert(x):
    hypid = "hyp" + str(x[len(x)-1])
    yield hypid,"hyperedge"
    for i in range(len(x[0])):
        yield x[0][i],"vertex"

def broadcast_hyp(x):
    hypid = "hyp" + str(x[len(x)-1])
    ar = []
    for i in range(len(x[0])):
        ar.append(x[0][i])
    
    return hypid,ar

def init_graph():
    #hyp_lines = spark.read.text("com-friendster.top5000.cmty.txt").rdd.map(lambda x: hyp_df(x, "\t")).toDF()
    #hyp_lines = spark.read.text("com-orkut.top5000.cmty.txt").rdd.map(lambda x: hyp_df(x, "\t")).toDF()
    hyp_lines = spark.read.text(sys.argv[1]).rdd.map(lambda x: hyp_df(x, " ")).toDF()
    hyp_lines.show()
    hyp_lines = hyp_lines.withColumn("id", monotonically_increasing_id())
    hyp_lines.show()

    df_edge = hyp_lines.rdd.flatMap(lambda x: parse_hyp(x)).toDF(["src","dst"])
    df_edge.show()
    df_vert = hyp_lines.rdd.flatMap(lambda x: parse_vert(x)).toDF(["id","type"]).dropDuplicates()
    df_hyp = hyp_lines.rdd.map(lambda x: broadcast_hyp(x))
    print("df edge",df_edge.show())
    print("df vert",df_vert.show())
    #print("df hyp", df_hyp.collectAsMap())
    new_g = GraphFrame(df_vert, df_edge)
    new_hyperedges = df_hyp.collectAsMap()
    return new_g, new_hyperedges

p_start_time = time.time()
new_g,new_hyperedges = init_graph()
p_end_time = time.time()
print(new_g)

def initialize_page_rank(vertex, count):
    return 1/count

def parseNeighbors(edge):
    dest_id = edge.dst
    if edge.dst in new_hyperedges:
        return edge.src, new_hyperedges[edge.dst]
    return edge.src,[]

def getContributions(ur):
    len_urls = len(ur[1][0])
    if len_urls == 0:
        len_urls = 1
    for site in sum(ur[1][0],[]):
        yield site, ur[1][1]/(len_urls)

def parallel_pagerank(iterations, graph):
    iterations = int(iterations)
    all_vertices =graph.vertices.filter('type=="vertex"')
    num_vertices = all_vertices.count()
    all_edges = graph.edges
    all_vertices.show()
    all_edges.show()
    ranks = all_vertices.rdd.map(lambda x:(x.id,initialize_page_rank(x, num_vertices))) #
    hyperedge_links = all_edges.rdd.map(lambda el: parseNeighbors(el)).groupByKey().cache() # vertex hyp1 hyp2 hyp3
    for i in range(iterations):
        print("hylinks",hyperedge_links.take(5))
        result_links = hyperedge_links.join(ranks).flatMap(lambda ur:getContributions(ur))
        print("reslinks",result_links.take(5))
        ranks = result_links.reduceByKey(add).mapValues(lambda rank: rank * 0.85 + 0.15)
    df_ranks = ranks.toDF(["vertex", "page_rank"])
    df_ranks.show()
    df_ranks.write.csv(sys.argv[1] + ".csv")
    print(ranks.collect())


start_time = time.time()
parallel_pagerank(sys.argv[2],new_g)
print("--- Execution Time: %s seconds ---" % (time.time() - start_time))
print("--- Partition Time: %s seconds ---" % (p_end_time - p_start_time))
