from graphframes import *
from functools import reduce
from pyspark import *
from pyspark.sql import *
spark = SparkSession.builder.appName('fun').getOrCreate()


with open("../data/testgraph", 'r') as file:
    lines = file.read().split("\n")

print("lines")
#print(lines)
vertices = []
existing_vertices = set()
edges = []
counter=0
for l in lines:
    if l == "":
        continue
    counter+=1
    for v in l.split(" "):
        hypid = "he" + str(counter)
        if not v in existing_vertices:
            vertices.append((v,"vertex"))
            existing_vertices.add(v)
        if not hypid in existing_vertices:
            vertices.append((hypid,"hyperedge"))
            existing_vertices.add(hypid)
        edges.append((v,hypid))
        edges.append((hypid,v))

vertex_metadata = ['id','type']
edges_metadata = ['src','dst']
#print(vertices)
#print(edges)

vertices = spark.createDataFrame(vertices,vertex_metadata)
edges = spark.createDataFrame(edges,edges_metadata)

"""vertices = spark.createDataFrame([('1', 'Carter', 'Derrick', 50), 
                                  ('2', 'May', 'Derrick', 26),
                                 ('3', 'Mills', 'Jeff', 80),
                                  ('4', 'Hood', 'Robert', 65),
                                  ('5', 'Banks', 'Mike', 93),
                                 ('98', 'Berg', 'Tim', 28),
                                 ('99', 'Page', 'Allan', 16)],
                                 ['id', 'name', 'firstname', 'age'])
edges = spark.createDataFrame([('1', '2', 'friend'), 
                               ('2', '1', 'friend'),
                              ('3', '1', 'friend'),
                              ('1', '3', 'friend'),
                               ('2', '3', 'follows'),
                               ('3', '4', 'friend'),
                               ('4', '3', 'friend'),
                               ('5', '3', 'friend'),
                               ('3', '5', 'friend'),
                               ('4', '5', 'follows'),
                              ('98', '99', 'friend'),
                              ('99', '98', 'friend')],
                              ['src', 'dst', 'type'])
"""
print("EDGESRCDEST")
g = GraphFrame(vertices, edges)## Take a look at the DataFrames
states = {"NY":g}
broadcastStates = spark.sparkContext.broadcast(states)

#edges = spark.sparkContext.broadcast(g.edges)
def initialize_page_rank(vertex, count):
    return 1/count

def get_incoming(vertexid,graph):
    res = []
    for hyperedge in graph.edges.filter('dst=="'+vertexid+'"').rdd.toLocalIterator():
        res.append(graph.edges.filter('dst=="'+hyperedge.src+'" and src!="'+vertexid+'"'))
    print(res)
    df = reduce(DataFrame.union,res)
    return df

def get_outgoing(vertexid,graph):
    res = []
    print("vertexid",str(vertexid))
    for hyperedge in graph.edges.filter('src=="'+str(vertexid)+'"').rdd.toLocalIterator():
        # res.append(graph.edges.filter('src=="'+hyperedge.dst+'"')) 
        res.append(graph.edges.filter('src=="'+hyperedge.dst+'" and dst!="'+vertexid+'"'))
    print(res)
    df = reduce(DataFrame.union,res)
    return df


def parseNeighbors(edge,all_edges):
    print("PARSE NEIGHBORS CALLED")
    print(edge.dst)
    dest_id = edge.dst
    neighbors = all_edges.filter('dst == "' + str(dest_id) + '"')
    for row in neighbors.rdd.toLocalIterator():
        #print("edgesrcdest",edge.src,row.dst)
        yield edge.src,row.dst
    #return edge.src,edge.dst


    
def getn(hyp, all_edges):
    neighbors = all_edges.filter('src == "' + str(dest_id) + '"')
    for row in neighbors.rdd.toLocalIterator():
        yield row.dst

def compute_contributions(edges,previous, graph, initial):
    total = 0
    print("edges type", type(edges))
    for edge in edges.rdd.toLocalIterator():
        print(edge.src)
        vertexid = edge.src
        #vertex = graph.vertices.filter('id=="'+str(edge.src)+'"')
        outgoing = get_outgoing(vertexid,graph)
        number = outgoing.count()
        if vertexid not in previous:
            total = total + initial
        else:
            total = total + previous[vertexid]/number

    return total

def test_f1(el0,el1):
    #g.vertices()
    #print(broadcastStates)
    #return broadcastStates.value["NY"],broadcastStates.value["NY"]
    return 1,1
    #for x in el:
    #    return x
def test_func(source, dest ,graph):
    return 1,1
    #df = get_outgoing(dest,graph)
    #for edge in df.rdd.toLocalIterator():
    #    return 1,1
        #yield source,edge.dst 

def parallel_pagerank(iterations, graph):
    all_vertices =g.vertices.filter('type=="vertex"')
    num_vertices = all_vertices.count()
    all_edges = graph.edges
    initial_page_rank = all_vertices.rdd.map(lambda x:(x.id,initialize_page_rank(x,num_vertices))) #
    hyperedge_links = all_edges.rdd.map(lambda el: (el.src,el.dst))
    #test_links = hyperedge_links.flatMap(lambda el: (el[0], el[1].flatMap(lambda x: test_func(x, graph))))
    
    test_links = hyperedge_links.map(lambda el: test_f1(el[0],el[1]))
    
    print("HYPEREGELINKS", hyperedge_links.collect())
    print("COLLECTEDDD",initial_page_rank.collect())
    #test_links.take(1)
    print("TESTLINKS",test_links.collect())
    #hyperedge_links = all_edges.rdd.flatMap(lambda el: parseNeighbors(el, all_edges)).distinct()#.groupByKey().cache() # vertex hyp1 hyp2 hyp3
    #print(type(hyperedge_links))
    #hyperedge_links.collect()
    #hyp1 vertex2 vertex 3 vertex 4
    #hyp2 v4 v5 v6
    #hyp3 v7 v8 v9
    #vertex v2 v3 v4 v5 v6 v7 v8 v9
    # vertex vertex 2 vertex 3 vertex 4
    #hyperedge_links.take(10)
    #hyperedge_links.map(lambda el: el.split(" ")[0],el.split(" ")[1:].map(lambda x: getn(x, all_edges)))
    #links = all_edges.rdd.flatMap(lambda el: parseNeighbors(el, all_edges)).distinct().groupByKey().cache()
    #links = lines.flatMap(lambda urls: parseNeighbors(urls)).distinct().groupByKey().cache()
    #initial_page_rank = 1/num_vertices


def serial_pagerank(iterations,graph):
    all_vertices =graph.vertices.filter('type=="vertex"')
    num_vertices = all_vertices.count()
    initial_val = 1/num_vertices
    page_ranks = {}
    for node in graph.vertices.rdd.toLocalIterator():
        if not node.id in page_ranks:
            page_ranks[node.id] = 1/num_vertices
        else:            
            df1 = get_incoming(node.id,graph)
            res = compute_contributions(edges,page_ranks,graph, initial_val)
            page_ranks[node.id] = res
    print(page_ranks)

#serial_pagerank(5,g)
parallel_pagerank(5,g)
g.edges.filter('src == "1"').show()
#print("VERTICES")
g.vertices.show()
g.edges.show()## Check the number of edges of each vertex
g.degrees.show()

