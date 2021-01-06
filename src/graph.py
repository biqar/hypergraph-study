from graphframes import *
from pyspark import *
from pyspark.sql import *
spark = SparkSession.builder.appName('fun').getOrCreate()
with open("../data/email-enron.txt", 'r') as file:
    lines = file.read().split("\n")

print("lines")
print(lines)
vertices = []
edges = []
counter=0
for l in lines:
    if l == "":
        continue
    counter+=1
    for v in l.split(" "):
        hypid = "he" + str(counter)
        vertices.append((v,"vertex"))
        vertices.append((hypid,"hyperedge"))
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
g = GraphFrame(vertices, edges)## Take a look at the DataFrames
print("rows",g.edges.filter('src == "1"').count())
g.vertices.filter('type=="vertex"').show()
g.vertices.filter('type=="hyperedge"').show()
g.edges.show()## Check the number of edges of each vertex
g.degrees.show()
