# hypergraph-study
## Installation required
The installation required are 
1) Java 7+
2) Apache Spark
3) Graph frames 

Please use this to install
https://spark.apache.org/docs/latest/

Please use this to install graphframes
https://graphframes.github.io/graphframes/docs/_site/index.html

Use spark submit like:

spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 <python-code.py>

1) to run bipartite page rank, do 

spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 bipartite_page_rank.py

2) to run clique page rank

spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 clique_page_rank.py

Be sure to install spark 2.4 version and graph frames 0.8.1 version
