# hypergraph-study

## Installation
The installation required the following libraries
1) Java 7+
2) Apache Spark (Code tested in version 2.4)
    * Please use this link to install spark: https://spark.apache.org/docs/latest/
3) Graph frames (Tested in version 0.8.1)
    * Please use this to install graphframes: https://graphframes.github.io/graphframes/docs/_site/index.html

## Useful run commands
Use spark submit like:
```
> spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 <python-code.py>
```

To run bipartite page rank, do 
```
> spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 bipartite_page_rank.py
```

To run clique page rank
```
> spark-submit --packages graphframes:graphframes:0.8.1-spark2.4-s_2.11 clique_page_rank.py
```
