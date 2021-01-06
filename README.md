# Study on Hypergraphs for Large Networks

### Table of Contents
**[Introduction](#introduction)**<br>
**[Hypergraph Algorithms](#hypergraph-algorithms)**<br>
**[Hypergraph Configuration](#hypergraph-configuration)**<br>
**[Initial Results](#initial-results)**<br>
**[Installation](#installation)**<br>
**[Useful Run Commands](#useful-run-commands)**<br>
**[Conclusion](#conclusion)**<br>
**[Reference](#reference)**<br>

## Introduction

Hypergraphs are a useful data representation when considering group interactions. Consider a authorship network where a group of authors - i.e., Author A, Author B, and Author C who have co authored in a single publication. Representing this information using traditional graph edges where each edge connects two authors to show the co-authorship relationships bring two issues. First, it introduces misleading information where we can not differentiate the case of a single publication by three authors; and three authors co-authored three publications among them (i.e. Publication one co-authored by Author A and Author B; Publication two co-authored by Author B and Author C; Publication three co-authored by Author A and Author C). The second implication of representing hypergraph using traditional graph representation is the inflation of data in the graph storage. For example in our previous co-authorship network, we will need to add N*(N-1) edges to represent the co-authorship among N authors for a single publication. By learning how to encode data into these structures and how to manipulate them to extract meaningful data, we may be able to gain insights about the data that would have not been possible before.

One of the purposes of this study is to understand the hypergraphs and find their adaptability in large networks. We have explored several representations of hypergraphs and understand pros and cons of each. In pursuing this direction, we made several exploratory searches on the existing libraries [1], [2] that work on this direction. These libraries allow to benchmark different algorithms using different encodings and implementations of hypergraph. But these libraries have  flaws- especially when the data sources are large and unyielding like Tweets, mail, social network interactions, or financial data.

In rescuing, researchers made several attempts by proposing parallel hypergraph algorithms [3] which works well on large datasets on a single machine. There is a growing need to process hypergraph algorithms in distributed setup to tackle the challenges of the data beyond the memory limit. In our work, we port the existing parallel hypergraph algorithm implementations to a distributed environment using Apache Spark. We build this Python module to explore the hypergraph PageRank implementation in a distributed setup using Apache Spark.

## Hypergraph Algorithms
### Pagerank
Pagerank is a commonly used graph analytic algorithm. It is used to find the relative importance of the vertices in a network. There are a wide range of applications where pagerank plays very important roles, for example in recommendation systems, link prediction, search engines, etc. We can think of the implication of pagerank in hypergraphs in two different ways. First, the importance of vertices in a network based on their group participation. For example, in a social network context we can measure the importance of a user based on the group membership, e.g., admin of a group with a minion of users might have a bigger influence in the whole network. Second, the importance of the hyperedges based on the vertices it is linked to. For example, from a co-authorship network we can find the most influential publications based on the relative importance of the authors in the network.

## Hypergraph Configuration

A method for representing hypergraphs is to create a clique among all pairs of vertices of each hyperedge and store the result as a regular graph. As we mentioned earlier, this leads to a couple of issues, for example loss of information. Furthermore, the space overhead is significantly higher than that of the original hypergraph. Another approach is to use a bipartite graph representation with participating vertices in one partition and the hyperedges in the other partition. We keep hypergraph information using this bipartite graph representation where the hyperedges are connected to the participating vertices as we can see in the following figure.

<p align="center">
  <img src="https://github.com/biqar/hypergraph-study/blob/main/resources/example_hypergraph.png" />
  <p align="center">Figure 1 - An example hypergraph representing the groups {0,1,2}, {1,2,3}, and {0,3}, and its bipartite representation from [3]<p align="center">
</p>

## Initial Results

We implemented a distributed PageRank algorithm for hypergraph. The experiments are carried out on an 8 node cluster running on AWS. Each node is a m4.2x large machine. Note that each worker corresponds to one core. One node acts as the master and the other 3 nodes act as slaves. The execution engine Apache Spark 2.4.7 is implemented in Python by utilizing Databricks GraphFrames API 0.8.1, which is a graph processing framework on top of which the hypergraph algorithms are implemented.

<table>
  <tr>
    <td>
       <figure>
        <img align="middle" src="https://github.com/biqar/hypergraph-study/blob/main/resources/evaluation_1.png" alt="evaluation_1"/>
        </figure>
    </td>
    <td>
      <figure>
        <img align="middle" src="https://github.com/biqar/hypergraph-study/blob/main/resources/evaluation_2.png" alt="evaluation_2"/>
      </figure>
    </td>
  </tr>
  <tr>
    <td>
       <figure>
        <img align="middle" src="https://github.com/biqar/hypergraph-study/blob/main/resources/evaluation_3.png" alt="evaluation_3"/>
        </figure>
    </td>
    <td>
      <figure>
        <img align="middle" src="https://github.com/biqar/hypergraph-study/blob/main/resources/evaluation_4.png" alt="evaluation_4"/>
      </figure>
    </td>
  </tr>
  <tr>
    <td colspan="2" align="middle">Figure 2 - Experiment results on PageRank.</td>
  </tr>
</table>

In figure 2(i), we compared the distributed partition and algorithm execution time for PageRank algorithm. For all the large datasets, partition cost is significantly lower than the algorithm execution time. In figure 2(ii), we plotted the overall performance of the PageRank algorithm for different datasets. Here, the orkut-5000 graph takes a longer running time compared to others. The reason is, orkut-5000 has much larger hyperedges (i.e. the number of participating nodes in the hyperedges). For example, compared with dbpl-5000, orkut-5000 have 12x larger hyperedges. On the other hand, dbpl-5000 runs only 6x times faster than orkut-5000. This actually implies the usefulness of running large hypergraphs in distributed environments. In figure 2(iii) and figure 2(iv), we compare two hypergraph representations (e.g. clique and bipartite representations) for email-enron and Friendster-5000 respectively. We weren't able to run PageRank on the clique implementation of the hypergraph for orkut-5000 and dbpl-5000, as those graphs are very large and it was not possible to fit them within our testbed setup. This further signifies the importance of our research. Figure 2(iv) indicates that the partition time in bipartite representation significantly reduces for larger hypergraphs compared to the execution time. This is quite expected, as in the clique representation we have to put a lot of edges and hence increases the partition time exponentially.


## Installation
The installation required the following libraries
1) Java 7+
2) Apache Spark (Code tested in version 2.4)
    * Please use this link to install spark: https://spark.apache.org/docs/latest/
3) Graph frames (Tested in version 0.8.1)
    * Please use this to install graphframes: https://graphframes.github.io/graphframes/docs/_site/index.html

## Useful Run Commands
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

## Conclusion
In conclusion, based on the current resources for hypergraph experimentation and implementation, building a library that allows for hypergraph creation and testing for the large networks in a distributed environment, along with a collection of hypergraph algorithms, would be a worthwhile endeavor. We plan to examine the performance of our current implementations and see how we can improve them. Further we will explore the implications of hypergraph partitioning research for distributed computation of hypergraph algorithms.

## Reference
1. Pacific Northwest National Laboratory, Python package for hypergraph analysis and visualization., (2020), GitHub repository, https://github.com/pnnl/HyperNetX
2. Leland McInnes, A library for hypergraphs and hypergraph algorithms., (2015), GitHub repository, https://github.com/lmcinnes/hypergraph
3. Julian Shun. 2020. Practical parallel hypergraph algorithms. In Proceedings of the 25th ACM SIGPLAN Symposium on Principles and Practice of Parallel Programming (PPoPP '20). Association for Computing Machinery, New York, NY, USA, 232–249. DOI:https://doi.org/10.1145/3332466.3374527
4. Y. Gu, et al.,"Distributed Hypergraph Processing Using Intersection Graphs" in IEEE Transactions on Knowledge & Data Engineering, vol. , no. 01, pp. 1-1, 5555. doi: 10.1109/TKDE.2020.3022014
