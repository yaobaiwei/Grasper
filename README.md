# Grasper

#### Unfortunately, this project has been closed source since 29th June, 2020, due to some commercial reasons. But it is still welcome to use Grasper for academic purposes. Please contact [Dr. Hongzhi CHEN](https://yaobaiwei.github.io/) to fetch the source code if you want to play with Grasper or evaluate Grasper for academic comparison/research.

Grasper is an RDMA-based high performance distributed system for OLAP on property graphs.

Graph analytics has a broad spectrum of applications in both academia and industry. However, processing online graph analytical queries (or OLAP) remains to be challenging since it has much stricter requirements on both latency and throughput. Existing systems suffer from various performance problems due to the key challenging factors as follows:
- Diverse query complexity: online analytical queries may differ signifcantly in their workloads (e.g., from a simple property check on a vertex to complicated pattern matching), and thus a mechanism is needed to support high parallelism and load balancing for heavy-workload queries, while at the same time preventing the starvation of light workload queries. 
- Diverse data access patterns: query operators (e.g., flter, traversal, count) often have diverse access patterns on data and hence require different optimizations (e.g., cache, index), which makes it challenging to design a unifed computational model that optimizes the execution of different query operators.
- High communication and CPU costs: a query may have complex execution logics such as graph traversals that access a large portion of a PG, aggregation that collects intermediate results to one place through network, and joins that are CPU- and data-intensive. Such complex logics often result in high overheads on both communication and computation.

We propose **Grasper**, a distributed system designed to address the aforementioned challenges of processing online analytical queries on PGs. Grasper adopts *Remote Direct Memory Access (RDMA)* to reduce the high network communication cost for distributed query processing and introduces an RDMA-enabled native PG storage to exploit the benefts of RDMA. The key design of Grasper is a novel query execution model, called **Expert Model**, which achieves high utilization of CPU and network resources to maximize the processing
effciency. There are three core benefts brought by Expert Model:
- It allows Grasper to support high concurrency in processing multiple queries and adaptive parallelism control within each query
according to its workload.
- It enables tailored optimizations for different categories of query operations based on their own data
access patterns.
- Underlying system optimizations such as memory locality and thread-level load balancing are also incorporated into the design, which are critical for achieving millisecond latency for processing complex analytical queries.

**Please see the [paper](docs/Grasper_SoCC19.pdf) for more details.**

## Getting Started

**Requirements**

To install Grasper's dependencies (G++, MPI, JDK, HDFS2), please follow the instructions in [here](http://www.cse.cuhk.edu.hk/systems/gminer/deploy.html).
In addition, we also request the following libraries:
* [ZeroMQ](https://zeromq.org/download/)
* [GLog](https://github.com/google/glog)
* [Libibverbs-1.2.0](https://git.kernel.org/pub/scm/libs/infiniband/libibverbs.git)
* [Intel TBB](https://github.com/intel/tbb)
* [Intel MKL](https://software.intel.com/en-us/articles/intelr-mkl-and-c-template-libraries)

**Build**

Please manually SET the dependency paths for above libraries in CMakeLists.txt at the root directory.

```bash
$ export GRASPER_HOME=/path/to/grasper_root  # must configure this ENV
$ cd $GRASPER_HOME
$ ./auto-build.sh
```
**How to run**

Please follow this [**tutorial**](docs/Tutorial.md).


## Academic Paper

[**SoCC 2019**] [Grasper: A High Performance Distributed System for OLAP on Property Graphs](docs/Grasper_SoCC19.pdf). Hongzhi Chen, Changji Li, Juncheng Fang, Chenghuan Huang, James Cheng, Jian Zhang, Yifan Hou, Xiao Yan.

[**SIGMOD 2020**] [High Performance Distributed OLAP on Property Graphs with Grasper](docs/Grasper_SIGMOD20.pdf). Hongzhi Chen, Bowen Wu, Shiyuan Deng, Chenghuan Huang, Changji Li, Yichao Li, James Cheng.

## License

Copyright 2019 Husky Data Lab, CUHK

