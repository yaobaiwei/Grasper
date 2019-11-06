## Grasper Tutorial

### Sample datasets

In the repo, we already provide a sample dataset in folder `/data`, which matches with the toy graph presented in this [**picture**](data/graph-example-1.jpg).
The fold contains four subfolders: `/vertices`, `/vtx_property`, `/edge_property` and `/index`, which stores the data of vertices (graph topology), properties of vertices, properties of edges and index information respectively.

#### Input format

* The files in `/vertices`: each file contains a number of lines, where each line represents one vertex as well as its adjacency list.
	```
	{vid} \tab {num_in_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s {num_out_neighbors} \tab {neighbor1} \s {neighbor2} \s ... {neighborN} \s
	```

* The files in `/vtx_property`: each file contains a number of lines, where each line represents one vertex as well as its **v_label** and property information (formed by a list of key-value pair <**vp_key**, val>).
	```
	{vid} \tab {label} \tab [vp_key1:val,vp_key2:val...]
	```

* The files in `/edge_property`: each file contains a number of lines, where each line represents one edge as well as its **e_label** and property information (formed by a list of key-value pair <**ep_key**, val>).
	```
	{in_vid} \tab {out_vid} \tab {label} \tab [ep_key1:val,ep_key2:val...]
	```

* In subfolder `/index`, there are four files named `vtx_label`, `edge_label`, `vtx_property_index`, `edge_property_index`.

	The files `vtx_label` and `edge_label` are used to record the *string-id* maps which converts the **v_label** and **e_label** from `string` to `int` stored in the memory for data compression. Each line of these two files follows the format: 
	```bash
	#string \tab int
	#for example:
	person	1
	software	2
	```

	The files `vtx_property_index` and `edge_property_index` are used to record the *string-id* maps which converts the **vp_key** and **ep_key** from `string` to `int` stored in the memory for data compression. Each line of these two files follows the format: 
	```bash
	#string \tab int
	#for example:
	name	1
	age 	2
	lang	3
	```

### Uploading the dataset to HDFS
Grasper reads data from HDFS only, and the graph data should be partitioned into `#num_of_servers` blocks for data loading by each server processes. Grasper will handle the graph partition automatically. But users need to upload their data onto HDFS based on the format in sample `/data` as we described above.

However, we cannot use the command `hadoop fs -put {local-file} {hdfs-path}` directly. Otherwise, the data will be loaded by one Grasper process only, while the other processes are simply waiting for its loading. We support parallel read to speed up data load processing, and it has no influence on the performance of graph partitioning and OLAP querying.

To enable it, we suggest to upload graph data (i.e., `/vertices`, `/vtx_property`, `/edge_property`) onto HDFS by running this program `put` with two arguments **{local-file}** and **{hdfs-path}**.
```bash
$ cd $GRASPER_HOME
$ mpiexec -n 1 $GRASPER_HOME/release/put /local_path/to/dataset/{local-file} /hdfs_path/to/dataset/
```


### Configuring and running Grasper

**1** Edit `grasper-conf.ini`, `machine.cfg` and `ib.cfg`

**grasper-conf.ini:**
```bash
#ini file for example
#new line to end this ini

[HDFS]
HDFS_HOST_ADDRESS = master	#the hostname of hdfs name node
HDFS_PORT = 9000		#the port of HDFS
HDFS_INPUT_PATH = /hdfs_path/to/input/
HDFS_INDEX_PATH = /hdfs_path/to/input/index/
HDFS_VTX_SUBFOLDER = /hdfs_path/to/input/vertices/
HDFS_VP_SUBFOLDER = /hdfs_path/to/input/vtx_property/
HDFS_EP_SUBFOLDER = /hdfs_path/to/input/edge_property/
HDFS_OUTPUT_PATH = /hdfs_path/to/input/output/


[SYSTEM]
NUM_THREADS = 20		#the num of threads launched in the thread pool of each server
VTX_P_KV_SZ_GB = 4		#the memory space pre-registered in RDMA-region for KVS of vertices' properties
EDGE_P_KV_SZ_GB = 8		#the memory space pre-registered in RDMA-region for KVS of vertices' properties
PER_SEND_BUF_SZ_MB = 2  	#the size of RDMA send-buf for each working thread 
PER_RECV_BUF_SZ_MB = 64		#the size of RDMA recv-buf for each working thread
KEY_VALUE_RATIO = 50		#the Header-Entry ratio in KVS
USE_RDMA = true			#if enable RDMA
ENABLE_CACHE = true		#if enable cache in query experts
ENABLE_CORE_BIND = true		#if enable core-bind
ENABLE_EXPERT_DIVISION = true	#if enable expert division for logical thread regions, only useful when core-bind is on.
ENABLE_STEP_REORDER = true	#if enable query-step reorder for query optimization
ENABLE_INDEXING = true		#if enable index construction
ENABLE_STEALING = true		#if enable thread-level work stealing 
MAX_MSG_SIZE = 524288 		#(bytes), the upper-bound of message size for splitting
SNAPSHOT_PATH = /local_path/for/snapshot	# the local path to store the graph snapshot on disk, to avoid repeatedly data loading when reboot the system.
```

**machine.cfg (one per line):**
```bash
w1
w2
w3
w4
w5
.......
```

**machines.cfg (one per line):**
```bash
#format: hostname:ibname:tcp_port:ib_port
w1:ib1:19935:-1
w2:ib2:19935:19936
w3:ib3:19935:19936
w4:ib2:19935:19936
w5:ib3:19935:19936
.......
```

**2** To start the Grasper servers, the user only needs to execute the script we provide as follow:

```bash
$ sh $GRASPER_HOME/start-server.sh {$NUM_of_Server+1} machine.cfg ib.cfg

#sample log
[hzchen@master Grasper]$ sh ./start-server.sh 5 machine.cfg ib.cfg
Node: { world_rank = 0 world_size = 5 local_rank = 0 local_size = 1 color = 0 hostname = worker25 ibname = ib25}
Node: { world_rank = 1 world_size = 5 local_rank = 0 local_size = 4 color = 1 hostname = worker26 ibname = ib26}
Node: { world_rank = 3 world_size = 5 local_rank = 2 local_size = 4 color = 1 hostname = worker28 ibname = ib28}
Node: { world_rank = 2 world_size = 5 local_rank = 1 local_size = 4 color = 1 hostname = worker27 ibname = ib27}
Node: { world_rank = 4 world_size = 5 local_rank = 3 local_size = 4 color = 1 hostname = worker29 ibname = ib29}
given SNAPSHOT_PATH = /home/hzchen/tmp/gq_snapshot, processed = /home/hzchen/tmp/gq_snapshot
DONE -> Config->Init() 
DONE -> Config->Init()
DONE -> Config->Init()
DONE -> Config->Init()
DONE -> Config->Init()
local_rank_ == 0 node 0: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 1 node 1: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 2 node 2: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
local_rank_ == 3 node 3: cores: [ 0 2 4 6], [ 8 10], [ 12 14], [ 1 3], [ 5 7 9], [ 11 13 15], threads: [ 0 4 8 12 16 20 24 28], [ 1 5 17 21], [ 9 13 25 29], [ 2 6 18 22], [ 10 14 3 26 30 19], [ 7 11 15 23 27 31], 
Worker0: DONE -> Init Core Affinity 
Worker2: DONE -> Init Core Affinity
Worker1: DONE -> Init Core Affinity 
Worker3: DONE -> Init Core Affinity 
Worker0: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker3: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker1: DONE -> Register RDMA MEM, SIZE = 12773753312 
Worker2: DONE -> Register RDMA MEM, SIZE = 12773753312 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936 
[librdma] : listener binding: tcp://*:19936
INFO: initializing RDMA done (1168 ms)
Worker3: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1210 ms)
Worker0: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1175 ms)
Worker1: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1164 ms)
Worker2: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
Worker3: DONE -> DataStore->Init()
Worker1: DONE -> DataStore->Init()
Worker0: DONE -> DataStore->Init()
Worker2: DONE -> DataStore->Init()
INFO: initializing RDMA done (1168 ms)
Worker3: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1210 ms)
Worker0: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1175 ms)
Worker1: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
INFO: initializing RDMA done (1164 ms)
Worker2: DONE -> Mailbox->Init()
INFO: vkvstore = 4294967296 bytes 
      header region: 134217728 slots (main = 12582917, indirect = 4194299)
      entry region: 2147483648 entries
INFO: ekvstore = 6442450944 bytes 
      header region: 201326592 slots (main = 12582917, indirect = 12582907)
      entry region: 3221225472 entries
Worker3: DONE -> DataStore->Init()
Worker1: DONE -> DataStore->Init()
Worker0: DONE -> DataStore->Init()
Worker2: DONE -> DataStore->Init()
Node 1 get_string_indexes() DONE !
Node 1 Get_vertices() DONE !
Node 1 Get_vplist() DONE !
Node 1 Get_eplist() DONE !
Node 3 get_string_indexes() DONE !
Node 3 Get_vertices() DONE !
Node 3 Get_vplist() DONE !
Node 3 Get_eplist() DONE !
Node 2 get_string_indexes() DONE !
Node 2 Get_vertices() DONE !
Node 2 Get_vplist() DONE !
Node 2 Get_eplist() DONE !
Node 0 get_string_indexes() DONE !
Node 0 Get_vertices() DONE !
Node 0 Get_vplist() DONE !
Node 0 Get_eplist() DONE !
get_vertices snapshot->TestRead('datastore_v_table')
Shuffle snapshot->TestRead('vkvstore')
Shuffle snapshot->TestRead('ekvstore')
Worker1: DONE -> DataStore->Shuffle()
Worker2: DONE -> DataStore->Shuffle()
Worker3: DONE -> DataStore->Shuffle()
Worker0: DONE -> DataStore->Shuffle()
DataConverter snapshot->TestRead('datastore_v_table')
Worker1: DONE -> Datastore->DataConverter()
Worker3: DONE -> Datastore->DataConverter()
Worker2: DONE -> Datastore->DataConverter()
Worker0: DONE -> Datastore->DataConverter()
Worker0: DONE -> Parser_->LoadMapping()
Worker2: DONE -> Parser_->LoadMapping()
Worker1: DONE -> Parser_->LoadMapping()
Worker3: DONE -> Parser_->LoadMapping()
Worker1: DONE -> expert_adapter->Start()
Worker3: DONE -> expert_adapter->Start()
Worker0: DONE -> expert_adapter->Start()
Worker2: DONE -> expert_adapter->Start()
Grasper Servers Are All Ready ...

```

3. To start the Grasper client, the user only needs to execute the script we provide as follow:
```bash
$ sh $GRASPER_HOME/start-client.sh ib.cfg

#sample log
[hzchen@master Grasper]$ sh ./start-client.sh ib.cfg 
DONE -> Client->Init()
Grasper> help
Grasper commands: 
    help                display general help infomation
    help index          display help infomation for building index
    help config         display help infomation for setting config
    help emu            display help infomation for running emulation of througput test
    quit                quit from console
    Grasper <args>       run Gremlin-Like queries
        -q <query> [<args>] a single query input by user
           -o <file>           output results into <file>
        -f <file> [<args>]  a single query from <file>
           -o <file>           output results into <file>
        -b <file> [<args>]  a set of queries configured by <file> (batch-mode)
           -o <file>           output results into <file>

Grasper> Grasper -q g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")
[Client] Processing query : g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")
[Client] Client just posted a REQ
[Client] Client 1 recvs a REP: get available worker_node0

[Client] Client posts the query to worker_node0

[Client] result: Query 'g.V().hasKey("<http://dbpedia.org/property/publisher>").hasLabel("link").has("<http://dbpedia.org/property/language>", "Irish")' result:
=>26693946
=>26693940
=>26668218
=>14265685
=>27713332
=>15486153
=>28085459
=>15513537
=>28249935
=>15819014
=>16591506
=>21324100
=>20466007
=>21352851
=>22472949
=>22411388
=>23789152
=>24437478
=>24437479
[Timer] 1309.06 ms for ProcessQuery
[Timer] 1319 ms for whole process.

```


