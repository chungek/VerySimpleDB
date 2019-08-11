# VerySimpleDB

#### Column-based implementation of a SQL database in Java. 
* Data is compressed from .csv to .dat format.
* Each query is broken down into a left-deep relational algebra tree so data is processed in a pipeline. 
* It is a push system where each relational algebra operator runs as its own thread and processes tuples in batches. 
* Join-ordering Strategy: Build a left deep tree per query with joins with highest cardinality estimations closest to the left-most leaf.
* Join algorithm: In-memory hash-join.

compile.sh to build
