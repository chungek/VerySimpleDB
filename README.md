# VerySimpleDB

#### Column-based implementation of a SQL database in Java. 
* Data is compressed from .csv to .dat format.
* Query is broken down into a left-deep relational algebra tree so data is processed in a pipeline. 
* It is a push system where each relational algebra operator runs as its own thread. 
* Join algorithm: In-memory hash-join.

compile.sh to build
