# Shelob

## Purpose
The purpose of Shelob program is to analyze and compute all relation between subject with given direct ownership relations. The result of the program is the computation of both direct and indirect ownership relations.

## Usage
```bash
Usage: shelob [csv|parquet|database] [options]

  -p, --parent-column <value>
                           Name of the column with parent id
  -c, --child-column <value>
                           Name of the column with child id
  -r, --relation-column <value>
                           Name of the column with relation id
  -l, --log-level <value>  Logging level of whole application
  -m, --master <value>     Spark application master
  -n, --name <value>       Spark application name
  -h, --hive-support       Enabling hive support
Command: csv [options]
Load data from CSV file
  -p, --path <value>       Path to CSV file
  -tgt, --target-name <value>
                           Path to result folder with saved csv
Command: parquet [options]
Load data from parquet file
  -p, --path <value>       Path to folder with parquet files
  -tgt, --target-name <value>
                           Path to result folder with saved parquets
Command: database [options]
Load data from hadoop
  -d, --database-name <value>
                           Name of database
  -t, --table-name <value>
                           Name of table
  -tgt, --target-name <value>
                           Name of result table
  --help                   Program help and usage
  --debug                  Turn on indefinite loop at the end of program. For debug purposes only. BEWARE: The program never stops unless killed    
```

## Algorithm description
1. Create graph with following attributes:
    * All distinct party_ids (child and parent) are vertices of the graph
    * All relation between parent and child are edges of the graph
2. Send the ownership message in opposite direction of every edge in the graph. The values of the message are:
    * Source: Source vertice of the edge
    * Target: Target vertice of the edge
    * Origin: Source vertice of the edge
    * Relation: Relation value of the edge
3. When messages are sent on the same edge, merge them and continue
4. When messages arrive to the vertice
    1. If the message source is the vertice itself, remember it
    2. If the message source is different from the vertice, look for already received messages
        * If the Target of the message and Origin of the message was already seen, discart it (loop)
        * If the Target of the message is the vertice itself, discart it (loop)
        * Calculate the new message by:
            * Search in remembered messages (RM) for the message with target equals to source of the message arrived (MA)
            * The new message M(src, dst, orig, value) = (vertice id, MA.dst, MA.orig, RM.value * MA.value) - following path ==> multiplying values
            * Remember the newly calculated message M. If there is some message with the same source and target, then add the newly calculated value to the remembered value - joining treees ==> adding values
5. Send all newly remembered messages in opposite direction of the every connected to the given vertice
6. If there are some messages to be sent, go to step 3). Otherwise end the algorithm.
