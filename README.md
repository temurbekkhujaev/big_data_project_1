# big_data_project_1

# Usage guide
The following is the command syntax for running Indexer:

```$ hadoop/bin/hadoop jar <jar file name>.jar Indexer <path to corpus on HDFS> <output path on HDFS>```  
e.g.: ```$ hadoop/bin/hadoop jar ibdhmw.jar Indexer /EnWikiSmall /indexer_results```

Here is the command syntax for running Query:

```$ hadoop/bin/hadoop jar <jar file name>.jar Query <path to Indexer output on HDFS> <query string inside quotes> <number of most relevant docs to show>```  
e.g: ```$ hadoop/bin/hadoop jar ibdhmw.jar Query /indexer_results "some query" 3```
