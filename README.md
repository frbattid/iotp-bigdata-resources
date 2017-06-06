# iotp-bigdata-resources
iotp-bigdata-resources is a general purpose library for Java applications
containing analysis jobs focused on Json-like data within HDFS files, and in
particular Json-like data containing NGSI data (as the one
[Cygnus](https://github.com/telefonicaid/fiware-cygnus) tool can generate from
[Orion Context Broker](https://github.com/telefonicaid/fiware-orion)
notifications).

For the time being, the library only contains analysis jobs written for
[Apache Hadoop](http://hadoop.apache.org/)'s MapReduce paradigm. Nevertheless,
it is expected it contains useful analytical jobs for other big data platforms
and tools, such as [Apache Spark](https://spark.apache.org/), very soon.

## Build
### Pre-requisites
Pre-requisites for building the library are:
* [Java Development Kit](http://openjdk.java.net/) 7 or higher.
* [Apache Maven](https://maven.apache.org/).

[Top](#iotp-bigdata-resources)

### Maven packaging
For the first time, git clone this repository:

    $ git clone https://github.com/frbattid/iotp-bigdata-resources.git

Then, enter `iotp-bigdata-resources`:

    $ cd iotp-bigdata-resources

and run the following Maven command to build the library:

    $ mvn package

A `target/` subfolder is created containing the Java `.jar` for the library.

    $ ls target | grep jar
    iotp-bigdata-resources-0.1.0.jar

You can rerun the build command as many times as you want; in that case it is
recommended to clean all before packaging:

    $ mvn clean package

[Top](#iotp-bigdata-resources)

## Installation
The library must be installed under any reachable path (from the point of view
of view of the Unix user running the MapReduce jobs it contains) within HDFS.
Tipically, it is installed under `/user/<user>/jars` folder.

You can do this using any of the mechanisms given by Hadoop when dealing with
HDFS interfacing. Here, [WebHDFS](http://hadoop.apache.org/docs/current/hadoop-project-dist/hadoop-hdfs/WebHDFS.html)-based
upload is shown (`curl` tool is used as Http client):

    $ curl -X PUT \
    "http://<namenode_host>:<webhdfs_port>/webhdfs/v1/user/<user>/jars \
    ?op=create&user.name=<user>" \
    -T iotp-bigdata-resources-0.1.0.jar \
    -L

[Top](#iotp-bigdata-resources)

## Usage
There exists the following MapReduce jobs within the library (fully qualified
class names, or FQCN, are given):
* [`com.telefonica.iot.bigdata.hadoop.mr.Aggregate`](aggregate)
* [`com.telefonica.iot.bigdata.hadoop.mr.AlterFieldName`](alterfieldname)
* [`com.telefonica.iot.bigdata.hadoop.mr.AlterFieldType`](alterfieldtype)
* [`com.telefonica.iot.bigdata.hadoop.mr.Count`](count)
* [`com.telefonica.iot.bigdata.hadoop.mr.CountByField`](countbyfield)
* [`com.telefonica.iot.bigdata.hadoop.mr.FilterColumn`](filtercolumn)
* [`com.telefonica.iot.bigdata.hadoop.mr.FilterRecord`](filterrecord)
* [`com.telefonica.iot.bigdata.hadoop.mr.Json2CSV`](json2csv)

Those jobs are fully executable from any Hadoop instance. Just ssh into any node
of the cluster and run:

    $ hadoop jar iotp-bigdata-resources-0.1.0.jar <job_FQCN> <parameters>

Parameters usually include an input and output directories, followed by any
other data required by the job. Next sub-section detail each one of the
available jobs.

[Top](#iotp-bigdata-resources)

### Aggregate
Aggregates all the Json documents within HDFS files into a single Json document
containing, field by field, the aggregated value of all the original fields'
values. The aggregation of each field follows an specified criteria.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).
* Aggregation criteria.

The aggregation criteria must be written with the following format:

    field1:aggr1&field2:aggr2&...&fieldN:aggrN

I.e., a list of pairs separated by `&`, where each pair in the concatenation
(`:` as concatenator) of a Json field within the data and any of these
aggregators:

* last
* first
* sum
* max
* min
* and
* or

There exists a special field name, `_default_`, that can be used as a wildcard
for all the fields not having a specific aggregation criteria.

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

And this aggregation criteria:

    attrValue:first&_default_:last

The the result of the `Aggregate` MapReduce job is:

    {"recvTime":"2017-05-25T09:54:58.427Z","entityType":"device","attrMd":[],"fiwareServicePath":"/sevilla","entityId":"dev1","recvTimeTs":"1495706098","attrValue":"30","attrName":"temperature","attrType":"Float"}

[Top](#iotp-bigdata-resources)

### AlterFieldName
Alters the names of the Json fields within HDFS files.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).
* List of name mappings.

The list of name mappings is a collection of pairs containing the old name and the new name (separated by `:`), being the pairs separated by `&`, following this format:

    oldName1:newName1&oldName2:newName2&...&oldNameN:newNameN

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

And these name mappings:

    recvTimeTs:ms&recvTime:ts

The the result of the `Aggregate` MapReduce job is:

```
{"ms":"1495706098","ts":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"ms":"1495706098","ts":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"ms":"1495706357","ts":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"ms":"1495706357","ts":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"ms":"1495706445","ts":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"ms":"1495706445","ts":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

[Top](#iotp-bigdata-resources)

### AlterFieldType
To be done.

[Top](#iotp-bigdata-resources)

### Count
Counts all the Json documents within HDFS files.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).

For instance, given a HDFS file with 6 Json documents, the result of the `Count`
MapReduce job is:

    {"count": 6}

[Top](#iotp-bigdata-resources)

### CountByField
Counts all the Json documents within HDFS files.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).
* Field used as key for the counts.

If any of the Json documents does not contain the given field, a special key is
used, i.e. `_rest_`.

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

If the filed used as key is `entityId` then the result of the `CountById`
MapReduce job is:

    {"dev1": 4, "dev2": 2, _rest_": 0}

[Top](#iotp-bigdata-resources)

### FilterColumn
Filters fields from Json-like content within HDFS files.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).
* Fields to be filtered.

The list of fields to be filtered is concatenated with `&`, i.e.:

    field1&field2&...&fieldN

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

And this list of fields:

    entityId&entityType

Then the result of the `FilterColumn` MapReduce job is:

```
{"entityType":"device","entityId":"dev1"}
{"entityType":"device","entityId":"dev1"}
{"entityType":"device","entityId":"dev1"}
{"entityType":"device","entityId":"dev1"}
{"entityType":"device","entityId":"dev2"}
{"entityType":"device","entityId":"dev2"}
```

[Top](#iotp-bigdata-resources)

### FilterRecord
Filters Json documents from HDFS files.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).
* List of conditions for filtering.

Filtering conditions are separated by `&`, being echa condition a triple
contain field, operator and value, i.e. following this format:

    field1op1value1&field2op2value2&...&fieldNopNvalueN

Being these the valid operators:
* \>
* \<
* ==
* !=

The same field may appear more than once in the conditions.

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

And this list of filtering conditions:

    recvTimeTs!=1495706098&entityId==dev2

Then the result of the `FilterRecord` MapReduce job is:

```
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

[Top](#iotp-bigdata-resources)

### Json2CSV
Translates Json-like content within HDFS files into the corresponding CSV
format.

Parameters:
* HDFS input directory.
* HDFS output directory (must not exist, it is created by the job).

For instance, given this HDFS content:

```
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"23","attrMd":[]}
{"recvTimeTs":"1495706098","recvTime":"2017-05-25T09:54:58.427Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"709","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"800","attrMd":[]}
{"recvTimeTs":"1495706357","recvTime":"2017-05-25T09:59:17.488Z","fiwareServicePath":"/sevilla","entityId":"dev1","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"pressure","attrType":"Integer","attrValue":"801","attrMd":[]}
{"recvTimeTs":"1495706445","recvTime":"2017-05-25T10:00:45.640Z","fiwareServicePath":"/sevilla","entityId":"dev2","entityType":"device","attrName":"temperature","attrType":"Float","attrValue":"30","attrMd":[]}
```

The result of the `J_son2CSV` MapReduce job is:

```
1495706098,2017-05-25T09:54:58.427Z,/sevilla,dev1,device,temperature,Float,23,[]
1495706098,2017-05-25T09:54:58.427Z,/sevilla,dev1,device,pressure,Integer,709,[]
1495706357,2017-05-25T09:59:17.488Z,/sevilla,dev1,device,pressure,Integer,800,[]
1495706357,2017-05-25T09:59:17.488Z,/sevilla,dev1,device,temperature,Float,30,[]
1495706445,2017-05-25T10:00:45.640Z,/sevilla,dev2,device,pressure,Integer,801,[]
1495706445,2017-05-25T10:00:45.640Z,/sevilla,dev2,device,temperature,Float,30,[]
```

[Top](#iotp-bigdata-resources)

## License
iotp-bigdata-resources is free software: you can redistribute it and/or modify
it under the terms of the **GNU Affero General Public License** as published by
the Free Software Foundation, either version 3 of the License, or (at your
option) any later version.

iotp-bigdata-resources is distributed in the hope that it will be useful, but
WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY or
FITNESS FOR A PARTICULAR PURPOSE. See the GNU Affero General Public License for
more details (there is a copy distributed within this repository).

[Top](#iotp-bigdata-resources)

## Author and contact
Author: Francisco Romero Bueno (francisco.romerobueno@telefonica.com)

Contact: iot_support@tid.es

[Top](#iotp-bigdata-resources)
