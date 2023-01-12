# UWaterloo CS 451/651

My solution to UW CS 451/651: Data-Intensive Distributed Computing (Fall 2022)

## Instructions

To run the project you'll need the following dependencies:

- Java: 1.8.0
- Scala: 2.11.8
- Hadoop: 3.0.3
- Spark: 2.3.1
- Maven: 3.3.9

For convenience create a file `exports.sh` linking the dependencies to your
environment:

```bash
export PATH=/usr/lib/jvm/java-8-openjdk-amd64/jre/bin:~/packages/spark/bin:~/packages/hadoop/bin:~/packages/maven/bin:~/packages/scala/bin:$PATH
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64/jre
```

Then source the environment variables in the working directory:

```
source exports.sh
```
