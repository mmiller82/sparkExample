# Apache Spark

Spark is a fast and general cluster computing system for Big Data. It provides
high-level APIs in Scala, Java, and Python, and an optimized engine that
supports general computation graphs for data analysis. It also supports a
rich set of higher-level tools including Spark SQL for SQL and structured
data processing, MLlib for machine learning, GraphX for graph processing,
and Spark Streaming for stream processing.

<http://spark.apache.org/>


## Example
The example can be run with this command:
LOCATION OF YOUR SPARK APPLICATION/bin/spark-submit --class "org.mmiller.spark.SparkApplication" --master local[4]   target/SparkExample-1.0-SNAPSHOT.jar