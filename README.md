# spark-operator-cluster


$SPARK_HOME/jars
 1. copy 
 cp /home/jojang/dev/workspace/spark/study50-01-with-mongo-01-cluster/target/study50-1.0-SNAPSHOT-jar-with-dependencies.jar .
 cp mongo-spark-connector_2.12-3.0.0.jar
 
 2. pom.xml에 추가
 
         <dependency>
             <groupId>org.mongodb</groupId>
             <artifactId>mongo-java-driver</artifactId>
             <version>3.12.6</version>
         </dependency>
