# spark-operator-cluster


$SPARK_HOME/jars
 1. copy 
 cp /home/jojang/dev/workspace/spark/study50-01-with-mongo-01-cluster/target/study50-1.0-SNAPSHOT-jar-with-dependencies.jar .
 cp mongo-spark-connector_2.12-3.0.0.jar

##필수설치
your need add this jars in the $SPARK_HOME/JARS

commons-pool2-2.8.0.jar
kafka-clients-2.5.0.jar
spark-sql-kafka-0-10_2.12-3.0.0.jar
spark-token-provider-kafka-0-10_2.12-3.0.0.jar

####반드시 넣어야 함... 
spark-token-provider-kafka-0-10_2.12-3.0.0.jar
kafka-clients-2.5.0.jar
 
 2. pom.xml에 추가
 
         <dependency>
             <groupId>org.mongodb</groupId>
             <artifactId>mongo-java-driver</artifactId>
             <version>3.12.6</version>
         </dependency>
         
3. error
 kubernetes에서 local mongodb connection error
 --> success
 
     private static String   MONGODB_INPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05?authSource=admin";
     private static String   MONGODB_OUTPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05?authSource=admin";

4. 참고 싸이트
-- hadoop 설치
https://phoenixnap.com/kb/install-hadoop-ubuntu

-- hadoop docker
https://hub.docker.com/r/bde2020/hadoop-datanode           
