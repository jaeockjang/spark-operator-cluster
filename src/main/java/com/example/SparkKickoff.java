package com.example;

import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.config.WriteConfig;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;
import org.bson.Document;

import java.util.*;

import static java.util.Collections.singletonList;


public class SparkKickoff {

    private static final String APPLICATION_NAME = "Spark Kickoff";
    private static final String RUN_LOCAL_WITH_AVAILABLE_CORES = "local[*]";

    static final String path = "./data/";
//    static final String path = "/opt/spark/mycm/";

    private static String streamDurationTime="10";

//    private static String metadatabrokerlist="localhost:9092";
    private static String metadatabrokerlist="my-kafka.default.svc.cluster.local:9092";

    private static String topicsAll="topic";
    //	@Autowired
//	private transient Gson gson;
//
    private static transient JavaStreamingContext jsc;
    //	@Autowired
    private static transient JavaSparkContext javaSparkContext;

    //	@Autowired
    private static SparkSession sparkSession;

    private static String sparkAppName="App01";

    private static String sparkMasteer="local[*]";
//    private static String sparkMasteer="spark://jojang-lenovo:7077";

//    private static final String MONGODB_OUTPUT_URI = "mongodb://root:NtwB4IHDVD@my-release-mongodb.default.svc.cluster.local:27017/test.test3";

//    private static String   MONGODB_INPUT_URI="mongodb://my-release-mongodb.default.svc.cluster.local/test.test05";
//    private static String   MONGODB_OUTPUT_URI="mongodb://my-release-mongodb.default.svc.cluster.local/test.test05";

//    private static String   MONGODB_INPUT_URI="mongodb://mongodb-local/test.test05";
//    private static String   MONGODB_OUTPUT_URI="mongodb://mongodb-local/test.test05";
    private static String   MONGODB_AUTH_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017";

//    private static String   MONGODB_AUTH_URI="mongodb://root:l5bRtkeK2n@my-release-mongo-mongodb.default.svc.cluster.local:27017/?authSource=admin&readPreference=primary&ssl=false";

    private static String   MONGODB_INPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05?authSource=admin";
    private static String   MONGODB_OUTPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05?authSource=admin";

//    private static String   MONGODB_INPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05";
//    private static String   MONGODB_OUTPUT_URI="mongodb://root:S2acqhUKNB@my-release-mongo-mongodb.default.svc.cluster.local:27017/test.test05";


    public static SparkConf sparkConf() {
        SparkConf conf = new SparkConf()
                .setAppName(sparkAppName)
                .setMaster(sparkMasteer)
                .set("spark.serializer","org.apache.spark.serializer.KryoSerializer")
                .set("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.0")
                .registerKryoClasses(new Class<?>[] { ConsumerRecord.class})

//                .set("spark.mongodb.auth.uri", MONGODB_AUTH_URI)
                .set("spark.mongodb.input.uri", MONGODB_INPUT_URI)
                .set("spark.mongodb.output.uri", MONGODB_OUTPUT_URI)

                ;
        return conf;
    }



    //	@ConditionalOnMissingBean(JavaSparkContext.class)
    public static JavaSparkContext javaSparkContext() {
        return new JavaSparkContext(sparkConf());
    }


    public SparkSession sparkSession() {
        return SparkSession
                .builder()
                .sparkContext(javaSparkContext().sc())
                .appName("Java Spark SQL basic example")
                .getOrCreate();
    }



    public static void main(String[] args) throws InterruptedException {
        // System.setProperty("hadoop.home.dir", "D:\\hadoop-2.7.5");
        Collection<String> topics = Arrays.asList("topic");

        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("metadata.broker.list", metadatabrokerlist);
        kafkaParams.put("bootstrap.servers", metadatabrokerlist);
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
//		kafkaParams.put("auto.offset.reset", "earliest");
//		kafkaParams.put("enable.auto.commit", true);
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", true);
//		kafkaParams.put("enable.auto.commit", false);

        javaSparkContext=javaSparkContext();
        sparkSession=SparkSession
                .builder()
                .sparkContext(javaSparkContext.sc())
                .appName("Java Spark SQL basic example")
                .getOrCreate();

        jsc = new JavaStreamingContext(javaSparkContext,
                Durations.seconds(Integer.valueOf(streamDurationTime)));

//		jsc.checkpoint("checkpoint");


//		final JavaPairInputDStream<String, String> stream = KafkaUtils.createDirectStream(jsc, f,
//				String.class, StringDeserializer.class, StringDeserializer.class, kafkaParams, topics);

        final JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        jsc,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.<String, String>Subscribe( topics, kafkaParams)
                );



        System.out.println("stream started!");
        stream.print();
        stream.foreachRDD(rdd -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            System.out.println( "rdd.count:" + rdd.count() );
        });

        stream.foreachRDD( x -> {
            x.collect().forEach( (xx)-> { System.out.println("Kafka Received Data:" + xx.value());  });

            x.collect().forEach( (xx)-> {
                System.out.println("Kafka Received Data:" + xx.value());
                List<KafkaData> data = Arrays.asList(new KafkaData(null, xx.value()));
                Dataset<Row> dataFrame = sparkSession.createDataFrame(data, KafkaData.class);
                dataFrame.createOrReplaceTempView("my_kafka");
                Dataset<Row> sqlDS=sparkSession.sql("select * from my_kafka");
                sqlDS.printSchema();
                sqlDS.show();
            });

        });


        //-----------------------------------------------------------------------------------------------------------------------------
        // MongoDB 저장
        //-----------------------------------------------------------------------------------------------------------------------------

        JavaSparkContext jsctx = javaSparkContext;


        // Create a custom WriteConfig
        Map<String, String> writeOverrides = new HashMap<String, String>();
        writeOverrides.put("database", "test");
        writeOverrides.put("collection", "test05");
        writeOverrides.put("writeConcern.w", "majority");
        WriteConfig writeConfig = WriteConfig.create(jsctx).withOptions(writeOverrides);


//        --------------------------------------------------------------------------------------------------


        stream.foreachRDD(x -> {
            if (x.count() > 0) {
                x.collect().forEach((xx) -> {
                    System.out.println("Kafka Received Data:" + xx.value());
                });

//                List<KafkaData> data = new ArrayList<>();
                List<String> data2 = new ArrayList<>();
                x.collect().forEach(record -> {
//                            data.add(new KafkaData(null, record.value()));
                            data2.add(record.value());
                        }
                );

                JavaRDD<Document> sparkDocuments2 = jsctx.parallelize(data2).map
                        (new Function<String, Document>() {
                            public Document call(final String i) throws Exception {
                                UUID uuid = UUID.randomUUID();
                                String txId=uuid.toString();

                                return Document.parse("{ \"" + "key" + "\" : \"" + i + "\"}");

                            }
                        });

                try {
                    System.out.println("================================================================");
                    System.out.println("MongoDB Save try");
                    System.out.println("================================================================");

                    MongoSpark.save(sparkDocuments2, writeConfig);


                    // Loading and analyzing data from MongoDB
                    JavaMongoRDD<Document> rdd = MongoSpark.load(jsctx);
                    System.out.println("mongodb count(): " + rdd.count());
                    System.out.println(rdd.first().toJson());

                    try {
                        JavaMongoRDD<Document> aggregatedRdd = rdd.withPipeline(singletonList(Document.parse("{ $match: { key : { jang } } }")));
                        System.out.println(aggregatedRdd.count());
                        System.out.println(aggregatedRdd.first().toJson());
                    }catch(Exception ex) {
                        System.out.println(ex.getMessage());
                    }

                    System.out.println("MongoDB Save Success!!!");

                }catch(Exception ex) {
                    System.out.println("MongoDB Save Failed!!! " + ex.getMessage());
                }

            }
        });



        //-----------------------------------------------------------------------------------------------------------------------------
        // Hadoop 저장
        //-----------------------------------------------------------------------------------------------------------------------------
//        boolean HADOOP_RUN_FLAG = false;
//        if (HADOOP_RUN_FLAG) {
//            stream.foreachRDD(x -> {
//                if (x.count() > 0) {
//                    x.collect().forEach((xx) -> {
////                    System.out.println("Kafka Received Data:" + xx.value());
//                    });
//
//                    List<KafkaData> data = new ArrayList<>();
//                    x.collect().forEach(record -> {
//                                data.add(new KafkaData(null, record.value()));
//                            }
//                    );
//
//                    Dataset<Row> dataFrame = sparkSession.createDataFrame(data, KafkaData.class);
//                    dataFrame.createOrReplaceTempView("my_kafka2");
//                    Dataset<Row> sqlDS = sparkSession.sql("select * from my_kafka2");
//                    sqlDS.printSchema();
//                    sqlDS.show();
//
//
//                    dataFrame.write().mode(SaveMode.Append).parquet(path + "my_kafka2.parquet");
//
//                    try {
//                        System.out.println("================================================================");
//                        System.out.println("HDFS Connecting");
//                        System.out.println("================================================================");
//
//
//                        //--------------------------------------
//                        //--------------------------------------
//                        // TODO: 20. 7. 24. 파일논리명을 DB에  업무 Key와 연관지어 저장한다.
//                        dataFrame.write().mode(SaveMode.Append).parquet("hdfs://hadoop-local:9000/user/hdoop/schedule/t1");
//                        System.out.println("HDFS Save success!!!!");
//
//
//                    } catch (Exception ex) {
//                        System.out.println("HDFS failed " + ex.getMessage());
//                        ex.printStackTrace();
//                    }
//
//                }
//            });
//        }



        //------------------------------------------------------------------------------------------------------------------------------
        jsc.start();
        jsc.awaitTermination();

    }



}
