package com.study.spark.streaming;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.log4j.Logger;
import org.apache.spark.SparkConf;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.streaming.Durations;
import org.apache.spark.streaming.api.java.JavaInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.streaming.kafka010.*;

import java.util.*;

/**
 * spark-submit --class com.study.spark.streaming.KafkaIntegration spark-study-1.0-SNAPSHOT.jar
 */

public class KafkaIntegration {

    private static Logger logger = Logger.getLogger("org.apache.spark");

    public static void main(String[] args) throws InterruptedException {
        Map<String, Object> kafkaParams = new HashMap<>();
        kafkaParams.put("bootstrap.servers", "BZD21333-PC.kingsoft.cn:9092");
        kafkaParams.put("key.deserializer", StringDeserializer.class);
        kafkaParams.put("value.deserializer", StringDeserializer.class);
        kafkaParams.put("group.id", "use_a_separate_group_id_for_each_stream");
        kafkaParams.put("auto.offset.reset", "latest");
        kafkaParams.put("enable.auto.commit", false);

        List<String> topics = Arrays.asList("my-topic");
        SparkConf conf = new SparkConf();
        conf.setAppName("KafkaIntegration").setMaster("local[3]");
        JavaSparkContext sc = new JavaSparkContext(conf);
        JavaStreamingContext streamingContext = new JavaStreamingContext(sc, Durations.seconds(10));

        JavaInputDStream<ConsumerRecord<String, String>> stream =
                KafkaUtils.createDirectStream(
                        streamingContext,
                        LocationStrategies.PreferConsistent(),
                        ConsumerStrategies.Subscribe(topics, kafkaParams)
                );

        stream.foreachRDD((rdd, time) -> {
            OffsetRange[] offsetRanges = ((HasOffsetRanges) rdd.rdd()).offsetRanges();
            rdd.foreachPartition(consumerRecords -> {
                OffsetRange o = offsetRanges[TaskContext.get().partitionId()];
                System.out.println(
                        o.topic() + " " + o.partition() + " " + o.fromOffset() + " " + o.untilOffset());
            });
            ((CanCommitOffsets) stream.inputDStream()).commitAsync(offsetRanges, (map, e) -> {
                if (Objects.nonNull(e)) {
                    logger.error(e);
                } else
                    logger.info("commitAsync onComplete  offsetRanges: " + map + "  time : " + time);
            });
        });
        streamingContext.start();
        streamingContext.awaitTermination();
        streamingContext.close();

    }
}
