package com.emily.udemy.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;

public class WordCountApp {
    public static void main(String[] args) {
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "wordcount-application-1");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        StreamsBuilder builder = new StreamsBuilder();

        //Get a stream from kafka
        KStream<String, String> wordCountInput = builder.stream("word-count-input-1");

        //map the values to lower case
        KTable<String, Long> wordCounts = wordCountInput.mapValues(value -> value.toLowerCase())
                //flatmap values chain split by space
                .flatMapValues(value -> Arrays.asList(value.split(" ")))
                //select key to apply a new key and discard the old one
                .selectKey((ignoredKey, word) -> word)
                //group by key before aggregation
                .groupByKey()
                //count occurences
                .count("counts");

        //send the output ti a kafka topic

        wordCounts.toStream().to(Serdes.String(), Serdes.Long(),"word-count-output-1");

        KafkaStreams streams = new KafkaStreams(builder.build(), config);
        streams.start();
        //print the topology
        System.out.println(streams.toString());
        //close the application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
