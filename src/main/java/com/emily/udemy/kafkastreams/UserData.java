package com.emily.udemy.kafkastreams;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Properties;

public class UserData {
    public static void main(String[] args) {
        Properties config = new Properties();

        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "userData");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        //Disables the cache in kafka streams
        config.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, "0");

        StreamsBuilder builder = new StreamsBuilder();

        //Get Stream from kafka
        KStream<String, String> userData = builder.stream("userdata");
        KStream<String, String> favoriteColors = userData
                //Ensure that you have a comma because you will use it to split the data
                .filter((key, value) -> value.contains(","))
                .selectKey((key, value) -> value.split(",")[0].toLowerCase())
                //get color from the values
                .mapValues(value -> value.split(",")[1].toLowerCase())
                //filter out undesired colors
                .filter((user, color) -> Arrays.asList("red", "green", "blue").contains(color));

        favoriteColors.to("user-keys-colors");

        //Read the topic as a KTable so that updates are read correctly
        KTable<String, String> table = builder.table("user-keys-colors");
        KTable<String, Long> favColors = table
        //count the occurences of color.
                 .groupBy((user, color) -> new KeyValue<>(color, color))
                .count("ColorCount");

        favColors.to(Serdes.String(), Serdes.Long(),"favoriteColorOutput");

        KafkaStreams stream = new KafkaStreams(builder.build(), config);
        stream.cleanUp();
        stream.start();

        //print the topology
        System.out.println(stream.toString());

        Runtime.getRuntime().addShutdownHook(new Thread(stream::close));


    }
}
