package com.lordofthejars.kstreamscat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

public class KafkaStreamsProducer {

    public KafkaStreams buildKafkaStreams(Topology topology, KStreamsCatOptions options) {

        Path localStateDirectory;
        try {
            localStateDirectory = Files.createTempDirectory("kafka-streams");
        } catch (IOException e) {
            throw new IllegalArgumentException(e);
        }

        final Properties streamsConfiguration = new Properties();
        streamsConfiguration.put(StreamsConfig.APPLICATION_ID_CONFIG, options.appId);
        streamsConfiguration.put(StreamsConfig.CLIENT_ID_CONFIG, options.appId + "-client");
        streamsConfiguration.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, options.broker);
        streamsConfiguration.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, options.offset);
        streamsConfiguration.put(ConsumerConfig.METADATA_MAX_AGE_CONFIG, 500);
        streamsConfiguration.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 10240);
        streamsConfiguration.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, 1000);
        streamsConfiguration.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 6000);
        streamsConfiguration.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,  Serdes.String().getClass().getName());
        streamsConfiguration.put(StreamsConfig.STATE_DIR_CONFIG, localStateDirectory.toAbsolutePath().toString());

        return new KafkaStreams(topology, streamsConfiguration);
    }

}