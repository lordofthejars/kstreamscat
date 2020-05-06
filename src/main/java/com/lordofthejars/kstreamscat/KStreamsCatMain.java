package com.lordofthejars.kstreamscat;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import picocli.CommandLine;

public class KStreamsCatMain {

    public static void main(String... args) {

        final KStreamsCatOptions options = CommandLine.populateCommand(new KStreamsCatOptions(), args);

        if (options.help) {
            CommandLine.usage(new KStreamsCatMain(), System.out);
            return;
        }

        final TopologyProducer topologyProducer = new TopologyProducer();
        final KafkaStreamsProducer kafkaStreamsProducer = new KafkaStreamsProducer();

        final KafkaStreams streams = kafkaStreamsProducer.buildKafkaStreams(topologyProducer.buildTopology(options),
                options);

        streams.cleanUp();
        System.out.println("Starting Kafka Streams...");
        streams.start();
        System.out.println("Kafka Stream Threads started");
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));

        if (options.globalKTable) {
            final ReadOnlyKeyValueStore<Integer, String> keyValueStore = streams
                    .store(StoreNameGenerator.generate(options), QueryableStoreTypes.keyValueStore());

            final KeyValueIterator<Integer, String> range = keyValueStore.all();
            while (range.hasNext()) {
                final KeyValue<Integer, String> next = range.next();
                System.out.println(next.key + ": " + next.value);
            }
        }
        //streams.close();
    }
    
}