package com.lordofthejars.kstreamscat;

import javax.inject.Inject;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import io.quarkus.runtime.QuarkusApplication;
import io.quarkus.runtime.annotations.QuarkusMain;
import picocli.CommandLine;

@QuarkusMain
public class KStreamsCatMain implements QuarkusApplication {

    @Inject
    CommandLine.IFactory factory;

    @Override
    public int run(String... args) {

        KStreamsCatOptions options = new KStreamsCatOptions();
        new CommandLine(options, factory).parseArgs(args);
        
        if (options.help) {
            CommandLine.usage(options, System.out);
            return 0;
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
            final ReadOnlyKeyValueStore<?,?> keyValueStore = streams
                    .store(StoreNameGenerator.generate(options), QueryableStoreTypes.keyValueStore());

            final KeyValueIterator<?, ?> range = keyValueStore.all();
            while (range.hasNext()) {
                final KeyValue<?, ?> next = range.next();
                System.out.println(next.key + ": " + next.value);
            }
        }
        //streams.close();
        return 0;
    }
    
}