package com.lordofthejars.kstreamscat;

import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;

import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueStore;

public class TopologyProducer {
    
    public Topology buildTopology(KStreamsCatOptions options) {
        final StreamsBuilder builder = new StreamsBuilder();

        if (options.globalKTable) {
            builder.globalTable(
                            options.topic,
                            Materialized.<Bytes, String, KeyValueStore<Bytes, byte[]>>as(
                            StoreNameGenerator.generate(options))
                            .withKeySerde(Serdes.Bytes())
                            .withValueSerde(Serdes.String())
                            );
        }

        
        Optional<KTable<Windowed<String>, Long>> table = WindowingTableFactory.countWithWindow(builder, options);

        table.ifPresent(t  -> {
            t.toStream().foreach((windowKey, count) -> {
                if (windowKey.key() != null && count != null) {
                    System.out.println("Window: "+ windowKey.window().start() + " -> Key: " + windowKey.key() + " = " + count);
                }
            });
        });

        return builder.build();
    }   

}