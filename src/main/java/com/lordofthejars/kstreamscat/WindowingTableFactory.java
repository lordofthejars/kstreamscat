package com.lordofthejars.kstreamscat;

import java.time.Duration;
import java.util.Optional;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.SessionWindows;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;

public class WindowingTableFactory {
    
    public static Optional<KTable<Windowed<String>,Long>> countWithWindow(StreamsBuilder builder, KStreamsCatOptions options) {
        
        KTable<Windowed<String>,Long> table = null;

        if (options.timeWindow > 0) {
            final KStream<String, String> stream = builder.stream(options.topic, Consumed.with(Serdes.String(), Serdes.String()));
            table = stream.groupByKey()
                .windowedBy(TimeWindows.of(Duration.ofSeconds(options.timeWindow)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));
        }

        if (options.sessionWindow > 0) {
            final KStream<String, String> stream = builder.stream(options.topic, Consumed.with(Serdes.String(), Serdes.String()));
            table = stream.groupByKey()
                .windowedBy(SessionWindows.with(Duration.ofSeconds(options.sessionWindow)))
                .count(Materialized.with(Serdes.String(), Serdes.Long()));
        }

        return Optional.ofNullable(table);
    }

}