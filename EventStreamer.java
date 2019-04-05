/**
 *
 */
package com.ms.kafka.com.ms.stream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.errors.InvalidStateStoreException;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.state.QueryableStoreType;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyKeyValueStore;

import java.util.Properties;


/**
 * @author vettri
 *
 */
public class EventStreamer {

    /**
     *
     */
    public EventStreamer() {
        // TODO Auto-generated constructor stub
    }

    public static void main(String[] args) throws InterruptedException {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trackeventstream_stream1");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.Long().getClass());
        props.put("auto.offset.reset","earliest");
        final StreamsBuilder builder = new StreamsBuilder();
        final KStream<String , String> eventStream =
                builder.stream("input",Consumed.with(Serdes.String(), Serdes.String()));

        KTable<String, Long> groupedByUniqueId = eventStream
                .groupBy((k, v) -> k)
                .count(Materialized.as("count-store"));
        final KafkaStreams stream = new KafkaStreams(builder.build(), props);
        stream.cleanUp(); stream.start();
        System.out.println("Strema state : "+stream.state().name());
        String queryableStoreName = groupedByUniqueId.queryableStoreName();

        waitUntilStoreIsQueryable(groupedByUniqueId.queryableStoreName(), QueryableStoreTypes.keyValueStore(), stream);

        ReadOnlyKeyValueStore<Long , String> keyValStore = stream.store(queryableStoreName, QueryableStoreTypes.<Long,String>keyValueStore());
        System.out.println(keyValStore.all());
    }

    public static <T> T waitUntilStoreIsQueryable(final String storeName,
                                                  final QueryableStoreType<T> queryableStoreType, final KafkaStreams streams) throws InterruptedException {
        while (true) {
            try {
                System.out.println("Check State:" + streams.state());
                return streams.store(storeName, (QueryableStoreType<T>) queryableStoreType);
            } catch (InvalidStateStoreException ignored) {
// store not yet ready for querying
                System.out.println("system is waitng to ready for state store::" + streams.state()
                );
                Thread.sleep(100);
                //streams.close();
            }
        }
    }

}
