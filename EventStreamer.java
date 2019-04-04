/**
 * 
 */
package com.ms.kafka.com.ms.stream;

import java.util.Properties;
import java.util.stream.Stream;

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

import com.ms.kafka.com.ms.entity.TrackingEvent;
import com.ms.kafka.com.ms.entity.TrackingEventDeserializer;
import com.ms.kafka.com.ms.entity.TrackingEvnetSerializer;


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
	    props.put(StreamsConfig.APPLICATION_ID_CONFIG, "trackeventstream_stream");
	    props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
	    props.put(StreamsConfig.CLIENT_ID_CONFIG,"testappdi");
	    props.put("auto.offset.reset","earliest");
	    final StreamsBuilder builder = new StreamsBuilder();
	    final KStream<String , TrackingEvent> eventStream = builder.stream("rt_event_command_topic_stream",Consumed.with(Serdes.String(),
	    		Serdes.serdeFrom(new TrackingEvnetSerializer(), new TrackingEventDeserializer())));
	    KTable<String, Long> groupedByUniqueId = eventStream.groupBy((k,v) -> v.getUniqueid()).count(Materialized.as("count-store"));
		final KafkaStreams stream = new KafkaStreams(builder.build(), props);
	    stream.cleanUp(); stream.start();
	    System.out.println("Strema state : "+stream.state().name());
	    String queryableStoreName = groupedByUniqueId.queryableStoreName();
		 ReadOnlyKeyValueStore<Long , TrackingEvent> keyValStore = stream.store(queryableStoreName, QueryableStoreTypes.<Long,TrackingEvent>keyValueStore());
		 System.out.println(keyValStore.all());
	}
	
	public static <T> T waitUntilStoreIsQueryable(final String storeName,
			final QueryableStoreTypes queryableStoreType, final KafkaStreams streams) throws InterruptedException {
		while (true) {
			try {
				return streams.store(storeName, (QueryableStoreType<T>) queryableStoreType);
			} catch (InvalidStateStoreException ignored) {
// store not yet ready for querying
				System.out.println("system is waitng to ready for state store");
				Thread.sleep(100);
				//streams.close();
			}
		}
}

}
