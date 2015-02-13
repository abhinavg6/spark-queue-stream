package com.sapient.stream.process;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;

import scala.Tuple2;

import com.google.common.collect.ImmutableList;

/**
 * A spark streaming processor to ingest meetup events from a Java queue, and
 * calculate count of events per technology category of interest per streaming
 * batch.
 * 
 * @author abhinavg6
 *
 */
public class MeetupEventStream {

	// Map used for categorization of events
	private static final Map<String, String> categoryMap = new HashMap<String, String>();
	static {
		categoryMap.put("NoSQL", "NoSQL/Big Data");
		categoryMap.put("MongoDB", "NoSQL/Big Data");
		categoryMap.put("Neo4J", "NoSQL/Big Data");
		categoryMap.put("Cassandra", "NoSQL/Big Data");
		categoryMap.put("CouchDB", "NoSQL/Big Data");
		categoryMap.put("Storm", "NoSQL/Big Data");
		categoryMap.put("Spark", "NoSQL/Big Data");
		categoryMap.put("Spring", "Spring");
		categoryMap.put("Web", "Web Programming");
		categoryMap.put("HTML5", "Web Programming");
		categoryMap.put("websocket", "Web Programming");
		categoryMap.put("Javascript", "Web Programming");
		categoryMap.put("Node.js", "Web Programming");
		categoryMap.put("REST", "Web Programming");
		categoryMap.put("jQuery", "Web Programming");
		categoryMap.put("ExtJS", "Web Programming");
		categoryMap.put("Wordpress", "Web Programming");
		categoryMap.put("Grails", "Web Programming");
		categoryMap.put("Scala", "Scala");
		categoryMap.put("Python", "Python");
		categoryMap.put(".NET", "Microsoft tech");
		categoryMap.put("Windows", "Microsoft tech");
		categoryMap.put("Mobile", "Mobile");
		categoryMap.put("Android", "Mobile");
		categoryMap.put("iOS", "Mobile");
		categoryMap.put("NFC", "Mobile");
		categoryMap.put("AWS", "Cloud");
		categoryMap.put("Amazon Web", "Cloud");
		categoryMap.put("Azure", "Cloud");
		categoryMap.put("Google App Engine", "Cloud");
	}

	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("MeetupEventStream")
				.setMaster("local[*]");

		// Create the context
		JavaStreamingContext ssc = new JavaStreamingContext(sparkConf,
				new Duration(1000));

		// Create the queue through which RDDs can be pushed to a
		// QueueInputDStream
		Queue<JavaRDD<String>> eventRddQueue = new LinkedList<JavaRDD<String>>();
		// Read a events text file and add it as RDDs to the queue
		JavaRDD<String> events = ssc.sparkContext().textFile(
				"/Users/abhinavg6/spark-meetup/meetupevents.txt"); // A copy can
																	// be found
																	// in
																	// resources
		for (int i = 0; i < 100; i++) {
			eventRddQueue.add(events);
		}

		// Create the QueueInputDStream from the queue of event RDDs
		JavaDStream<String> inputEventStream = ssc.queueStream(eventRddQueue);

		// Filter the input stream by removing the ones with null country, and
		// get only the event name from the line
		JavaDStream<String> filteredEvents = inputEventStream.filter(
				new Function<String, Boolean>() {
					@Override
					public Boolean call(String eventLine) throws Exception {
						if (eventLine.endsWith("\"null\"")) {
							return false;
						}
						return true;
					}
				}).map(new Function<String, String>() {
			@Override
			public String call(String eventLine) throws Exception {
				String[] eventLineArr = eventLine.split("\",\"");
				return eventLineArr[1];
			}
		});

		// Convert each event to matching categories
		JavaDStream<String> eventCategories = filteredEvents
				.flatMap(new FlatMapFunction<String, String>() {
					@Override
					public Iterable<String> call(String event) throws Exception {
						List<String> emittableList = new ArrayList<String>();

						// Map an event to different categories - Not very
						// efficient as it uses a simple map for classification
						for (String keyword : categoryMap.keySet()) {
							boolean isMatched = Pattern
									.compile(Pattern.quote(keyword),
											Pattern.CASE_INSENSITIVE)
									.matcher(event).find();
							if (isMatched) {
								String category = categoryMap.get(keyword);
								if (!emittableList.contains(category)) {
									emittableList.add(category);
								}
							}
						}

						return (emittableList.isEmpty() ? ImmutableList
								.of("UNMATCHED") : emittableList);
					}
				});

		// Map key-value pairs by event category and count of 1
		JavaPairDStream<String, Integer> categoryOnes = eventCategories
				.mapToPair(new PairFunction<String, String, Integer>() {
					@Override
					public Tuple2<String, Integer> call(String eventCategory)
							throws Exception {
						return new Tuple2<String, Integer>(eventCategory, 1);
					}
				});

		// Reduce key-value pairs to counts per category
		JavaPairDStream<String, Integer> categoryCounts = categoryOnes
				.reduceByKey(new Function2<Integer, Integer, Integer>() {

					@Override
					public Integer call(Integer count1, Integer count2)
							throws Exception {
						return count1 + count2;
					}
				});

		categoryCounts.print();
		ssc.start();
		ssc.awaitTermination();
	}
}
