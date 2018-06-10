package org.apache.flink.addons.hbase;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.util.Collector;

import akka.event.Logging.LogEvent;


public class Flink_test {
	public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<LogEvent> stream = env
        		// create stream from Kafka
               .addSource(new FlinkkafkaConsumer());
//                .keyBy("Timestamp");
                // window of size 60 minutes
//                .timeWindow(Time.minutes(60))
                // do operations per window
//                .apply(new OperationFunction());            
       
        env.execute();
    }

}
