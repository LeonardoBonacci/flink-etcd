package guru.bonacci.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class StreamingJob {

	public static void main(String[] args) throws Exception {

		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> initSource = env.fromElements(EtcdSource.KEYS);

		DataStream<Tuple2<String, String>> events = 
				initSource.map(new MapFunction<String, Tuple2<String, String>>() {

					@Override
					public Tuple2<String, String> map(String str) throws Exception {
						return Tuple2.of(str, "");
					}
				});
		
		DataStream<Tuple2<String, String>> etcdSource = env.addSource(new EtcdSource());
		
		events
			.union(etcdSource)
			.addSink(new EtcdSink());
		
		// execute program
		env.execute("Flink Etcd");
	}
}
