package guru.bonacci.flink;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;

public class EtcdSource extends RichParallelSourceFunction<Tuple2<String, String>>{

	private static final long serialVersionUID = 1L;

	static String[] KEYS = new String[] {"a", "b", "c"};

	Client client;

	volatile AtomicBoolean isRunning;
	
	
	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		client = Client.builder().endpoints("http://localhost:2379").build();
		isRunning = new AtomicBoolean(true);
	}
	
	@Override
	public void run(SourceContext<Tuple2<String, String>> ctx) throws Exception {
		while (isRunning.get()) {

			for (String k : KEYS) {
				KV kvClient = client.getKVClient();
				CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(k.getBytes()));

				GetResponse response = getFuture.get();

				System.out.println(response);
				
				ctx.collect(Tuple2.of(k,  response.getKvs().toString()));
			}

			System.out.println("---------------------------------------");
			Thread.sleep(1000);
		}
	}

	@Override
	public void cancel() {
		isRunning.set(false);
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		client.close();
	}
}
