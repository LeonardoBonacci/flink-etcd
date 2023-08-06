package guru.bonacci.flink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;

public class EtcdSink extends RichSinkFunction<Tuple2<String, String>>{

	private static final long serialVersionUID = 1L;
	
	Client client;

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		client = Client.builder().endpoints("http://localhost:2379").build();
	}
	
	@Override
	public void close() throws Exception {
		super.close();
		client.close();
	}

	@Override
  public void invoke(Tuple2<String, String> kv, Context context) throws Exception {
		KV kvClient = client.getKVClient();
		ByteSequence key = ByteSequence.from(kv.f0.getBytes());
		ByteSequence value = ByteSequence.from(kv.f1.getBytes());

		kvClient.put(key, value).get();
	}
}
