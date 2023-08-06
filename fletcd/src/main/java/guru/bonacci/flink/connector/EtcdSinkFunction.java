package guru.bonacci.flink.connector;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.table.data.RowData;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;

public class EtcdSinkFunction extends RichSinkFunction<RowData> {

	static String[] KEYS = new String[] {"a", "b", "c"};

  public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue();
  public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().noDefaultValue();

  private final String hostname;
  private final int port;

	Client client;

  public EtcdSinkFunction(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public void invoke(RowData data) throws Exception {
  		KV kvClient = client.getKVClient();
  		
  		ByteSequence key = ByteSequence.from(data.getString(0).toBytes());
  		ByteSequence value = ByteSequence.from(data.getString(1).toBytes());

  		kvClient.put(key, value).get();
  }

  @Override
  public void open(Configuration parameters) throws Exception {
  	client = Client.builder().endpoints("http://" + hostname + ":" + port).build();
  }

  @Override
  public void close() throws Exception {
  	client.close();
  }
}  
