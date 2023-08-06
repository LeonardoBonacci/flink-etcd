package guru.bonacci.flink.connector;

import java.util.concurrent.CompletableFuture;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;

import io.etcd.jetcd.ByteSequence;
import io.etcd.jetcd.Client;
import io.etcd.jetcd.KV;
import io.etcd.jetcd.kv.GetResponse;

public class EtcdSourceFunction extends RichSourceFunction<RowData> {

	static String[] KEYS = new String[] {"a", "b", "c"};

  public static final ConfigOption<String> HOST = ConfigOptions.key("host").stringType().noDefaultValue();
  public static final ConfigOption<Integer> PORT = ConfigOptions.key("port").intType().noDefaultValue();

  private final String hostname;
  private final int port;

	Client client;

  private volatile boolean isRunning = true;

  public EtcdSourceFunction(String hostname, int port) {
    this.hostname = hostname;
    this.port = port;
  }

  public void open(Configuration parameters) throws Exception {
  	client = Client.builder().endpoints("http://" + hostname + ":" + port).build();
  }

  @Override
  public void run(SourceContext<RowData> ctx) throws Exception {
    while (isRunning) {

			for (String k : KEYS) {
				KV kvClient = client.getKVClient();
				CompletableFuture<GetResponse> getFuture = kvClient.get(ByteSequence.from(k.getBytes()));

				GetResponse response = getFuture.get();

				System.out.println(response);
				
				ctx.collect(GenericRowData.of(
	          StringData.fromString(k),
	          StringData.fromString(response.getKvs().toString())
						)
				);
			}

			System.out.println("---------------------------------------");
			Thread.sleep(1000);
    }
  }

  @Override
  public void cancel() {
    isRunning = false;
    try {
      client.close();
    } catch (Throwable t) {
      // ignore
    }
  }
}  
