package guru.bonacci.flink.connector;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.sink.SinkFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class EtcdDynamicTableSink implements DynamicTableSink {

  private final String hostname;
  private final int port;

  public EtcdDynamicTableSink(
      String hostname,
      int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public String asSummaryString() {
    return "Etcd Table Sink";
  }

	@Override
	public ChangelogMode getChangelogMode(ChangelogMode requestedMode) {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .addContainedKind(RowKind.DELETE)
        .build();
	}

	@Override
	public SinkRuntimeProvider getSinkRuntimeProvider(Context context) {
    final SinkFunction<RowData> sinkFunction = new EtcdSinkFunction(
        hostname,
        port);

    return SinkFunctionProvider.of(sinkFunction, 4);
	}

	@Override
	public DynamicTableSink copy() {
		 return new EtcdDynamicTableSink(hostname, port);	}
}