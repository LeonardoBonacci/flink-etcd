package guru.bonacci.flink.connector;

import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;

public class EtcdDynamicTableSource implements ScanTableSource {

  private final String hostname;
  private final int port;

  public EtcdDynamicTableSource(
      String hostname,
      int port) {
    this.hostname = hostname;
    this.port = port;
  }

  @Override
  public ChangelogMode getChangelogMode() {
    return ChangelogMode.newBuilder()
        .addContainedKind(RowKind.INSERT)
        .addContainedKind(RowKind.UPDATE_AFTER)
        .addContainedKind(RowKind.DELETE)
        .build();

  }

  @Override
  public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
    final SourceFunction<RowData> sourceFunction = new EtcdSourceFunction(
        hostname,
        port);

      return SourceFunctionProvider.of(sourceFunction, false);
  }

  @Override
  public DynamicTableSource copy() {
    return new EtcdDynamicTableSource(hostname, port);
  }

  @Override
  public String asSummaryString() {
    return "Etcd Table Source";
  }
}