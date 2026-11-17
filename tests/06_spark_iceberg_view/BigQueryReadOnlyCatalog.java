package poc;

import java.util.HashMap;
import java.util.Map;
import java.util.Collections;
import java.util.Set;
import org.apache.spark.sql.connector.catalog.*;
import org.apache.spark.sql.connector.read.ScanBuilder;
import org.apache.spark.sql.connector.read.Scan;
import org.apache.spark.sql.connector.read.SupportsPushDownRequiredColumns;
import org.apache.spark.sql.connector.read.SupportsPushDownFilters;
import org.apache.spark.sql.sources.Filter;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.connector.expressions.Transform;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.util.CaseInsensitiveStringMap;

/** Read-only name resolution; all scans are delegated to the official BigQuery provider. */
public final class BigQueryReadOnlyCatalog implements TableCatalog {
  private String catalogName;
  private Map<String, String> options;

  @Override public void initialize(String name, CaseInsensitiveStringMap config) {
    catalogName = name;
    options = new HashMap<>(config.asCaseSensitiveMap());
  }
  @Override public String name() { return catalogName; }
  @Override public Table loadTable(Identifier ident) {
    if (ident.namespace().length != 2) {
      throw new IllegalArgumentException("Use catalog.project.dataset.table");
    }
    Map<String, String> readOptions = new HashMap<>(options);
    readOptions.put("table", String.join(".", ident.namespace()) + "." + ident.name());
    try {
      TableProvider provider = (TableProvider) Class.forName(
          "com.google.cloud.spark.bigquery.v2.Spark35BigQueryTableProvider")
          .getDeclaredConstructor().newInstance();
      Table delegate = provider.getTable(null, new Transform[0], readOptions);
      return new ReadOnlyTable(delegate, readOptions);
    } catch (ReflectiveOperationException e) {
      throw new IllegalStateException("Spark 3.5 BigQuery V2 provider is required", e);
    }
  }
  private static final class ReadOnlyTable implements SupportsRead {
    private final Table delegate;
    private final Map<String, String> options;
    ReadOnlyTable(Table delegate, Map<String, String> options) {
      this.delegate = delegate;
      this.options = Collections.unmodifiableMap(new HashMap<>(options));
    }
    @Override public String name() { return delegate.name(); }
    @Override public StructType schema() { return delegate.schema(); }
    @Override public Map<String, String> properties() { return options; }
    @Override public Set<TableCapability> capabilities() {
      return Collections.singleton(TableCapability.BATCH_READ);
    }
    @Override public ScanBuilder newScanBuilder(CaseInsensitiveStringMap scanOptions) {
      Map<String, String> merged = new HashMap<>(options);
      merged.putAll(scanOptions.asCaseSensitiveMap());
      ScanBuilder scan = ((SupportsRead) delegate).newScanBuilder(new CaseInsensitiveStringMap(merged));
      return new ReadOnlyScan(scan, delegate.schema());
    }
  }
  private static final class ReadOnlyScan implements ScanBuilder,
      SupportsPushDownRequiredColumns, SupportsPushDownFilters {
    private final ScanBuilder delegate;
    private final StructType schema;
    ReadOnlyScan(ScanBuilder delegate, StructType schema) {
      this.delegate = delegate;
      this.schema = schema;
    }
    @Override public Scan build() { return delegate.build(); }
    @Override public void pruneColumns(StructType requested) {
      // The built-in V2 provider executes a BigQuery COUNT query on empty schemas,
      // regardless of optimizedEmptyProjection. Keep one column and count in Spark.
      StructType effective = requested;
      if (requested.isEmpty()) {
        effective = new StructType(new StructField[] {schema.fields()[0]});
      }
      if (delegate instanceof SupportsPushDownRequiredColumns) {
        ((SupportsPushDownRequiredColumns) delegate).pruneColumns(effective);
      }
    }
    @Override public Filter[] pushFilters(Filter[] filters) {
      return delegate instanceof SupportsPushDownFilters
          ? ((SupportsPushDownFilters) delegate).pushFilters(filters) : filters;
    }
    @Override public Filter[] pushedFilters() {
      return delegate instanceof SupportsPushDownFilters
          ? ((SupportsPushDownFilters) delegate).pushedFilters() : new Filter[0];
    }
  }
  private UnsupportedOperationException readOnly() {
    return new UnsupportedOperationException("Read-only BigQuery catalog; manage tables in BigQuery");
  }
  @Override public Identifier[] listTables(String[] namespace) { throw readOnly(); }
  @Override public Table createTable(Identifier id, StructType schema, Transform[] parts,
      Map<String, String> props) { throw readOnly(); }
  @Override public Table alterTable(Identifier id, TableChange... changes) { throw readOnly(); }
  @Override public boolean dropTable(Identifier id) { throw readOnly(); }
  @Override public void renameTable(Identifier oldId, Identifier newId) { throw readOnly(); }
}
