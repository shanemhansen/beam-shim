package ai.shane.bigtableshim;

import java.util.List;

import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.client.Result;
import org.apache.beam.sdk.schemas.Schema.Field;
import org.apache.beam.sdk.schemas.Schema.FieldType;

import ai.shane.bigtableshim.RowConverter;

public class BigtableShim extends PTransform<PBegin, PCollection<Row>> {
    private PTransform<PBegin, PCollection<Result>> wrapped;
    private Schema resultSchema;
    private Schema cellSchema;
    private BigtableShim(Schema resultSchema, Schema cellSchema) {
        this.resultSchema = resultSchema;
        this.cellSchema = cellSchema;
    }
    public static BoundedSource<Result> read(String projectId, String instanceId, String tableId) {
        CloudBigtableScanConfiguration btConfig = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(projectId)
        .withTableId(tableId)
        .withInstanceId(instanceId).build();
        return CloudBigtableIO.read(btConfig);
    }
    public static BigtableShim From(String projectId, String instanceId, String tableId) {
        
        BigtableShim shim = new BigtableShim(getResultSchema(), getCellSchema());
        shim.wrapped = Read.from(read(projectId, instanceId, tableId));
        return shim;
    }
    @Override
    public PCollection<Row> expand(PBegin input) {
        return input.apply(this.wrapped).apply(ParDo.of(new RowConverter(resultSchema, cellSchema))).setRowSchema(resultSchema);
    }
    public static Schema getCellSchema() {
		// A bigtable Result is conceptually a List of Cells. And a Cell has a column family, key, and list of values
        List<Field> cellFields = List.of(
            Field.of("family", Schema.FieldType.BYTES),
            Field.of("qualifier", Schema.FieldType.BYTES),
            Field.of("value", Schema.FieldType.BYTES),
            Field.of("timestamp", Schema.FieldType.DATETIME)
        );
        return new Schema(cellFields);
    }
	public static Schema getResultSchema() {
        Schema row = Schema.builder().addArrayField("result", FieldType.row(getCellSchema())).build();
        return row;
	}
}

