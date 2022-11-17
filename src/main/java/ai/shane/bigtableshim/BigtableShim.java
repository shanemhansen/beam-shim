package ai.shane.bigtableshim;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.HashMap;
import java.util.List;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableScanConfiguration;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.io.Read;
import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.PTransform;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.ParDo.SingleOutput;
import org.apache.beam.sdk.values.PBegin;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.Builder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.apache.beam.sdk.schemas.Schema.Field;


public class BigtableShim extends PTransform<PBegin, PCollection<Row>> {
    private PTransform<PBegin, PCollection<Result>> wrapped;
    public static BoundedSource<Result> read(String projectId, String instanceId, String tableId) {
        CloudBigtableScanConfiguration btConfig = new CloudBigtableScanConfiguration.Builder()
        .withProjectId(projectId)
        .withTableId(tableId)
        .withInstanceId(instanceId).build();
        // TODO(shanemhansen) Scan and other options
        return CloudBigtableIO.read(btConfig);
    }
    public static BigtableShim From(String projectId, String instanceId, String tableId) {
        BigtableShim shim = new BigtableShim();
        shim.wrapped = Read.from(read(projectId, instanceId, tableId));
        return shim;
    }
    @Override
    public PCollection<Row> expand(PBegin input) {
        List<Field> fields = List.of(Field.of("json", Schema.FieldType.STRING));
        Schema s = new Schema(fields);
        SingleOutput<Result, Row> val = ParDo.of(new DoFn<Result, Row>() {
            @ProcessElement public void processElement(@Element Result element, OutputReceiver<Row> r) {
                HashMap<String, String> values = new HashMap<>();
                for(Cell value : element.listCells()) {
                    values.put(new String(value.getQualifierArray()), new String(value.getValueArray()));
                }
                Builder builder = Row.withSchema(s);
                ObjectMapper mapper = new ObjectMapper();
                try {
                    r.output(builder.addValue(mapper.writeValueAsString(values)).build());
                } catch (JsonProcessingException e) {
                    StringWriter sw = new StringWriter();
                    e.printStackTrace(new PrintWriter(sw));

                    r.output(builder.addValue("{\"error\": \"unable to convert row to json\"").build());
                    e.printStackTrace();
                }
            }});
        return input.apply(this.wrapped).apply(val).setRowSchema(s);
    }
}

