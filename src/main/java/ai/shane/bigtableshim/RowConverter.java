package ai.shane.bigtableshim;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.beam.sdk.schemas.Schema;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.Row;
import org.apache.beam.sdk.values.Row.Builder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import org.joda.time.DateTime;

public class RowConverter extends DoFn<Result, Row> {
    final private Schema cellSchema;
    final private Schema resultSchema;
    public RowConverter(Schema resultSchema, Schema cellSchema) {
        this.cellSchema = cellSchema;
        this.resultSchema = resultSchema;
    }
    @ProcessElement public void processElement(@Element Result element, OutputReceiver<Row> r) {
        r.output(convertElement(element));
    }
    public Row convertElement(Result element) {
        ArrayList<Row> entries = new ArrayList<>();
        while(element.advance()) {
            final Cell cell = element.current();
            final Row cv = Row.withSchema(cellSchema)
                .addValue(Arrays.copyOfRange(cell.getFamilyArray(), cell.getFamilyOffset(), cell.getFamilyOffset()+cell.getFamilyLength()))
                .addValue(Arrays.copyOfRange(cell.getQualifierArray(), cell.getQualifierOffset(), cell.getQualifierOffset()+cell.getQualifierLength()))
                .addValue(Arrays.copyOfRange(cell.getValueArray(), cell.getValueOffset(), cell.getValueOffset()+cell.getValueLength()))
                .addValue(new DateTime(cell.getTimestamp())).build();
            entries.add(cv);
        }
        Builder builder = Row.withSchema(resultSchema);
        return builder.addValue(entries).build();
    }
}