package ai.shane.bigtableshim;

import org.junit.Test;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Mode;
import org.openjdk.jmh.annotations.OutputTimeUnit;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.TimeValue;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import java.util.Collection;
import java.util.concurrent.TimeUnit;

import org.apache.beam.sdk.io.BoundedSource;
import org.apache.beam.sdk.values.Row;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Result;

public class BigtableShimTest {
    @Test
    public void testShimConstructor() {
        // At least make sure we can construct the object. Ideally we want to ensure that certain methods
        // are compatible with cross language schema stuff
        BoundedSource<Result> result = BigtableShim.read("project-id", "instance-id", "table-id");
        assertNotNull(result);
    }
    @Test
    public void testShimCoder() {
        // Ensure codec is python compatible if possible
        BigtableShim shim = BigtableShim.From("project-id", "instance-id", "table-id");
        assertNotNull(shim);
    }
    @Test
    public void testResultRowConvert() {
        RowConverter converter = new RowConverter(BigtableShim.getResultSchema(), BigtableShim.getCellSchema());
        Cell[] cells = new Cell[]{
            CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, "family".getBytes(), "qualifier1".getBytes(), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), "value1".getBytes()),
            CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, "family".getBytes(), "qualifier2".getBytes(), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), "value2".getBytes()),
        };
        Result result = Result.create(cells);
        Row converted = converter.convertElement(result);
        // pull it back out.
        Collection<Row> rows = converted.getArray("result");
        for(Row r : rows) {
            // some basic checks.
            assertEquals("Same column family", new String(r.getBytes("family")), "family");
            // regex checks aren't beautiful but this covers qualifier1/value1 etc showing up in the output.
            assertTrue("rows have qualifier", new String(r.getBytes("qualifier")).matches("^qualifier\\d+$"));
            assertTrue("rows have correct value", new String(r.getBytes("value")).matches("^value\\d+$"));
        }
    }
    @Benchmark
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void benchRowConvert() {
        Cell[] cells = new Cell[]{
            CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, "family".getBytes(), "qualifier1".getBytes(), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), "value1".getBytes()),
            CellUtil.createCell(HConstants.EMPTY_BYTE_ARRAY, "family".getBytes(), "qualifier2".getBytes(), HConstants.LATEST_TIMESTAMP, KeyValue.Type.Maximum.getCode(), "value2".getBytes()),
        };
        RowConverter converter = new RowConverter(BigtableShim.getResultSchema(), BigtableShim.getCellSchema());
        Result result = Result.create(cells);
        for(int i=0;i<1000;i++) {
            converter.convertElement(result);            
        }
    }

}
