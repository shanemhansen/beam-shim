# Beam Shim
Utilities for working with Java Beam Sources/Sinks from python

## What's included

Currently this is a holding place for some example code for accessing BigTable via python in Apache Beam/Dataflow jobs.

## How to use

Why do we have to build any java code to use the java bigtable source?

Well in order to easily reference java Transforms from python you need
a simple java interface. Ideally one factory function that just requires primitive
types, or possibly a primitive type with a very simple configuration object. See [Creating cross language java transforms](https://beam.apache.org/documentation/programming-guide/#1311-creating-cross-language-java-transforms) for more info.

The other thing you need to do is use a serialization method that both java and python
understand. By default java often uses `Serializable` and python uses `pickle`. This
repo contains some supporting code to utilize Beam schemas and convert a bigtable result to a beam `Row` using schemas. Rows are supported in python/java. Each java bigtable `Result` is transformed into a Schema that looks something like:

```js
message CellValue {
    bytes family = 0;
    bytes qualifier = 1;
    bytes value = 2;
    Timestamp timestamp = 3;
}
message Result {
    repeated Cellvalue result = 0;
}
```

So in python you can access these results like so:

```python
# this would be in something passed to beam.Map(lambda val: ...)
for r in val.result:
    # make json serializable
    family = r.family.decode("utf-8")
    qualifier = r.qualifier.decode("utf-8")
    ts = r.timestamp.micros*1.0/1000/1000 # convert from integer microseconds to fractional seconds
    cell = {
        "family": family,
        "qualifier": qualifier,
        "timestamp": ts,
        "value":r.value.decode("utf-8")
    }
```


### Building the jar

To build the library run `make`. This essentially just does a `mvn install` and creates a `classpath` file that you'll need later.

## Use the jar in an external transform

Ensure you have dependencies installed using whatever dependency manager you use. When in doubt: `pip install apache-beam[gcp]`

See [python/bigtableexport.py](python/bigtableexport.py) for example of using java transform. You can use this transform to test your bigtable to gcs export or as a starting point. The pipeline can be invoked like so:

```sh
OUTPUT=gs://your-bucket/prefix
PROJECT=your-project
BIGTABLE_INSTANCE=your-bigtable-instance
BIGTABLE_TABLE=your-bigtable-table
python python/bigtableexport.py \
    --bigtableProjectId $PROJECT \
    --bigtableInstanceId $BIGTABLE_INSTANCE \
    --bigtableTableId $BIGTABLE_TABLE \
    --output $OUTPUT --classpath="$(cat classpath)" --runner=direct
```

You can view your results via gsutil:

```sh
gsutil cat ${OUTPUT}* | head
```

Expected results: 1 line of JSON per Result. Which is an array of Cells. Note that your results won't match those below because you are dumping from your table. You will need
to have an actual bigtable instance with data in order to test.
```
[{"family": "column", "qualifier": "greeting", "timestamp": 1668719170.905, "value": "Hello World!"}, {"family": "column", "qualifier": "greeting", "timestamp": 1668719033.208, "value": "Hello World!"}]
[{"family": "column", "qualifier": "greeting", "timestamp": 1668719171.015, "value": "Hello Cloud Bigtable!"}, {"family": "column", "qualifier": "greeting", "timestamp": 1668719033.209, "value": "Hello Cloud Bigtable!"}]
```



## Write your own pipelines using this transform

The [python/bigtableexport.py](python/bigtableexport.py) file shows that is needed to utilize a java external transform. It's only 3 small things:

- Add the import of JavaExternalTransform
- Define a classpath argument to your pipeline
- Add the JavaExternalTransform to your pipeline with the classpath defined above

```python
# place this line with the rest of your imports
from apache_beam.transforms.external import JavaExternalTransform
# you'll need to pass the classpath file in that was generated above. Add this where you pass your arguments
parser.add_argument(
    '--classpath',
    dest='classpath',
    required=True,
    help='list of jars required. eg a.jar:b.jar:c.jar'
)
# And finally use the transform in your pipeline
with beam.Pipeline(options=pipeline_options) as pipe:
    pipe | 'Read' \
            >> JavaExternalTransform(
                'ai.shane.bigtableshim.BigtableShim',
                classpath=classpath).From(
                    known_args.bigtableProjectId,
                    known_args.bigtableInstanceId,
                    known_args.bigtableTableId)

```

## Limitations

The java transform utilizes [CloudBigtableIO](https://cloud.google.com/bigtable/docs/dataflow-hbase-api/javadoc/com/google/cloud/bigtable/beam/CloudBigtableIO) which supports many options not specified here such as `Scan` config, filters, and key ranges.

## Troubleshooting

If you get errors about the jvm not starting. It's possible you are using an older version of Beam which generates invalid jars for long classpaths.