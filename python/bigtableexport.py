#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""A bigtable to gcs export demo using java external transform"""


import os
import argparse
import logging
import glob
import hashlib
import zipfile
from typing import NamedTuple, Dict

import apache_beam as beam
from apache_beam.io import WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.transforms.external import JavaExternalTransform
from apache_beam.utils.subprocess_server import JavaJarServer

class ConfigMap(NamedTuple):
    values: Dict[str, str]

def run(argv=None) -> None:
    """Main entry point; defines and runs the export pipeline."""
    parser = argparse.ArgumentParser()
    parser.add_argument(
        '--classpath',
        dest='classpath',
        required=True,
        help='list of jars required. eg a.jar:b.jar:c.jar'
    )
    parser.add_argument(
        '--bigtableProjectId',
        dest='bigtableProjectId',
        required=True,
        help='projectId for bigtable instance'
    )
    parser.add_argument(
        '--bigtableInstanceId',
        dest='bigtableInstanceId',
        required=True,
        help='instance ID for bigtable'
    )
    parser.add_argument(
        '--bigtableTableId',
        dest='bigtableTableId',
        required=True,
        help='Bigtable table to scan'
    )
    parser.add_argument(
        '--output',
        dest='output',
        required=True,
        help='Output file to write results to.')
    known_args, pipeline_args = parser.parse_known_args(argv)
    pipeline_options = PipelineOptions(pipeline_args)

    # The pipeline will be run on exiting the with block.
    import json
    with beam.Pipeline(options=pipeline_options) as pipe:
        # pylint: disable=unsupported-binary-operation
        classpath = known_args.classpath.split(":")
        lines = pipe | 'Read' \
            >> JavaExternalTransform(
                'ai.shane.bigtableshim.BigtableShim',
                classpath=classpath).From(
                    known_args.bigtableProjectId,
                    known_args.bigtableInstanceId,
                    known_args.bigtableTableId, ConfigMap(values={}))
        # pylint: disable=expression-not-assigned
        lines | beam.Map(lambda val: json.dumps([
            {
                "family":r.family.decode("utf-8"),
                "qualifier": r.qualifier.decode("utf-8"),
                "timestamp":r.timestamp.micros*1.0/1000/1000,
                "value":r.value.decode("utf-8")
                } for r in val.result])
                ) | 'Write' >> WriteToText(known_args.output)


def patch_subprocess_server() -> None:
    """
    patch_subprocess_server fixes a bug where line wrapping is not applied
    to a large list of jar files, resulting in an invalid jar.
    """
    def make_classpath_jar(cls, main_jar, extra_jars, cache_dir=None):
        if cache_dir is None:
            cache_dir = cls.JAR_CACHE
        composite_jar_dir = os.path.join(cache_dir, 'composite-jars')
        os.makedirs(composite_jar_dir, exist_ok=True)
        classpath = []
        # Class-Path references from a jar must be relative, so we create
        # a relatively-addressable subdirectory with symlinks to all the
        # required jars.
        for pattern in [main_jar] + list(extra_jars):
            for path in glob.glob(pattern) or [pattern]:
                path = os.path.abspath(path)
                rel_path = hashlib.sha256(
                    path.encode('utf-8')).hexdigest() + os.path.splitext(path)[1]
                classpath.append(rel_path)
                if not os.path.lexists(os.path.join(composite_jar_dir, rel_path)):
                    os.symlink(path, os.path.join(composite_jar_dir, rel_path))
        # Now create a single jar that simply references the rest and has the same
        # main class as main_jar.
        composite_jar = os.path.join(
            composite_jar_dir,
            hashlib.sha256(' '.join(sorted(classpath)
                                    ).encode('ascii')).hexdigest()
            + '.jar')
        if not os.path.exists(composite_jar):
            with zipfile.ZipFile(main_jar) as main:
                with main.open('META-INF/MANIFEST.MF') as manifest:
                    main_class = next(
                        filter(lambda line: line.startswith(b'Main-Class: '), manifest))
            with zipfile.ZipFile(composite_jar + '.tmp', 'w') as composite:
                with composite.open('META-INF/MANIFEST.MF', 'w') as manifest:
                    manifest.write(b'Manifest-Version: 1.0\n')
                    manifest.write(main_class)
                    manifest.write(
                        b'Class-Path: ' + '\n  '.join(classpath).encode('ascii') + b'\n')
            os.rename(composite_jar + '.tmp', composite_jar)
        return composite_jar
    setattr(JavaJarServer, "make_classpath_jar", make_classpath_jar)


if __name__ == '__main__':
    logging.getLogger().setLevel(logging.INFO)
    # patch_subprocess_server is only needed for older versions of beam uncomment the below line to apply
    # a fix to make large classpaths work on older versions.
    # patch_subprocess_server()
    run()
