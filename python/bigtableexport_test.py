from contextlib import closing
import logging
import os
import socket
import subprocess
import time
from typing import NamedTuple, Dict, Tuple, List
import unittest


import apache_beam as beam
from apache_beam.testing.test_pipeline import TestPipeline
from apache_beam.transforms.external import JavaExternalTransform
from apache_beam.testing.util import assert_that
from apache_beam.testing.util import equal_to

logger = logging.getLogger(__name__)

class BigtableArguments(NamedTuple):
    bigtableProjectId: str
    bigtableInstanceId: str
    bigtableTableId: str

class ConfigMap(NamedTuple):
    values: Dict[str, str]

def filldata(n: int, btArgs: BigtableArguments, family: str):
    dirname = os.path.dirname(__file__)
    fill_script = os.path.join(dirname, "filldata.py")
    subprocess.check_call([fill_script, "--bigtableProjectId", btArgs.bigtableProjectId,
        "--bigtableInstanceId", btArgs.bigtableInstanceId,
        "--bigtableTable", btArgs.bigtableTableId,
        "--columnFamily", family,
        '--count', str(n)], stderr=subprocess.STDOUT)


def racy_free_address() -> Tuple[str, str]:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind(('127.0.0.1', 0))
        host, port = s.getsockname()
        return host, str(port)

def wait_for_port(host: str, port: int, timeout_s:float=10.0):
    start = time.time()
    deadline = start+timeout_s
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise Exception(f"Unable to get connection within timeout: {timeout_s}")
        try:
            socket.create_connection((host, port), timeout=remaining)
            return
        except OSError:
            pass # 99/111 all possible errors.
        time.sleep(10*1e-3) # 10 millisecond
        
            
    
class BeamShimTest(unittest.TestCase):

    def setUp(self) -> None:
        self.old_emulator = os.environ.get("BIGTABLE_EMULATOR_HOST", None)
        host, port = racy_free_address()
        addr = f"{host}:{port}"
        self.emulator = subprocess.Popen(['gcloud', 'beta', 'emulators', 'bigtable', 'start', f'--host-port={addr}'])
        # wait for port to be open
        wait_for_port(host, int(port), timeout_s=5)
        os.environ["BIGTABLE_EMULATOR_HOST"] = addr
        self.known_args = BigtableArguments(bigtableProjectId="fake", bigtableInstanceId="test_instance", bigtableTableId="test_table")
        self.family = "family_test"
        subprocess.check_output(["cbt",
            "-project", self.known_args.bigtableProjectId,
            "-instance", self.known_args.bigtableInstanceId,
            "createtable", self.known_args.bigtableTableId,
            "families={}".format(self.family)], stderr=subprocess.STDOUT)
        # load data
        self.record_count = 10
        filldata(10, self.known_args, self.family)
        # the beam java expansion server really barfs alot of stuff out.
        if os.environ.get('BEAM_SHIM_VERBOSE', '') != '1':
            logger = logging.getLogger('root')
            logger.setLevel(logging.CRITICAL) # TODO: either fix or silence only the lines about unclear grpc channel shutdown. This is way too heavy of a stick.
        
    def classpath(self) -> List[str]:
        try:
            return os.environ["CLASSPATH"].split(':')
        except KeyError:
            raise Exception("CLASSPATH must be defined. Try: make all; CLASSPATH=$(cat classpath) python -m unittest python/bigtableexport_test.py")
        
    def test_beam_shim(self) -> None:
        classpath = self.classpath()
        known_args = BigtableArguments(self.known_args.bigtableProjectId, self.known_args.bigtableInstanceId, self.known_args.bigtableTableId)
        values: Dict[str, str] = {
         "google.bigtable.emulator.endpoint.host": os.environ["BIGTABLE_EMULATOR_HOST"],
        }
        config = ConfigMap(values=values)
        with TestPipeline() as p:
            external = JavaExternalTransform(
                    'ai.shane.bigtableshim.BigtableShim',
                    classpath=classpath).From(
                        known_args.bigtableProjectId,
                        known_args.bigtableInstanceId,
                        known_args.bigtableTableId,
                        config)
            # pylint: disable=unsupported-binary-operation
            output = p | external | beam.combiners.Count.Globally()
            assert_that(output, equal_to([self.record_count])) #TODO make 3/n a parameter of filldata and/or make filldata not an external script.
    def tearDown(self) -> None:
        # clean up test tables
        try:
            subprocess.check_output(["cbt",
                "-project", self.known_args.bigtableProjectId,
                "-instance", self.known_args.bigtableInstanceId,
                'deletetable', self.known_args.bigtableTableId], stderr=subprocess.STDOUT)
        except subprocess.CalledProcessError as e:
            logger.error("cbt deletetable failed: %s", e)
        if self.emulator is not None:
            self.emulator.kill()
            self.emulator.wait()
        if self.old_emulator is not None:
            os.environ["BIGTABLE_EMULATOR_HOST"] = self.old_emulator

            