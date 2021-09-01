import os
import json
import unittest
from mma_test import utils

logger = utils.logger


def json2dict(file):
    return json.load(open(file))


class TestConfig(unittest.TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.mma = utils.MMA(init=False)
        cls.test_server_conf = os.path.join(cls.mma.test_res, 'mma_server_config.json')
        cls.test_server_conf2 = os.path.join(cls.mma.test_res, 'mma_server_config_2.json')
        cls.test_server_conf3 = os.path.join(cls.mma.test_res, 'mma_server_config_3.json')

    def test_gen_table_job_config(self):
        _, file = self.mma.gen_table_job_config('hive_db', 'hive_table', 'mc_db', 'mc_table')
        job_conf = json2dict(file)
        self.assertEqual(job_conf['mma.object.type'], 'TABLE')
        self.assertEqual(job_conf['mma.object.source.catalog.name'], 'hive_db')
        self.assertEqual(job_conf['mma.object.source.name'], 'hive_table')
        self.assertEqual(job_conf['mma.object.dest.catalog.name'], 'mc_db')
        self.assertEqual(job_conf['mma.object.dest.name'], 'mc_table')

    def test_gen_catalog_job_config(self):
        file = self.mma.gen_catalog_job_config('SOURCE', 'DEST', 'RANDOM_ID')
        job_conf = json2dict(file)
        self.assertEqual(job_conf['mma.object.type'], 'CATALOG')
        self.assertEqual(job_conf['mma.object.source.catalog.name'], 'SOURCE')
        self.assertEqual(job_conf['mma.object.dest.catalog.name'], 'DEST')
        self.assertEqual(job_conf['mma.job.id'], 'RANDOM_ID')

    def test_gen_server_config_mma(self):
        self._test_gen_server_config(self.test_server_conf, False)

    def test_gen_server_config_mma2(self):
        self._test_gen_server_config(self.test_server_conf2, True)

    def test_gen_server_config_mma3(self):
        self._test_gen_server_config(self.test_server_conf3, True)

    def _test_gen_server_config(self, ref_file, with_kerberos):
        ref = json2dict(ref_file)
        self.mma.gen_server_config(ref, with_kerberos)
        self.assertTrue(utils.verify_json(ref, json2dict(self.mma.server_conf)))
