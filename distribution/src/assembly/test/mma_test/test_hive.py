#
# Copyright 1999-2021 Alibaba Group Holding Ltd.
# 
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# 
#      http://www.apache.org/licenses/LICENSE-2.0
# 
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
import time
import traceback
import unittest
from mma_test.utils import logger
from mma_test import utils
from .timeout import Timeout


class TestHive(unittest.TestCase):
    timestamp = int(time.time())

    hive_db = 'MMA_TEST'
    non_partition_table = 'test_text_1x1k'
    partition_table = "test_text_partitioned_10x1k"
    date_partition_table = "test_date_partitioned"
    test_new_pt = 'p1="new_pt", p2=123456'
    mc_project = 'mma_test'

    migration_timeout_seconds = 300
    verification_timeout_seconds = 300

    mc_tables = set()
    mma = utils.MMA()
    odps = utils.Odps(mma)
    hive = utils.Hive()

    @classmethod
    def setUpClass(cls) -> None:
        cls.mma.start_server()
        cls.hive.drop_partition(cls.hive_db, cls.partition_table, cls.test_new_pt)

    @classmethod
    def tearDownClass(cls):
        cls.mma.stop_server()
        time.sleep(5)
        for mc_table in cls.mc_tables:
            print(f'drop mc table {mc_table}')
            cls.odps.drop_table(cls.mc_project, mc_table)
        cls.hive.drop_partition(cls.hive_db, cls.partition_table, cls.test_new_pt)

    def verify(self, hive_table, mc_table):
        with Timeout(self.verification_timeout_seconds):
            hive_avg, mc_avg = utils.verify(
                self.mma, self.hive_db, hive_table, self.mc_project, mc_table)

        logger.info(f"VERIFY: {hive_avg} == {mc_avg}")
        self.assertEqual(hive_avg, mc_avg)

    def migrate_and_verify(self, hive_table, mc_table, pt=None):
        try:
            with Timeout(self.migration_timeout_seconds):
                jobid = self.mma.migrate(
                    self.hive_db, hive_table, self.mc_project, mc_table, pt)
                self.mma.wait_until_finish(jobid, 20)

            if pt is None:
                self.verify(hive_table, mc_table)
            else:
                # todo varify partition
                pass

        except Exception as e:
            # logger.info(f"VERIFY FAIL: hive {hive_avg} != mc {mc_avg}")
            self.fail(traceback.format_exc())

    def _get_mc_table(self, hive_table):
        mc_table = "%s_%s" % (hive_table, str(self.timestamp))
        self.mc_tables.add(mc_table)
        return mc_table

    def test_non_partitioned_table(self):
        """
            Migrate whole non partitioned table.
        """
        logger.info('Test non partitioned table migrate')
        hive_table = self.non_partition_table
        mc_table = self._get_mc_table(hive_table)
        self.migrate_and_verify(hive_table, mc_table)

    def test_partitioned_table_1(self):
        """
            Migrate whole table. The integer in test name ensures this case
            runs before test_partitioned_table_2 and test_partitioned_table_3
        """
        logger.info('Test partitioned table migrate')
        hive_table = self.partition_table
        mc_table = self._get_mc_table(hive_table)
        self.migrate_and_verify(hive_table, mc_table)

    def test_partitioned_table_2(self):
        """
            migrate a new partition
        """
        logger.info('Test migrate new partition')
        hive_table = self.partition_table
        mc_table = self._get_mc_table(hive_table)

        # create a new partition & insert some data
        self.hive.new_pt(self.hive_db, hive_table, self.test_new_pt)
        self.hive.insert_data_to_pt(self.hive_db, hive_table, self.test_new_pt)

        self.migrate_and_verify(hive_table, mc_table)

    def test_partitioned_table_3(self):
        """
            migrate a modified partition
        """
        logger.info('Test migrate modified partition')
        hive_table = self.partition_table
        mc_table = self._get_mc_table(hive_table)

        # insert some data
        self.hive.insert_data_to_pt(self.hive_db, hive_table, self.test_new_pt)
        self.migrate_and_verify(hive_table, mc_table)

    def test_partition_filter(self):
        """
            migrate specific partition
        """
        logger.info('Test partition filter')
        pt = {'begin': 'ZhpSC/7730', 'end': 'ZhpSC/7730', 'orders': 'lex/num'}
        hive_table = self.partition_table
        mc_table = self._get_mc_table(hive_table)

        self.migrate_and_verify(hive_table, mc_table, pt)

    def test_catalog(self):
        # todo
        pass

    def test_breakpoint_reset(self):
        """
            submit job -> wait for running -> running for 10s -> stop job -> reset job
        """
        logger.info('Test Stop job -> Reset job')
        hive_table = self.non_partition_table
        mc_table = self._get_mc_table(hive_table)
        jobid = self.mma.migrate(self.hive_db, hive_table, self.mc_project, mc_table)

        self.mma.wait_until(jobid, ['RUNNING'])
        time.sleep(10)
        self.assertEqual(self.mma.status(jobid), 'RUNNING')

        self.mma.stop(jobid)

        self.mma.reset(jobid)
        self.mma.wait_until_finish(jobid)
        self.verify(hive_table, mc_table)

    def test_kill_server(self):
        """
            submit job -> wait for running -> running for 10s -> kill server -> restart server
        """
        logger.info('Test kill server -> Reset job')
        hive_table = self.non_partition_table
        mc_table = self._get_mc_table(hive_table)
        jobid = self.mma.migrate(self.hive_db, hive_table, self.mc_project, mc_table)

        self.mma.wait_until(jobid, ['RUNNING'])
        time.sleep(10)
        self.assertEqual(self.mma.status(jobid), 'RUNNING')

        self.mma.stop_server()
        self.mma.start_server()

        self.mma.wait_until_finish(jobid)
        self.verify(hive_table, mc_table)


if __name__ == '__main__':
    unittest.main()
