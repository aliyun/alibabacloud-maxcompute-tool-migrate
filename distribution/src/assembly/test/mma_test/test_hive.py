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
import mma_test.utils as utils

from .timeout import Timeout


class TestHive(unittest.TestCase):

    timestamp = int(time.time())
    hive_db = "MMA_TEST"
    mc_project = utils.get_mc_config()["project_name"]

    migration_timeout_seconds = 300
    verification_timeout_seconds = 300

    mc_tables = set()

    @classmethod
    def tearDownClass(cls):
        mc_project = utils.get_mc_config()["project_name"]
        for mc_table in cls.mc_tables:
            utils.drop_mc_table(mc_project, mc_table)
        utils.drop_hive_partition(
            "mma_test",
            "TEST_PARTITIONED_100x10K",
            "p1=\"new_pt\", p2=123456")

    def test_non_partitioned_table(self):
        hive_table = "TEST_NON_PARTITIONED_1x100K"
        mc_table = "%s_%s" % (hive_table, str(self.timestamp))
        self.mc_tables.add(mc_table)

        try:
            with Timeout(self.migration_timeout_seconds):
                utils.migrate(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            with Timeout(self.verification_timeout_seconds):
                hive_avg, mc_avg = utils.verify(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            self.assertEquals(hive_avg, mc_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_partitioned_table_1(self):
        """
            Migrate whole table. The integer in test name ensures this case
            runs before test_partitioned_table_2 and test_partitioned_table_3
        """
        hive_table = "TEST_PARTITIONED_100x10K"
        mc_table = "%s_%s" % (hive_table, str(self.timestamp))
        self.mc_tables.add(mc_table)

        try:
            with Timeout(self.migration_timeout_seconds):
                utils.migrate(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            with Timeout(self.verification_timeout_seconds):
                hive_avg, mc_avg = utils.verify(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            self.assertEqual(hive_avg, mc_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_partitioned_table_2(self):
        """
            migrate a new partition
        """
        hive_table = "TEST_PARTITIONED_100x10K"
        mc_table = "%s_%s" % (hive_table, str(self.timestamp))
        self.mc_tables.add(mc_table)

        # create a new partition
        dml = "ALTER TABLE %s.%s ADD PARTITION(p1=\"new_pt\", p2=123456)" % (
            self.hive_db, hive_table)
        hive_command = "hive -e '%s'" % dml
        utils.execute_command(hive_command)

        # insert some data
        dml = "INSERT INTO TABLE %s.%s PARTITION(p1=\"new_pt\", p2=123456) SELECT extend_table(100)" % (
            self.hive_db, hive_table)
        hive_command = "hive -e '%s'" % dml
        utils.execute_command(hive_command)

        try:
            with Timeout(self.migration_timeout_seconds):
                utils.migrate(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            with Timeout(self.verification_timeout_seconds):
                hive_avg, mc_avg = utils.verify(
                      self.hive_db, hive_table, self.mc_project, mc_table)

            self.assertEqual(hive_avg, mc_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_partitioned_table_3(self):
        """
            migrate a modified partition
        """
        hive_table = "TEST_PARTITIONED_100x10K"
        mc_table = "%s_%s" % (hive_table, str(self.timestamp))
        self.mc_tables.add(mc_table)

        # insert some data
        dml = "INSERT INTO TABLE %s.%s PARTITION(p1=\"new_pt\", p2=123456) SELECT extend_table(100)" % (
          self.hive_db, hive_table)
        hive_command = "hive -e '%s'" % dml
        utils.execute_command(hive_command)

        try:
            with Timeout(self.migration_timeout_seconds):
                utils.migrate(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            with Timeout(self.verification_timeout_seconds):
                hive_avg, mc_avg = utils.verify(
                    self.hive_db, hive_table, self.mc_project, mc_table)

            self.assertEqual(hive_avg, mc_avg)
        except Exception as e:
            self.fail(traceback.format_exc())


if __name__ == '__main__':
    unittest.main()
