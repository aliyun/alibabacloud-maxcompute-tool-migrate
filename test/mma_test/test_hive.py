# Copyright 1999-2019 Alibaba Group Holding Ltd.
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

import time
import traceback
import unittest
import mma_test.utils as utils


class TestHive(unittest.TestCase):

    mc_tables = []

    def tearDownClass(self):
        mc_project = utils.get_mc_config()["project_name"]
        for mc_table in self.mc_tables:
            utils.drop_mc_table(mc_project, mc_table)

    def test_non_partitioned_table(self):
        timestamp = int(time.time())
        hive_db = "MMA_TEST"
        hive_table = "TEST_NON_PARTITIONED_1x100K"
        mc_project = utils.get_mc_config()["project_name"]
        mc_table = "%s_%s" % (hive_table, str(timestamp))

        try:
            utils.migrate(hive_db, hive_table, mc_project, mc_table)
            hive_avg, mc_avg = utils.verify(
                hive_db, hive_table, mc_project, mc_table)
            self.assertEquals(hive_avg, mc_avg)
        except Exception as e:
            self.fail(traceback.format_exc())

    def test_partitioned_table(self):
        pass

    def test_partitioned_table_with_new_partition(self):
        pass

    def test_partitioned_table_with_modified_partition(self):
        pass


if __name__ == '__main__':
    unittest.main()
