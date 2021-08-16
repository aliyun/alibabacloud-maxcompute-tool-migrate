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

import argparse
import os
import unittest
import mma_test.utils as utils
import shutil
import time

from typing import Dict
from mma_test.test_hive import TestHive


def get_test_suites_map() -> Dict[str, unittest.TestSuite]:
    test_suites = {}
    test_suites[TestHive.__name__] = (
        unittest.defaultTestLoader.loadTestsFromTestCase(TestHive))

    return test_suites


if __name__ == '__main__':
    suites = get_test_suites_map()

    parser = argparse.ArgumentParser(description='MMA FT runner')
    parser.add_argument(
        "--list_test_suites",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="list available test suites")
    parser.add_argument(
        "--list_test_cases",
        required=False,
        type=str,
        help="list test cases of specified test suite")
    parser.add_argument(
        "--run_test_suite",
        required=False,
        help="run specified test suite")
    parser.add_argument(
        "--run_test_case",
        required=False,
        help="run specified test case, should be in format suite.case")
    parser.add_argument(
        "--fail_fast",
        required=False,
        const=True,
        action="store_const",
        default=False,
        help="fail fast")

    args = parser.parse_args()

    if args.list_test_suites:
        for suite in suites.keys():
            print(suite)
        exit(0)

    if args.list_test_cases is not None:
        suite_name = args.list_test_cases
        if suite_name in suites:
            suite = suites[suite_name]
            for test in suite._tests:
                print(test.id().split(".")[-1])
            exit(0)
        else:
            raise Exception("Test suite not found: %s" % suite_name)

    if args.run_test_suite is not None and args.run_test_case is not None:
        err_msg = ("--run_test_suite and "
                   "--run_test_case cannot present at the same time")
        raise Exception(err_msg)

    os.makedirs(utils.get_test_temp_dir(), exist_ok=True)
    print("Start MMA server")
    mma_server_sp = utils.start_mma_server()
    print("MMA server pid: %s" % str(mma_server_sp.pid))
    time.sleep(10)

    try:
        s = unittest.TestSuite()
        if args.run_test_suite is not None:
            if args.run_test_suite in suites:
                s.addTest(suites[args.run_test_suite])
            else:
                raise Exception("Invalid test suite")
        elif args.run_test_case is not None:
            splits = args.run_test_case.split(".")
            if len(splits) != 2:
                raise Exception("Invalid testcase: %s" % args.run_test_case)
            for test in suites[splits[0]]._tests:
                if splits[1] == test.id().split(".")[-1]:
                    s.addTest(test)
        else:
            s.addTests(suites.values())

        runner = unittest.TextTestRunner(
            verbosity=3, failfast=args.fail_fast, buffer=True)
        runner.run(s)
    finally:
        print("Stop MMA server")
        utils.stop_mma_server(mma_server_sp)
        shutil.rmtree(utils.get_test_temp_dir())


