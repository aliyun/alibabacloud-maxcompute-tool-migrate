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
import os
import argparse
import unittest

from typing import Dict
from mma_test.test_hive import TestHive
from mma_test.test_config import TestConfig


def get_test_suites_map() -> Dict[str, unittest.TestSuite]:
    test_suites = {
        TestHive.__name__: (unittest.defaultTestLoader.loadTestsFromTestCase(TestHive)),
        TestConfig.__name__: (unittest.defaultTestLoader.loadTestsFromTestCase(TestConfig))
    }
    return test_suites


def list_test_cases(level=0, filter=None):
    """
        level 0: list test suites
        level 1: list test cases
    """
    if filter is not None and filter not in suites:
        raise Exception("Test suite not found: %s" % filter)

    for suite_name in suites.keys():
        if filter is not None and suite_name != filter:
            continue

        print(suite_name)

        if level > 0:
            suite = suites[suite_name]
            for test in suite._tests:
                print('\t' + test.id().split(".")[-1])
    exit(0)


if __name__ == '__main__':
    suites = get_test_suites_map()

    parser = argparse.ArgumentParser(description='MMA FT runner')
    parser.add_argument("--list_all",
                        const=True,
                        action="store_const",
                        default=False,
                        help="list all test suites and cases")
    parser.add_argument("--list_test_suites",
                        const=True,
                        action="store_const",
                        default=False,
                        help="list available test suites")
    parser.add_argument("--list_test_cases",
                        type=str,
                        help="list test cases of specified test suite")
    parser.add_argument("--run",
                        nargs='?',
                        default='all',
                        help="[suite]/[suite.case] run specified test suite/case")
    parser.add_argument("--fail_fast",
                        const=True,
                        action="store_const",
                        default=True,
                        help="fail fast")

    args = parser.parse_args()

    if args.list_all:
        list_test_cases(1)

    if args.list_test_suites:
        list_test_cases(0)

    if args.list_test_cases is not None:
        list_test_cases(1, args.list_test_cases)

    s = unittest.TestSuite()

    if args.run is None:
        print("Run all cases")
        s.addTests(suites.values())
    elif "." not in args.run:
        test_suite = args.run
        print(f"Run test suite: {test_suite}")
        if test_suite in suites:
            s.addTest(suites[test_suite])
        else:
            raise Exception(f"Invalid test suite {test_suite}")
    else:
        splits = args.run.split('.')
        if len(splits) != 2:
            raise Exception(f"Invalid testcase: {args.run}")
        test_suite, test_case = splits
        print(f"Run test case: {test_suite}.{test_case}")
        for test in suites[splits[0]]._tests:
            if splits[1] == test.id().split(".")[-1]:
                s.addTest(test)

    runner = unittest.TextTestRunner(
        verbosity=3, failfast=args.fail_fast, buffer=True)
    runner.run(s)