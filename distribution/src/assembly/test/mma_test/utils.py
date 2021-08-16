# Copyright 1999-2020 Alibaba Group Holding Ltd.
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


import configparser
import os
import signal
import subprocess
import time
import traceback

from concurrent.futures import ThreadPoolExecutor


def get_mma_home():
    return os.path.dirname(get_test_dir())


def get_test_dir():
    return os.path.dirname(os.path.dirname(os.path.realpath(__file__)))


def get_test_temp_dir():
    return os.path.join(get_test_dir(), "temp")


def get_conf_dir():
    return os.path.join(get_mma_home(), "conf")


def get_mc_config_path():
    return os.path.join(get_conf_dir(), "odps_config.ini")


def get_hive_config_path():
    return os.path.join(get_conf_dir(), "hive_config.ini")


def get_odpscmd_path():
    return os.path.join(get_mma_home(), "res", "odpscmd", "bin", "odpscmd")


def get_mma_server_path():
    return os.path.join(get_mma_home(), "bin", "mma-server")


def get_mma_client_path():
    return os.path.join(get_mma_home(), "bin", "mma-client")


def get_generate_config_path():
    return os.path.join(get_mma_home(), "bin", "generate-config")


def get_mc_config():
    parser = configparser.ConfigParser()
    with open(get_mc_config_path()) as fd:
        parser.read_string("[dummy section]\n" + fd.read())
    return parser["dummy section"]


def execute_command(cmd):
    try:
        print("Executing: %s" % cmd)

        sp = subprocess.Popen(cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              encoding='utf-8')
        stdout, stderr = sp.communicate()

        print("stdout: %s" % stdout)
        print("stderr: %s" % stderr)

        if sp.returncode != 0:
            raise Exception(
                "Execute %s failed, stdout: %s, stderr %s\n" % (cmd, stdout, stderr))
        return stdout, stderr
    except Exception as e:
        raise Exception(traceback.format_exc())


def execute_command_non_blocking(cmd) -> subprocess.Popen:
    try:
        print("Executing: %s" % cmd)
        return subprocess.Popen(cmd,
                                shell=True,
                                stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE,
                                preexec_fn=os.setsid,
                                encoding='utf-8')
    except Exception as e:
        raise Exception(traceback.format_exc())


def start_mma_server() -> subprocess.Popen:
    cmd = "sh %s" % (get_mma_server_path())
    return subprocess.Popen(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            preexec_fn=os.setsid,
                            encoding='utf-8')


def stop_mma_server(popen: subprocess.Popen):
    os.killpg(os.getpgid(popen.pid), signal.SIGKILL)


def generate_migration_config(mapping: dict) -> str:
    cur_time = int(time.time())
    table_mapping_path = os.path.join(
        get_test_temp_dir(), "temp_table_mapping_%s.txt" % str(cur_time))

    with open(table_mapping_path, 'w') as fd:
        for key in mapping.keys():
            hive_db, hive_table = key
            mc_db, mc_table = mapping[key]
            fd.write("%s.%s:%s.%s" % (hive_db, hive_table, mc_db, mc_table))

    cmd = "sh %s --table_mapping %s -m -p %s_" % (
        get_generate_config_path(), table_mapping_path, str(cur_time))
    execute_command(cmd)
    return os.path.join(
        get_test_temp_dir(), "%s_mma_migration_config.json" % str(cur_time))


def migrate(hive_db, hive_table, mc_project, mc_table):
    pwd = os.curdir
    os.chdir(get_test_temp_dir())

    try:
        migration_config_path = generate_migration_config(
            {(hive_db, hive_table): (mc_project, mc_table)})

        start_command = "sh %s --start %s" % (
            get_mma_client_path(), migration_config_path)
        wait_command = "sh %s --wait %s.%s" % (
            get_mma_client_path(), hive_db, hive_table)
        _, _ = execute_command(start_command)
        _, _ = execute_command(wait_command)
    finally:
        os.chdir(pwd)


def verify(hive_db,
           hive_table,
           mc_project,
           mc_table,
           hive_where_condition=None,
           mc_where_condition=None):

    def parse_hive_stdout(hive_stdout: str) -> float:
        return float(hive_stdout.strip())

    def parse_odps_stdout(odps_stdout: str) -> float:
        return float(odps_stdout.strip().split("\n")[1])

    executor = ThreadPoolExecutor(2)

    sql = "SELECT AVG(t_smallint) FROM %s.%s %s;"

    if hive_where_condition is None:
        hive_sql = sql % (hive_db, hive_table, "")
    else:
        hive_sql = sql % (hive_db, hive_table, "WHERE " + hive_where_condition)
    hive_command = "hive -e '%s'" % hive_sql

    if mc_where_condition is None:
        odps_sql = sql % (mc_project, mc_table, "")
    else:
        odps_sql = sql % (mc_project, mc_table, "WHERE " + mc_where_condition)
    odps_sql = "set odps.sql.allow.fullscan=true; " + odps_sql
    odps_command = "%s --config=%s -M -e '%s'" % (
        get_odpscmd_path(), get_mc_config_path(), odps_sql)

    hive_future = executor.submit(execute_command, hive_command)
    odps_future = executor.submit(execute_command, odps_command)
    hive_stdout, _ = hive_future.result()
    odps_stdout, _ = odps_future.result()

    return parse_hive_stdout(hive_stdout), parse_odps_stdout(odps_stdout)


def drop_mc_table(mc_project, mc_table):
    ddl = "DROP TABLE IF EXISTS %s.%s" % (mc_project, mc_table)
    command = "%s --config=%s -M -e '%s'" % (
        get_odpscmd_path(), get_mc_config_path(), ddl)
    execute_command(command)


def drop_mc_partition(mc_project, mc_table, mc_partition_spec):
    dml = "ALTER TABLE %s.%s DROP IF EXISTS PARTITION(%s)" % (
        mc_project, mc_table, mc_partition_spec)
    command = "%s --config=%s -M -e '%s'" % (
        get_odpscmd_path(), get_mc_config_path(), dml)
    execute_command(command)


def drop_hive_table(hive_db, hive_table):
    ddl = "DROP TABLE IF EXISTS %s.%s" % (hive_db, hive_table)
    command = hive_command = "hive -e '%s'" % ddl
    execute_command(command)


def drop_hive_partition(hive_db, hive_table, hive_partition_spec):
    dml = "ALTER TABLE %s.%s DROP IF EXISTS PARTITION(%s)" % (
        hive_db, hive_table, hive_partition_spec)
    command = hive_command = "hive -e '%s'" % dml
    execute_command(command)
