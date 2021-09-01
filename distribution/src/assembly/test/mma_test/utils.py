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
import re
import os
import signal
import subprocess
import time
import traceback
import json
from concurrent.futures import ThreadPoolExecutor
from functools import wraps
from typing import Tuple
import logging


def get_logger():
    """
        debug msg in file
        info msg in file & console
    """
    log_formatter = logging.Formatter('%(asctime)s [%(levelname)-5.5s]  %(message)s')
    logger = logging.getLogger()
    logger.setLevel(logging.DEBUG)

    file_handler = logging.FileHandler('test.log')
    file_handler.setFormatter(log_formatter)
    file_handler.setLevel(logging.DEBUG)
    logger.addHandler(file_handler)

    console_handler = logging.StreamHandler()
    console_handler.setFormatter(log_formatter)
    console_handler.setLevel(logging.INFO)
    logger.addHandler(console_handler)
    return logger


logger = get_logger()


def execute_command(cmd):
    try:
        logger.debug('==================== START EXEC =====================')
        logger.debug(f'[CMD]: {cmd}')

        cp = subprocess.run(cmd,
                            shell=True,
                            stdout=subprocess.PIPE,
                            stderr=subprocess.PIPE,
                            encoding='utf-8')

        logger.debug(f'[STDOUT]: {cp.stdout.strip()}')
        logger.debug(f'[STDERR]: {cp.stderr.strip()}')
        cp.check_returncode()

        logger.debug('==================== END   EXEC =====================')
        return cp.stdout, cp.stderr
    except Exception as e:
        print(f'[ERROR]: Execute failed')
        raise Exception(traceback.format_exc())


def execute_command_non_blocking(cmd) -> Tuple[int, str, str]:
    try:
        logger.debug('==================== START EXEC =====================')
        logger.debug('[: %s' % cmd)
        sp = subprocess.Popen(cmd,
                              shell=True,
                              stdout=subprocess.PIPE,
                              stderr=subprocess.PIPE,
                              preexec_fn=os.setsid,
                              encoding='utf-8')
        pgid = os.getpgid(sp.pid)
        out, err = sp.communicate()
        if sp.returncode != 0:
            raise RuntimeError()
        logger.debug('==================== START EXEC =====================')
        return pgid, out, err
    except Exception as e:
        print(f'[ERROR]: Execute failed')
        raise Exception(traceback.format_exc())


def write_line(p, v):
    p.stdin.write(v + '\n')


class MMA:
    """
        config key for config test
    """
    hive_config = [
        'mma.metadata.source.hive.metastore.uris',
        'mma.data.source.hive.jdbc.url',
        'mma.data.source.hive.jdbc.username',
        'mma.data.source.hive.jdbc.password',
    ]

    kerberos_config = [
        'java.security.auth.login.config',
        'java.security.krb5.conf',
        'mma.metadata.source.hive.metastore.kerberos.principal',
        'mma.metadata.source.hive.metastore.kerberos.keytab.file'
    ]

    mc_config = [
        'mma.data.dest.mc.endpoint',
        'mma.job.execution.mc.project',
        'mma.data.dest.mc.access.key.id',
        'mma.data.dest.mc.access.key.secret',
    ]

    def __init__(self, init=True):
        """ init dir structure
        path <-------------mapping table------------------> class field:
        mma_latest                                          # home
        ├── assembly.xml
        ├── bin                                             # bin
        │    ├── configure                                  # configure
        │    ├── gen-job-conf                               # job_conf
        │    ├── mma-client                                 # client
        │    └── mma-server                                 # server
        ├── conf                                            # conf
        │    ├── mma_server_config.json                     # server_config
        │    ├── gss-jaas.conf.template
        │    └── table_mapping.txt.template
        ├── res
        │    ├── h2
        │    │    └── ...
        │    └── ...
        └── test                                            # test
            ├── mma_test
            │    ├── __init__.py
            │    ├── test_config.py
            │    ├── test_hive.py
            │    ├── timeout.py
            │    └── utils.py
            ├── resources                                   # test_res
            │    └── job config / server config / ... templates
            ├── temp                                        # test_temp
            │    └── odps_config.ini                        # odps_config
            ├── setup.py
            ├── setup.sql
            └── test.py
        env
        └── odpscmd
            └── bin
                └── odpscmd                                 # odpscmd
        """
        self.test = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))
        self.test_temp = os.path.join(self.test, 'temp')
        os.makedirs(self.test_temp, exist_ok=True)
        time.sleep(1)  # wait mkdir finish
        self.test_res = os.path.join(self.test, 'resources')

        self.home = os.path.dirname(self.test)

        self.conf = os.path.join(self.home, 'conf')
        self.server_conf = os.path.join(self.conf, 'mma_server_config.json')
        if init:
            if not os.path.exists(self.server_conf):
                raise RuntimeError('conf/mma_server_config.json not found\n'
                                   'use: cp test/resources/mma_server_config_{n}.json conf/mma_server_config.json'
                                   ' to set')
            self.mma_server_config = json.load(open(self.server_conf))

        self.bin = os.path.join(self.home, 'bin')
        self.server = os.path.join(self.bin, 'mma-server')
        self.client = os.path.join(self.bin, 'mma-client')
        self.configure = os.path.join(self.bin, 'generate')
        self.job_conf = os.path.join(self.bin, 'gen-job-conf')

        self.odps_config = os.path.join(self.test_temp, 'odps_config.ini')
        self.odpscmd = os.path.join(os.path.dirname(self.home), 'env', 'odpscmd', 'bin', 'odpscmd')
        if not os.path.exists(self.odpscmd):
            raise RuntimeError(f'{self.odpscmd} not found')
        if init:
            self._init_odps_config()
        self.pgid = None

    def _init_odps_config(self):
        # create temp/odps_config.ini from mma_server_config.json
        with open(self.odps_config, 'w') as f:
            f.write(f'access_id={self.mma_server_config["mma.data.dest.mc.access.key.id"]}\n')
            f.write(f'access_key={self.mma_server_config["mma.data.dest.mc.access.key.secret"]}\n')
            f.write(f'project_name={self.mma_server_config["mma.job.execution.mc.project"]}\n')
            f.write(f'end_point={self.mma_server_config["mma.data.dest.mc.endpoint"]}\n')

    # ======================== Server =============================
    def start_server(self):
        # start mma server
        logger.info('Start mma server')
        self.pgid, stdout, _ = execute_command_non_blocking(f'{self.server}')

        stdout = ''.join(stdout.split('\n'))
        pattern = r'(.*), PID=(.*)'
        m = re.match(pattern, stdout)
        pid = m.group(2)
        logger.info(f'mma server start in {pid}')

    def stop_server(self):
        # stop mma server
        logger.info('Stop mma server')
        if self.pgid is not None:
            print(f'kill server in {self.pgid}')
            os.killpg(self.pgid, signal.SIGKILL)

    # ======================== Config =============================
    def gen_server_config(self, ref: dict, with_kerberos: bool):
        # generate mma_server_config.json from reference dict
        p = subprocess.Popen(['./bin/configure'],
                             stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE,
                             universal_newlines=True)
        for key in MMA.hive_config:
            write_line(p, ref[key])

        if with_kerberos:
            write_line(p, 'Y')
            for key in MMA.kerberos_config:
                write_line(p, ref[key])
        else:
            write_line(p, 'N')

        for key in MMA.mc_config:
            write_line(p, ref[key])

        write_line(p, '')
        p.communicate()

    def gen_table_job_config(self, hive_db, hive_table, mc_db, mc_table, pt=None):
        table_mapping_path = os.path.join(
            self.test_temp, f'temp_table_mapping.txt')

        with open(table_mapping_path, 'w') as fd:
            fd.write(f'{hive_db}.{hive_table}:{mc_db}.{mc_table}')

        out, err = execute_command(f'sh {self.job_conf}'
                                   f' --objecttype Table'
                                   f' --tablemapping {table_mapping_path}'
                                   f' --output {self.test_temp}')

        # "generated: xxx-id.json"
        job_config_file = out.split(':')[-1].strip()
        jobid = job_config_file.split('-')[-1][:-5]

        with open(job_config_file) as f:
            c = json.load(f)
        if pt is not None:
            c['mma.filter.partition.begin'] = pt['begin']
            c['mma.filter.partition.end'] = pt['end']
            c['mma.filter.partition.orders'] = pt['orders']
        c['mma.job.id'] = jobid
        with open(job_config_file, 'w') as f:
            json.dump(c, f)

        return jobid, job_config_file

    def gen_catalog_job_config(self, source, dest, jobid):
        execute_command(f'sh {self.job_conf} '
                        f' --objecttype Catalog '
                        f' --sourcecatalog {source} '
                        f' --destcatalog {dest}'
                        f' --output {self.test_temp}'
                        f' --jobid {jobid}')
        return os.path.join(self.test_temp, f'CATALOG-{source}-{dest}-{jobid}.json')

    # ======================== client API =============================
    def submit(self, conf):
        logger.info('Submit job')
        return execute_command(f'sh {self.client} --action SubmitJob --conf {conf}')

    def list(self):
        logger.info('List Jobs')
        return execute_command(f'sh {self.client} --action ListJobs')

    def info(self, jobid):
        logger.info(f'Get job info {jobid}')
        execute_command(f'sh {self.client} --action GetJobInfo --jobid {jobid}')

    def delete(self, jobid):
        logger.info(f'Delete job {jobid}')
        return execute_command(f'sh {self.client} --action DeleteJob --jobid {jobid}')

    def reset(self, jobid):
        logger.info(f'Reset job {jobid}')
        return execute_command(f'sh {self.client} --action ResetJob --jobid {jobid}')

    def stop(self, jobid):
        logger.info(f'Stop job {jobid}')
        return execute_command(f'sh {self.client} --action StopJob --jobid {jobid}')

    def clean(self):
        logger.info('Delete all jobs')
        out, err = self.list()
        pattern = r'Job ID: (.*), status: (.*), progress: (.*)'
        for line in out.strip().split('\n'):
            line = line.strip()
            m = re.match(pattern, line)
            jobid = m.group(1)
            self.delete(jobid)

    def status(self, jobid):
        out, err = self.list()
        job_list = out.split('\n')

        pattern = r'Job ID: (.*), status: (.*), progress: (.*)'
        for job in job_list:
            job = job.strip()
            if jobid in job:
                m = re.match(pattern, job)
                return m.group(2)

    def wait_until(self, jobid, status_list, time_slice=10):
        run = True
        while run:
            status = self.status(jobid)
            if status in status_list:
                run = False
            logger.info(f'Waiting for job {jobid}, current status: {status}')
            time.sleep(time_slice)

    def wait_until_finish(self, jobid, time_slice=10):
        self.wait_until(jobid, ['SUCCEEDED', 'FAILED', 'CANCELED'], time_slice)

    # ======================== Migrate =============================
    def migrate(self, hive_db, hive_table, mc_project, mc_table, pt=None):
        jobid, migration_config_path = self.gen_table_job_config(
            hive_db, hive_table, mc_project, mc_table, pt)
        self.submit(migration_config_path)
        return jobid



def verify_json(conf1: dict, conf2: dict):
    print('======================TEST RESULT===========================')
    equal = True
    if len(conf1.keys()) != len(conf2.keys()):
        equal = False
    for k, v in conf1.items():
        if v != conf2[k]:
            print('Key %s not equal' % k)
            print('ref:', v)
            print('gen:', conf2[k])
            equal = False
    return equal


def verify(mma, hive_db, hive_table,
           mc_project, mc_table,
           hive_where_condition=None,
           mc_where_condition=None):
    def parse_hive_stdout(hive_stdout: str) -> float:
        return float(hive_stdout.strip().split('\n')[0])

    def parse_odps_stdout(odps_stdout: str) -> float:
        return float(odps_stdout.strip().split('\n')[1])

    executor = ThreadPoolExecutor(2)

    sql = 'SELECT AVG(t_smallint) FROM %s.%s %s;'

    if hive_where_condition is None:
        hive_sql = sql % (hive_db, hive_table, '')
    else:
        hive_sql = sql % (hive_db, hive_table, 'WHERE ' + hive_where_condition)
    hive_command = "hive -e '%s'" % hive_sql

    if mc_where_condition is None:
        odps_sql = sql % (mc_project, mc_table, '')
    else:
        odps_sql = sql % (mc_project, mc_table, 'WHERE ' + mc_where_condition)
    odps_sql = 'set odps.sql.allow.fullscan=true; ' + odps_sql
    odps_command = "%s --config=%s -M -e '%s'" % (
        mma.odpscmd, mma.odps_config, odps_sql)

    hive_future = executor.submit(execute_command, hive_command)
    odps_future = executor.submit(execute_command, odps_command)
    hive_stdout, _ = hive_future.result()
    odps_stdout, _ = odps_future.result()
    return parse_hive_stdout(hive_stdout), parse_odps_stdout(odps_stdout)


class Odps:

    def __init__(self, mma):
        self.mma = mma

    def _execute_mc_sql(self, cmd):
        command = f"{self.mma.odpscmd} --config={self.mma.odps_config} -M -e '{cmd}'"
        execute_command(command)

    def count_mc_table(self, pj, table):
        sql = f'SELECT COUNT(*) FROM {pj}.{table};'
        self._execute_mc_sql(sql)

    def drop_table(self, mc_project, mc_table):
        ddl = f'DROP TABLE IF EXISTS {mc_project}.{mc_table}'
        self._execute_mc_sql(ddl)

    def drop_partition(self, mc_project, mc_table, mc_partition_spec):
        dml = f'ALTER TABLE {mc_project}.{mc_table} DROP IF EXISTS PARTITION({mc_partition_spec})'
        self._execute_mc_sql(dml)


class Hive:

    def __init__(self):
        pass

    def _execute_hive_cmd(self, cmd):
        hive_command = f"hive -e '{cmd}'"
        execute_command(hive_command)

    def count_hive_pt(self, db, table, pt):
        # todo
        sql = f'SELECT COUNT(*) FROM {db}.{table}'
        sql = 'select p1, count(*) from test_text_partitioned_10x1k where p1 = "ZhpSC" group by p1;'

    def new_hive_pt(self, db, table, pt):
        dml = f'ALTER TABLE {db}.{table} ADD PARTITION({pt})'
        self._execute_hive_cmd(dml)

    def insert_data_to_pt(self, db, table, pt):
        dml = f'INSERT INTO TABLE {db}.{table} PARTITION({pt}) SELECT extend_table(100)'
        self._execute_hive_cmd(dml)

    def drop_table(self, hive_db, hive_table):
        ddl = f'DROP TABLE IF EXISTS {hive_db}.{hive_table}'
        self._execute_hive_cmd(ddl)

    def drop_partition(self, hive_db, hive_table, hive_partition_spec):
        dml = f'ALTER TABLE {hive_db}.{hive_table} DROP IF EXISTS PARTITION({hive_partition_spec})'
        self._execute_hive_cmd(dml)


hive = Hive()
