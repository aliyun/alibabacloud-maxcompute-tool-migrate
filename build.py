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

import os
import subprocess
import shutil
import sys
import traceback


def execute(cmd: str) -> int:
    try:
        sp = subprocess.Popen(cmd, shell=True, preexec_fn=os.setsid)
        sp.wait()
        return sp.returncode
    except Exception as e:
        print(traceback.format_exc())
        return 1


if __name__ == '__main__':
    root = os.path.dirname(os.path.realpath(__file__))
    os.chdir(root)

    # remove existing build directory and package
    if os.path.isdir("mma"):
        shutil.rmtree("mma")
    if os.path.exists("mma.tar.gz"):
        os.unlink("mma.tar.gz")

    # install ding talk dependencies
    local_install_command = (
        "mvn install:install-file "
        "-Dfile=task-scheduler/resource/taobao-sdk-java-auto_1479188381469-20200701.jar "
        "-DgroupId=com.dingtalk "
        "-DartifactId=dingtalk-sdk "
        "-Dversion=1.0 "
        "-Dpackaging=jar"
    )
    ret = execute(local_install_command)
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    # compile
    ret = execute("mvn clean package -DskipTests")
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    # mkdirs
    os.makedirs("mma")
    os.makedirs("mma/lib")
    os.makedirs("mma/conf")
    os.makedirs("mma/res")

    # assemble
    # bin
    shutil.copytree("build/bin", "mma/bin")

    # lib
    task_scheduler = "task-scheduler-1.0-SNAPSHOT.jar"
    shutil.copyfile("task-scheduler/target/" + task_scheduler,
                    "mma/lib/" + task_scheduler)
    udtf = "data-transfer-hive-udtf-1.0-SNAPSHOT-jar-with-dependencies.jar"
    shutil.copyfile("data-transfer-hive-udtf/target/" + udtf,
                    "mma/lib/" + udtf)

    # conf
    shutil.copyfile("build/odps_config.ini.template",
                    "mma/conf/odps_config.ini.template")
    shutil.copyfile("build/oss_config.ini.template",
                    "mma/conf/oss_config.ini.template")
    shutil.copyfile("build/hive_config.ini.template",
                    "mma/conf/hive_config.ini.template")
    shutil.copyfile("build/table_mapping.txt", "mma/conf/table_mapping.txt")

    # res
    shutil.copyfile("build/mma_server_log4j2.xml", 
                    "mma/res/mma_server_log4j2.xml")
    shutil.copyfile("build/mma_client_log4j2.xml", 
                    "mma/res/mma_client_log4j2.xml")

    ret = execute("tar cvpfz mma.tar.gz mma")
    if ret != 0:
        print("Build failed, exit")
        sys.exit(1)

    shutil.rmtree("mma")
    print("Done")
