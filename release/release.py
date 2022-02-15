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
import subprocess
import sys
from datetime import datetime
from os import mkdir
from github import Github


def run(cmd):
    print(f'Run cmd {cmd}')
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
    p.check_returncode()
    print(f'Output: {p.stdout}')
    print(f'Error: {p.stderr}')


def package_hive(hive_version: int):
    print(f'Build release for hive{hive_version}')
    run(f'mvn -U -q clean package -DskipTests -Dhive={hive_version}')
    output_file = f'{TARGET_DIR}/mma-{MMA_VERSION}-{hive_version}.x.zip'
    run(f'mv distribution/target/mma-{MMA_VERSION}.zip {output_file}')
    print('Done')
    return output_file


def release_to_github(files, tag):
    """
        check release [tag] exists?
            - exist -> delete old -> update new package
            - not exist -> create release -> upload package
    """
    with open('release/.GITHUB_TOKEN', 'r') as f:
        token = f.read()
    g = Github(token)
    repo = g.get_repo('aliyun/alibabacloud-maxcompute-tool-migrate')

    release = repo.get_releases()[0]
    if release.tag_name == tag:

        print(f'Release {tag} exists, update assets...')

        # delete old package
        assets = release.get_assets()
        for asset in assets:
            if asset.name in files:
                print(f'Delete package {asset.name}')
                asset.delete_asset()

        # update new package
        for file in files:
            print(f'Updating {file}')
            release.upload_asset(file)
    else:
        print(f'Create release {tag}...')
        release = repo.create_git_release(tag=tag, name=tag, prerelease=True, message='')
        for file in files:
            print(f'Updating {file}')
            release.upload_asset(file)


def release_mma():
    run('git checkout master')
    run('git pull')
    release_packages = [package_hive(1), package_hive(2), package_hive(3)]
    release_to_github(release_packages, f'v{MMA_VERSION}')


if __name__ == '__main__':
    sql_checker_files = ['pom.xml', 'distribution/pom.xml',
                         'distribution/src/assembly/assembly.xml']
    MMA_VERSION = sys.argv[1]
    TARGET_DIR = f'tmp/{datetime.now().strftime("%Y_%m_%d_%H_%M_%S")}'
    mkdir(TARGET_DIR)
    release_mma()
