import subprocess
import sys
from github import Github


def run(cmd):
    p = subprocess.run(cmd, shell=True, stdout=subprocess.PIPE)
    p.check_returncode()
    print(p.stdout)


def uncomment_file(files_list, tag_name):
    begin = f'<!-- {tag_name} begin -->'
    end = f'<!-- {tag_name} end -->'
    for file in files_list:
        with open(file, 'r') as f:
            lines = f.readlines()
        uncomment = False
        for i, line in enumerate(lines):
            if begin in line:
                uncomment = True
                continue
            elif end in line:
                uncomment = False
                continue

            if uncomment:
                if '&lt;!&ndash;' in line or '<!--' not in line:
                    continue
                lines[i] = lines[i].replace('<!--', '')
                lines[i] = lines[i].replace('-->', '')

        with open(file, 'w') as f:
            f.writelines(lines)


def package_branch(branch, version):
    """
        checkout -> uncomment sql checker -> mvn package -> rename zip -> reset sql checker file
    """
    hive_version = branch.split('/')[1]
    print(f'Build release branch origin/release/{branch}')
    run(f'git checkout --quiet {branch}')
    uncomment_file(sql_checker_files, 'sql checker')
    run(f'mvn -U -q clean package -DskipTests')
    output_file = f'mma-{version}-{hive_version}.zip'
    run(f'mv distribution/target/mma-{version}.zip {output_file}')
    for file in sql_checker_files:
        run(f'git restore {file}')
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
            release.upload_asset(file)


def release_mma(branches, version):
    release_packages = []
    for i, branch in enumerate(branches):
        release_packages.append(package_branch(branch, version))
    release_to_github(release_packages, f'v{version}')


if __name__ == '__main__':
    sql_checker_files = ['pom.xml', 'distribution/pom.xml',
                         'distribution/src/assembly/assembly.xml']
    BRANCHES = ['release/hive-1.x', 'release/hive-2.x', 'release/hive-3.x']
    version = sys.argv[1]
    release_mma(BRANCHES, version)
