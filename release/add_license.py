import subprocess
import os.path


def get_ext(path):
    return os.path.splitext(path)[1][1:]


def remote_old_license(begin, end, files):
    # todo support rm old
    # todo rm in xml file
    change_files = []
    for file in files:
        with open(file, 'r') as f:
            lines = f.readlines()
        if lines[1].strip() == begin and lines[16].strip() == end:
            with open(file, 'w') as f:
                change_files.append(f'{str(file)}\n')
                lines = lines[18:]
                # skip empty lines
                while len(lines[0].strip()) == 0:
                    lines = lines[1:]
                f.writelines(lines)
    return change_files


def add_new_license(tmpl_list, files, begin):
    change_files = []
    for file in files:
        with open(file, 'r') as f:
            lines = f.readlines()
            # license exists, skip
            if begin in ''.join(lines[:5]):
                continue
        with open(file, 'w') as f:
            ext = get_ext(file)
            change_files.append(f'{str(file)}\n')
            if ext == 'xml':
                lines = lines[:1] + tmpl_list[ext] + lines[1:]
            else:
                lines = tmpl_list[ext] + lines
            f.writelines(lines)
    return change_files


def get_file_list(ok_ext, ignore_ext, ok_file, ignore_file):
    """
        filter: git-ls -> ignore file -> ok file -> ignore_ext -> ok_ext
        handle .template file by hand
    """
    assert len(set(ok_ext + ignore_ext)) == len(ok_ext) + len(ignore_ext)
    assert len(set(ok_file + ignore_file)) == len(ok_file) + len(ignore_file)

    def file_filter(file):
        ext = get_ext(file)
        if file in ignore_file:
            return False
        elif file in ok_file:
            return True
        elif ext in ignore_ext:
            return False
        elif ext in ok_ext:
            return True
        else:
            raise RuntimeError(f'Check new extension: file {file}')

    p = subprocess.run('git ls-files .', shell=True, stdout=subprocess.PIPE, text=True)
    files = p.stdout.strip().split('\n')
    files = [file for file in files if file_filter(file)]
    return files


def get_tmpl(file_ext, year=2021):
    """
    :return:
        {
            'java': license_lines(list)
            'sql': ...
            ...
        }
    """
    with open('release/LICENSE.tmpl', 'r') as f:
        lines = f.readlines()
        lines[0] = lines[0].replace('${year}', str(year))

    begin, end = lines[0].strip(), lines[-1].strip()

    comment_mapping = {
        '*': ['java', 'sql'],
        '#': ['py', 'yml', 'ini', 'sh', 'txt', 'properties', ''],
        '<': ['xml']
    }

    comment_type2lines = {
        '*': ['/*\n'] + [f' * {line}' for line in lines] + [' */\n', '\n'],
        '#': ['#\n'] + [f'# {line}' for line in lines] + ['#\n'],
        '<': ['<!--', '\n'] + lines + ['-->\n'],
    }

    # for test
    # for lines in comment_type2lines.values():
    #     print(''.join(lines))
    #     print()

    ext2lines = {}
    for comment_type, ext_list in comment_mapping.items():
        for ext in ext_list:
            ext2lines[ext] = comment_type2lines[comment_type]

    for ext in file_ext:
        if ext not in ext2lines.keys():
            raise RuntimeError(f'Unsupported ext: {ext}')

    return ext2lines, begin, end


def log(add_files):
    log_file = 'update_license.log'
    with open(log_file, 'w') as f:
        f.write('[ADD TO]\n')
        f.writelines(add_files)
    print(log_file)


def add_license():
    """
                comment format: /**
                                 * begin
                                 ...
                                 * end
                                 */
            get all files -> modify license -> add log
    :return:
    """
    ok_ext = ['java', 'py', 'xml', 'yml', 'ini', 'sh', 'sql', 'txt', 'properties', '']
    ignore_ext = ['md', 'js', 'css', 'png', 'jar', 'template']
    ok_file = ['.gitignore']
    ignore_file = ['LICENSE']

    ext2tmpl_lines, begin, end = get_tmpl(ok_ext)
    file_list = get_file_list(ok_ext, ignore_ext, ok_file, ignore_file)
    add_files = add_new_license(ext2tmpl_lines, file_list, begin)
    log(add_files)


if __name__ == '__main__':
    add_license()
