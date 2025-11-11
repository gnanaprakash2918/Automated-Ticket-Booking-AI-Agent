import os

def print_structure(path, indent=0, ignore_folders=None):
    if ignore_folders is None:
        ignore_folders = []
    files = sorted(os.listdir(path))
    for file in files:
        full_path = os.path.join(path, file)
        if file in ignore_folders and os.path.isdir(full_path):
            continue
        prefix = '│   ' * indent + '├── '
        print(prefix + file)
        if os.path.isdir(full_path):
            print_structure(full_path, indent + 1, ignore_folders)

print_structure('D:\\TNSTC', ignore_folders=['.venv', '.git'])
