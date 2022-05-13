import shutil
from pathlib import Path


def copy_files(src_dir, src_filenames, dest_dir):
    src_dir = Path(src_dir)
    dest_dir = Path(dest_dir)
    for s in src_filenames:
        if len(str(s)) == 0:
            continue
        
        src = Path(s)
        if len(str(src)) > 0:
            if src_dir:
                src_path = src_dir / src
            else:
                src_path = src
            
            src.parent.mkdir(exist_ok=True, parents=True)
            
            dest = dest_dir / src.parent
            dest.mkdir(exist_ok=True, parents=True)
            
            shutil.copy(src_path, dest)
