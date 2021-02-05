import subprocess
import os
from threading import current_thread
from multiprocessing import current_process
import logging
from pathlib import Path

def gunzip(dir_path):
    logging.info("====== gunzip is being executed on "+dir_path+"/* =========")
    cmd = f"gunzip {dir_path}/*"
    p = subprocess.Popen(
        cmd,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))

def decryption(src_dir_path, decrypt_dir):
    logging.info("====== Decryption is being executed on "+src_dir_path+"/* =========")
    decryption_pwd = 'Firefly54Extreme80Seep'
    cmd = "\
          \pushd {0} \
          \n    rm -rf ./{1}\
          \n    mkdir {1}\
          \n    for filename in *.gpg; do \
          \n        echo \"Working on $filename, generating ./{1}/${3}\"\
          \n        echo \"{2}\" | gpg --batch --yes --passphrase-fd 0 -o ./{1}/${3} -d $filename\
          \n    done\
          \npopd\
                ".format(src_dir_path, decrypt_dir, decryption_pwd, "{filename%.gpg}")
    p = subprocess.Popen(
        cmd,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))
    
def import_gpg_key(key_path=None):
    logging.info("====== Importing gpg key ======")
    if key_path is None:
        file_dir = Path(os.path.abspath(os.path.dirname(__file__)))
        key_path = os.path.join(file_dir, "my-private-key.asc")
    logging.info("Imported gpg key: {}".format(key_path))
    #my-private-key.asc
    cmd = "gpg --import {}".format(key_path)
    p = subprocess.Popen(
        cmd,
        shell=True,
        executable='/bin/bash',
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        universal_newlines=True,
        encoding='utf-8'
    )
    logging.info("stdout:\n {}".format(p.stdout.read()))
    if not p.stdout.read() == "":
        logging.error("stderr:\n {}".format(p.stderr.read()))
    
