#!bin/bash

src_dir_path=$1
decrypt_dir=$2

pushd ${src_dir_path}
rm -rf ./decrypt
mkdir decrypt
for filename in *.gpg; do
    echo "Working on $filename, generating ./${decrypt_dir}/${filename%.gpg}"
    echo "Firefly54Extreme80Seep" | gpg --batch --yes --passphrase-fd 0 -o ./${decrypt_dir}/${filename%.gpg} -d $filename
done
popd