# install:
# pip3 install python-gnupg

# note - gpg needs to be installed first:
# brew install gpg
# apt install gpg

# you may need to also:
# export GPG_TTY=$(tty)

import gnupg

gpg = gnupg.GPG()

# generate key
input_data = gpg.gen_key_input(
    name_email='me@email.com',
    passphrase='passphrase',
)
key = gpg.gen_key(input_data)
print(key)

# create ascii-readable versions of pub / private keys
ascii_armored_public_keys = gpg.export_keys(key.fingerprint)
ascii_armored_private_keys = gpg.export_keys(
    keyids=key.fingerprint,
    secret=True,
    passphrase='passphrase',
)

# export
with open('mykeyfile.asc', 'w') as f:
    f.write(ascii_armored_public_keys)
    f.write(ascii_armored_private_keys)

# import
with open('mykeyfile.asc') as f:
    key_data = f.read()
import_result = gpg.import_keys(key_data)

for k in import_result.results:
    print(k)

# encrypt file
with open('plain.txt', 'rb') as f:
    status = gpg.encrypt_file(
        file=f,
        recipients=['me@email.com'],
        output='encrypted.txt.gpg',
    )

print(status.ok)
print(status.status)
print(status.stderr)
print('~'*50)

# decrypt file
with open('encrypted.txt.gpg', 'rb') as f:
    status = gpg.decrypt_file(
        file=f,
        passphrase='passphrase',
        output='decrypted.txt',
    )

print(status.ok)
print(status.status)
print(status.stderr)
