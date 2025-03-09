#!bin/bash
cd ~ && mkdir postgres_source && cd postgres_source
wget https://ftp.postgresql.org/pub/source/v12.1/postgresql-12.1.tar.gz
tar xvzf postgresql-12.1.tar.gz
cd postgresql-12.1/
./configure --prefix=$HOME/postgres --with-python PYTHON=/usr/bin/python3
make world -j $(cat /proc/cpuinfo | grep processor | wc -l)
make install-world
echo 'export PATH="$HOME/postgres/bin:$PATH"' >> ~/.bash_profile && source ~/.bash_profile
