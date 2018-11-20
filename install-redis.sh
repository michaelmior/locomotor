#!/bin/bash
git clone -b 3.2 https://github.com/antirez/redis vendor/redis
(cd vendor/redis; make && sudo make install)
echo -e '\n\n\n\n\n\n' | sudo ./vendor/redis/utils/install_server.sh
