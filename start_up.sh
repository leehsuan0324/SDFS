#/bin/bash
rm -rf /tmp/
mkdir /tmp/
cp ./test/MP1/machine.${MACHINE_NUM}.log /tmp/machine.log
./start_fileserver.sh &
./start_logserver.sh
