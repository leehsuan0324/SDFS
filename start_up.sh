#/bin/bash
cp ./test/MP1/machine.${MACHINE_NUM}.log /tmp/machine.log
go run ./cmd/server/*.go $MACHINE_NUM