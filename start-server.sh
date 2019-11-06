# $1 PRO_NUM   $2 MACHINE.CFG    $3 IB.CONF
mpirun -ppn 1 -n $1 -f $2 ./release/server $3
