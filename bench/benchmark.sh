#!/bin/sh

mime="application/x-www-form-urlencoded"
svc_url="http://gp07.datalab.noao.edu:6998/spec/getSpec"

/bin/rm -f _out _n _t

declare -a C=(1 2 4 8 16 32 64 128)
declare -a N=(1 5 10 15 20 25 30 35 40 45 50 60 70 80 90 100)
 
# Iterate the string array using for loop

for c in ${C[@]}; do
  echo "Concurrent requests: " $c
  for n in ${N[@]}; do
    if [ $n -ge $c ]
    then
       echo $n		>> _n
       ab -c $c -n $n -p post.args -T $mime $svc_url	 >> _out
    else
       echo $n		>> _n
       echo ' '		>> _t
       echo ' '		>> _rps
       echo ' '		>> _tr
       echo ' '		>> _trp
    fi
  done

  egrep Time\ taken _out | cut -f 7 --delim=' ' >> _t
  egrep Requests\ per\ sec _out | cut -f 7 --delim=' ' >> _rps
  egrep Transfer\ rate _out | cut -f 12 --delim=' ' >> _tr
  egrep Time\ per\ req _out | egrep concurrent | cut -f 10 --delim=' ' >> _trp

  paste _n _t  |& tee _c.$c
  paste _n _rps  |& tee _crps.$c
  paste _n _tr  |& tee _ctr.$c
  paste _n _trp  |& tee _ctrp.$c
  /bin/rm -f _t _n _rps _tr _trp _out
done

echo 'Time Taken:'
paste _c.1 _c.2 _c.4 _c.8 _c.16 _c.32 _c.64 _c.128 | \
     cut -f 1,2,4,6,8,10,12,14,16

echo ' ' 
echo 'Time per Request:'
paste _ctrp.1 _ctrp.2 _ctrp.4 _ctrp.8 _ctrp.16 _ctrp.32 _ctrp.64 _ctrp.128 | \
     cut -f 1,2,4,6,8,10,12,14,16

echo ' ' 
echo 'Requests per Second:'
paste _crps.1 _crps.2 _crps.4 _crps.8 _crps.16 _crps.32 _crps.64 _crps.128 | \
     cut -f 1,2,4,6,8,10,12,14,16

echo ' ' 
echo 'Transfer Rate:'
paste _ctr.1 _ctr.2 _ctr.4 _ctr.8 _ctr.16 _ctr.32 _ctr.64 _ctr.128 | \
     cut -f 1,2,4,6,8,10,12,14,16
