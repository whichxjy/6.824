#!/usr/bin/env bash

#
# basic map-reduce test
#

RACE=-race
BUILD_PLUGIN=-buildmode=plugin

WORK_DIR=$(cd "$(dirname "$0")" || exit; pwd)
INPUT_DIR=$WORK_DIR/input
TEMP_DIR=$WORK_DIR/mr-tmp
BIN_DIR=$TEMP_DIR/bin
PLUGINS_DIR=$TEMP_DIR/plugins
APPS_DIR=$TEMP_DIR/../../mrapps

# run the test in a fresh sub-directory.
rm -rf "$TEMP_DIR"
mkdir "$TEMP_DIR" || exit 1
cd "$TEMP_DIR" || exit 1

SEQUENTIAL=$BIN_DIR/mrsequential
COORDINATOR=$BIN_DIR/mrcoordinator
WORKER=$BIN_DIR/worker

(cd .. && go build $RACE -o "$SEQUENTIAL" mrsequential/mrsequential.go) || exit 1
(cd .. && go build $RACE -o "$COORDINATOR" mrcoordinator/mrcoordinator.go) || exit 1
(cd .. && go build $RACE -o "$WORKER" mrworker/mrworker.go) || exit 1

WC=$PLUGINS_DIR/wc
INDEXER=$PLUGINS_DIR/indexer
MTIMING=$PLUGINS_DIR/mtiming
RTIMING=$PLUGINS_DIR/rtiming
JOBCOUNT=$PLUGINS_DIR/jobcount
EARLY_EXIT=$PLUGINS_DIR/early_exit
CRASH=$PLUGINS_DIR/crash
NOCRASH=$PLUGINS_DIR/nocrash

(cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$WC" wc/wc.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$INDEXER" indexer/indexer.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$MTIMING" mtiming/mtiming.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$RTIMING" rtiming/rtiming.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$JOBCOUNT" jobcount/jobcount.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$EARLY_EXIT" early_exit/early_exit.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$CRASH" crash/crash.go) || exit 1
# (cd "$APPS_DIR" && go build $RACE $BUILD_PLUGIN -o "$NOCRASH" nocrash/nocrash.go) || exit 1

failed_any=0

#########################################################
# first word-count

# generate the correct output
$SEQUENTIAL "$WC" "$INPUT_DIR"/* || exit 1
sort mr-out-0 > mr-correct-wc.txt
rm -f mr-out*

echo '***' Starting wc test.

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &
pid=$!

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s "$WORKER" "$WC" &
timeout -k 2s 180s "$WORKER" "$WC" &
timeout -k 2s 180s "$WORKER" "$WC" &

# wait for the coordinator to exit.
wait $pid

# TODO: Remove this line
exit

# since workers are required to exit when a job is completely finished,
# and not before, that means the job has finished.
sort mr-out* | grep . > mr-wc-all
if cmp mr-wc-all mr-correct-wc.txt
then
  echo '---' wc test: PASS
else
  echo '---' wc output is not the same as mr-correct-wc.txt
  echo '---' wc test: FAIL
  failed_any=1
fi

# wait for remaining workers and coordinator to exit.
wait

#########################################################
# now indexer
rm -f mr-*

# generate the correct output
$SEQUENTIAL "$INDEXER" "$INPUT_DIR"/* || exit 1
sort mr-out-0 > mr-correct-indexer.txt
rm -f mr-out*

echo '***' Starting indexer test.

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &
sleep 1

# start multiple workers
timeout -k 2s 180s "$WORKER" "$INDEXER" &
timeout -k 2s 180s "$WORKER" "$INDEXER"

sort mr-out* | grep . > mr-indexer-all
if cmp mr-indexer-all mr-correct-indexer.txt
then
  echo '---' indexer test: PASS
else
  echo '---' indexer output is not the same as mr-correct-indexer.txt
  echo '---' indexer test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting map parallelism test.

rm -f mr-*

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &
sleep 1

timeout -k 2s 180s "$WORKER" "$MTIMING" &
timeout -k 2s 180s "$WORKER" "$MTIMING"

NT=$(cat mr-out* | grep -c '^times-' | sed 's/ //g')
if [ "$NT" != "2" ]
then
  echo '---' saw "$NT" workers rather than 2
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

if cat mr-out* | grep '^parallel.* 2' > /dev/null
then
  echo '---' map parallelism test: PASS
else
  echo '---' map workers did not run in parallel
  echo '---' map parallelism test: FAIL
  failed_any=1
fi

wait

#########################################################
echo '***' Starting reduce parallelism test.

rm -f mr-*

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &
sleep 1

timeout -k 2s 180s "$WORKER" "$RTIMING" &
timeout -k 2s 180s "$WORKER" "$RTIMING"

NT=$(cat mr-out* | grep -c '^[a-z] 2' | sed 's/ //g')
if [ "$NT" -lt "2" ]
then
  echo '---' too few parallel reduces.
  echo '---' reduce parallelism test: FAIL
  failed_any=1
else
  echo '---' reduce parallelism test: PASS
fi

wait

#########################################################
echo '***' Starting job count test.

rm -f mr-*

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &
sleep 1

timeout -k 2s 180s "$WORKER" "$JOBCOUNT" &
timeout -k 2s 180s "$WORKER" "$JOBCOUNT"
timeout -k 2s 180s "$WORKER" "$JOBCOUNT" &
timeout -k 2s 180s "$WORKER" "$JOBCOUNT"

NT=$(cat mr-out* | awk '{print $2}')
if [ "$NT" -ne "8" ]
then
  echo '---' map jobs ran incorrect number of times "($NT != 8)"
  echo '---' job count test: FAIL
  failed_any=1
else
  echo '---' job count test: PASS
fi

wait

#########################################################
# test whether any worker or coordinator exits before the
# task has completed (i.e., all output files have been finalized)
rm -f mr-*

echo '***' Starting early exit test.

timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* &

# give the coordinator time to create the sockets.
sleep 1

# start multiple workers.
timeout -k 2s 180s "$WORKER" "$EARLY_EXIT" &
timeout -k 2s 180s "$WORKER" "$EARLY_EXIT" &
timeout -k 2s 180s "$WORKER" "$EARLY_EXIT" &

# wait for any of the coord or workers to exit
# `jobs` ensures that any completed old processes from other tests
# are not waited upon
jobs &> /dev/null
wait -n

# a process has exited. this means that the output should be finalized
# otherwise, either a worker or the coordinator exited early
sort mr-out* | grep . > mr-wc-all-initial

# wait for remaining workers and coordinator to exit.
wait

# compare initial and final outputs
sort mr-out* | grep . > mr-wc-all-final
if cmp mr-wc-all-final mr-wc-all-initial
then
  echo '---' early exit test: PASS
else
  echo '---' output changed after first worker exited
  echo '---' early exit test: FAIL
  failed_any=1
fi
rm -f mr-*

#########################################################
echo '***' Starting crash test.

# generate the correct output
$SEQUENTIAL "$NOCRASH" "$INPUT_DIR"/* || exit 1
sort mr-out-0 > mr-correct-crash.txt
rm -f mr-out*

rm -f mr-done
(timeout -k 2s 180s "$COORDINATOR" "$INPUT_DIR"/* ; touch mr-done ) &
sleep 1

# start multiple workers
timeout -k 2s 180s "$WORKER" "$CRASH" &

# mimic rpc.go's coordinatorSock()
SOCKNAME=/var/tmp/824-mr-$(id -u)

( while [ -e "$SOCKNAME" ] && [ ! -f mr-done ]
  do
    timeout -k 2s 180s "$WORKER" "$CRASH"
    sleep 1
  done ) &

( while [ -e "$SOCKNAME" ] && [ ! -f mr-done ]
  do
    timeout -k 2s 180s "$WORKER" "$CRASH"
    sleep 1
  done ) &

while [ -e "$SOCKNAME" ] && [ ! -f mr-done ]
do
  timeout -k 2s 180s "$WORKER" "$CRASH"
  sleep 1
done

wait

rm "$SOCKNAME"
sort mr-out* | grep . > mr-crash-all
if cmp mr-crash-all mr-correct-crash.txt
then
  echo '---' crash test: PASS
else
  echo '---' crash output is not the same as mr-correct-crash.txt
  echo '---' crash test: FAIL
  failed_any=1
fi

#########################################################
if [ $failed_any -eq 0 ]; then
    echo '***' PASSED ALL TESTS
else
    echo '***' FAILED SOME TESTS
    exit 1
fi
