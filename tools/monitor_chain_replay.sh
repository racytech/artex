#!/usr/bin/env bash
#
# Monitors chain_replay process memory + fault metrics at fixed intervals.
# Writes CSV to stdout. Column semantics:
#   ts_unix     : wall-clock epoch seconds
#   rss_mb      : resident set
#   swap_mb     : process bytes in swap
#   vmdata_mb   : VmData high-water (growable data)
#   vmsize_mb   : total virtual size
#   minflt      : cumulative minor faults (soft page hits)
#   majflt      : cumulative major faults (disk reads)
#   minflt_ps   : minor faults/sec since last sample
#   majflt_ps   : major faults/sec since last sample
#
# Usage:
#   ./monitor_chain_replay.sh [interval_sec] > metrics.csv
#
# Requires: a single chain_replay process running.

INTERVAL=${1:-30}

PID=$(pgrep -x chain_replay)
if [ -z "$PID" ]; then
    echo "ERROR: no chain_replay process found" >&2
    exit 1
fi
echo "# monitoring pid=$PID every ${INTERVAL}s" >&2

echo "ts_unix,rss_mb,swap_mb,vmdata_mb,vmsize_mb,minflt,majflt,minflt_ps,majflt_ps"

prev_min=0
prev_maj=0
prev_ts=$(date +%s)

while [ -d /proc/$PID ]; do
    ts=$(date +%s)

    rss=$(awk '/^VmRSS:/ {print $2}'   /proc/$PID/status)
    sw=$(awk  '/^VmSwap:/ {print $2}'  /proc/$PID/status)
    vd=$(awk  '/^VmData:/ {print $2}'  /proc/$PID/status)
    vs=$(awk  '/^VmSize:/ {print $2}'  /proc/$PID/status)
    read min maj < <(awk '{print $10, $12}' /proc/$PID/stat)

    dt=$(( ts - prev_ts ))
    if [ $dt -eq 0 ]; then dt=1; fi
    min_ps=$(( (min - prev_min) / dt ))
    maj_ps=$(( (maj - prev_maj) / dt ))

    printf "%d,%.0f,%.0f,%.0f,%.0f,%d,%d,%d,%d\n" \
        $ts $((rss/1024)) $((sw/1024)) $((vd/1024)) $((vs/1024)) \
        $min $maj $min_ps $maj_ps

    prev_min=$min
    prev_maj=$maj
    prev_ts=$ts

    sleep $INTERVAL
done

echo "# process $PID exited" >&2
