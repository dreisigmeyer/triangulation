#!/bin/bash

# Files to process
A1_FILE="closed_paths_A1.csv"
A2_FILE="closed_paths_A2.csv"
A3_FILE="closed_paths_A3.csv"

# This is for the potential A1 matches: firmid and EIN match
awk -F',' -v OFS=',' '
    function abs(v) {return v < 0 ? -v : v}
    {print $6,$2,$5,abs($8 - $4),$8,abs($9 - $3),$9,$13,$4,$15,$16,$17,$18,$19,$20}' \
    $A1_FILE \
    | sort -u -t',' -T ./ -k2,8 -k1,1 \
    | awk -F',' -v OFS=',' '{print $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15}' \
    | uniq -c \
    | sed -e 's/^[ ]*\([0-9]*\) /\1,/' \
    | sort -t',' -T ./ -k2,2 -k3,7n -k1,1nr \
    > a1_sorted_grouped_counted.csv

# This is for the potential A2 matches: firmid matches - EIN does not match
awk -F',' -v OFS=',' '
    function abs(v) {return v < 0 ? -v : v}
    {print $6,$2,$5,abs($8 - $4),$8,abs($9 - $3),$9,$13,$4,$15,$16,$17,$18,$19,$20}' \
    $A2_FILE \
    | sort -u -t',' -T ./ -k2,8 -k1,1 \
    | awk -F',' -v OFS=',' '{print $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15}' \
    | uniq -c \
    | sed -e 's/^[ ]*\([0-9]*\) /\1,/' \
    | sort -t',' -T ./ -k2,2 -k3,7n -k1,1nr \
    > a2_sorted_grouped_counted.csv

# This is for the potential A3 matches: EIN matches - firmid does not match
awk -F',' -v OFS=',' '
    function abs(v) {return v < 0 ? -v : v}
    {print $6,$2,$5,abs($8 - $4),$8,abs($9 - $3),$9,$13,$4,$15,$16,$17,$18,$19,$20}' \
    $A3_FILE \
    | sort -u -t',' -T ./ -k2,8 -k1,1 \
    | awk -F',' -v OFS=',' '{print $2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13,$14,$15}' \
    | uniq -c \
    | sed -e 's/^[ ]*\([0-9]*\) /\1,/' \
    | sort -t',' -T ./ -k2,2 -k3,7n -k1,1nr \
    > a3_sorted_grouped_counted.csv
