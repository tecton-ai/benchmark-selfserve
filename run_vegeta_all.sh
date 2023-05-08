#!/bin/sh

#
# Use this as a "workspace" to loadtest multiple services as the same time,
# or just as a reference to paste line(s) into your console.
#

./run_vegeta.py --service fs_mixed_5_fv --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_mixed_10_fv --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_mixed_18_fv --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_nonagg_1_fv --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_nonagg_2_fv --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_nonagg_4_fv --file -r 5 -d 10 -t 5000 &

