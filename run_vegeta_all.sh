#!/bin/sh

#
# Use this as a "workspace" to loadtest multiple services as the same time,
# or just as a reference to paste line(s) into your console.
#

./run_vegeta.py --service fs_mixed_5_feature_views --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_mixed_10_feature_views --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_mixed_18_feature_views --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_non_aggregate_1_feature_views --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_non_aggregate_2_feature_views --file -r 5 -d 10 -t 5000 &
./run_vegeta.py --service fs_non_aggregate_4_feature_views --file -r 5 -d 10 -t 5000 &

