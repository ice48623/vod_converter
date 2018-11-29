#!/bin/sh

INPUT_FILE=$1
PIXEL_DIMENSION=$2
OUTPUT_FILE=$3

ffmpeg -i $INPUT_FILE -vf scale=$PIXEL_DIMENSION -c:v libx264 -preset ultrafast $OUTPUT_FILE