#!/usr/bin/env python3
from pymongo import MongoClient
import os
import subprocess
import pika
import json
import logging
from rabbit import Rabbit


MONGO_URL = os.getenv('MONGO_URL', 'localhost')
MONGO_PORT = int(os.getenv('MONGO_PORT', 27017))
MONGO_DB = os.getenv('MONGO_DB', 'my_db')
MONGO_COLLECTION = os.getenv('MONGO_COLLECTION', 'my_collection')
mongoClient = MongoClient(f'mongodb://{MONGO_URL}', MONGO_PORT)
db = mongoClient[MONGO_DB]
collection = db[MONGO_COLLECTION]

LOG = logging
LOG.getLogger('pika').setLevel(LOG.INFO)
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# resolution 360, 720, 1048
RESOLUTIONS = {360: '640:360', 720: '1280:720', 1080: '1920:1080'}
BASE_VIDEOS_FOLDER = os.getenv('BASE_VIDEOS_FOLDER', './videos')
BASE_VIDEO_URL = os.getenv('BASE_VIDEO_URL', 'http://localhost:4000')


def get_input_output_location(video_id, filename, resolution):
    fname, extension = os.path.splitext(filename)

    video_folder = f'{BASE_VIDEOS_FOLDER}/{video_id}'
    input_file = f'{video_folder}/{filename}'

    output_name = f'{fname}_{resolution}{extension}'
    output_file = f'{video_folder}/{output_name}'
    return input_file, output_file, output_name

def convert_video_resolution(video_id, filename, resolution):
    input_file, output_file, _ = get_input_output_location(video_id, filename, resolution)

    pixel_dimension = RESOLUTIONS.get(resolution)
    LOG.info(f'Converting {output_file}')

    success = subprocess.call(['./convert.sh', input_file, pixel_dimension, output_file])
    return success == 0

def update_video_resolution_in_db(video_id, filename, resolution):
    LOG.info(f'Updating resolution in mongo')
    _, _, output_name = get_input_output_location(video_id, filename, resolution)

    db_resolution = {
        'src': f'{BASE_VIDEO_URL}/hls/{video_id}/{output_name},.urlset/master.m3u8',
        'type': 'application/x-mpegURL',
        'label': str(resolution),
        'res': resolution
    }
    collection.find_one_and_update(
        {'video_id': video_id},
        {'$push': {'source': db_resolution}}
    )

def run_convert_task(data):
    video_id = data.get('video_id')
    filename = data.get('filename')
    resolution = data.get('resolution')

    convert_success = convert_video_resolution(video_id, filename, resolution)
    if (convert_success):
        update_video_resolution_in_db(video_id, filename, resolution)
    else:
        LOG.error(f'Error converting {filename} with {resolution} resolution')

def callback(ch, method, properties, body):
    data = json.loads(body)
    run_convert_task(data)
    LOG.info(f'Finish converting video')

if __name__ == '__main__':
    rabbit = Rabbit('convert')
    rabbit.consume(callback)

    LOG.info(' [*] Waiting for Job.')
    rabbit.start_consuming()
