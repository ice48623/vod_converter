#!/usr/bin/env python3
from pymongo import MongoClient
import os
import subprocess
import threading
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
LOG.basicConfig(
    level=LOG.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

# resolution 360, 720, 1048
RESOLUTIONS = {360: '640:360', 720: '1280:720', 1080: '1920:1080'}

def get_input_output_location(video_id, filename, resolution):
    fname, extension = os.path.splitext(filename)

    video_folder = f'./videos/{video_id}'
    input_file = f'{video_folder}/{filename}'

    output_file = f'{video_folder}/{fname}_{resolution}{extension}'
    return input_file, output_file

def convert_video_resolution(video_id, filename, resolution):

    input_file, output_file = get_input_output_location(video_id, filename, resolution)

    pixel_dimension = RESOLUTIONS.get(resolution)
    LOG.info(f'Converting {output_file}')
    success = subprocess.call(['ffmpeg', '-i', input_file, '-vf', f'scale={pixel_dimension}', output_file])
    return success == 0

def update_video_resolution_in_db(video_id, filename, resolution):
    _, output_file = get_input_output_location(video_id, filename, resolution)
    LOG.info(f'Updating resolution in mongo')

    db_resolution = {
        'resolution': resolution,
        'location': output_file
    }
    collection.find_one_and_update(
        {'video_id': video_id},
        {'$push': {'resolutions': db_resolution}}
    )

def run_convert_task(data):
    video_id = data.get('video_id')
    filename = data.get('filename')
    resolution = data.get('resolution')

    update_video_resolution_in_db(video_id, filename, resolution)
    convert_video_resolution(video_id, filename, resolution)

def callback(ch, method, properties, body):
    data = json.loads(body)
    run_convert_task(data)
    LOG.info(f'Finish converting video')

def callback_thread(ch, method, properties, body):
    thread = threading.Thread(target=callback, args=(ch, method, properties, body))
    thread.start()

if __name__ == '__main__':
    rabbit = Rabbit('convert')
    rabbit.consume(callback_thread)

    LOG.info(' [*] Waiting for Job.')
    rabbit.start_consuming()