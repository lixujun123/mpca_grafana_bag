# -*-coding:utf-8-*-
from mark_frames_mpca import *
from bag_info_mpca import *
from tag_time_mpca import *
from tag_mark_frames_mpca import *
from tag_worldmap_mpca import *

if __name__ == '__main__':
    config_file_path = './config.json'
    config_info = read_json(config_file_path)
    mpca_dir = config_info['mpca_dir']

    get_mpca_mark_frames_dict(mpca_dir)

    get_mpca_bag_info(mpca_dir)

    get_tag_time(mpca_dir)

    get_tag_mark_frames(mpca_dir)

    get_tag_location_info(mpca_dir)