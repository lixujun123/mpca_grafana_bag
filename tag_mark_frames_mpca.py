# -*-coding:utf-8-*-
import os
import time
import pymysql
from mark_frames_mpca import read_json, convert_tag_name


# 获取mpca_data_set目录下，同时包含tag_meta.json和label_mate.json的文件夹目录
def get_tag_mark_frames(mpca_dir):
    both_folder = []
    for folder in os.listdir(mpca_dir):
        mpca_dir_info = os.path.join(mpca_dir, folder)
        for i in os.listdir(mpca_dir_info):
            merge_folder_path = os.path.join(mpca_dir_info, i)
            mate_file_path = os.path.join(merge_folder_path, "label_mate.json")
            meta_file_path = os.path.join(merge_folder_path, "tag_meta.json")
            if os.path.exists(mate_file_path) and os.path.exists(meta_file_path):
                both_folder.append(merge_folder_path)
    get_tag_frames(both_folder)


# 获取both_folder中，所有目录下各种tag的送标帧数
def get_tag_frames(both_folder):
    tag_frames = {}
    for folder in both_folder:
        mate_file_path = os.path.join(folder, "label_mate.json")
        meta_file_path = os.path.join(folder, "tag_meta.json")
        camera = read_json(mate_file_path)
        camera_time = camera["total"]
        tag_info = read_json(meta_file_path)
        tag_frames_info = get_tag_frames_info(tag_info, camera_time)
        # print(tag_frames_info)
        if len(tag_frames) > 0:
            for tag in tag_frames_info.keys():
                if tag in tag_frames.keys():
                    tag_frames[tag] = int(tag_frames_info[tag]) + int(tag_frames[tag])
                else:
                    tag_frames[tag] = tag_frames_info[tag]
        else:
            tag_frames = tag_frames_info
    # print(tag_frames)
    insert_tag_mark_frames(tag_frames)


# 获取tag_meta.json中时间戳所在的tag
def get_tag_frames_info(tag_info, camera_time):
    tag_frames = {}
    for tag in tag_info.keys():
        tag_info_dict = tag_info[tag]
        if len(tag_info_dict) > 0:
            for i in tag_info_dict.keys():
                # print(i)
                tag_specific_info = tag_info_dict[i]
                if len(tag_specific_info) > 0:
                    for tag_spec in tag_specific_info:
                        start_time = tag_spec['start_time']
                        stop_time = tag_spec['stop_time']
                        for times in camera_time:
                            if float(start_time) < float(times) < float(stop_time):
                                if i in tag_frames.keys():
                                    tag_frames[i] += 1
                                else:
                                    tag_frames[i] = 1
                # print(tag_frames)
    return tag_frames


# 将各种tag的送标帧数，放入表tag_mark_frames_mpca中
def insert_tag_mark_frames(tag_frames):
    db = pymysql.connect("localhost", "root", "123", "grafana_bag", charset='utf8')
    cursor = db.cursor()
    # print(tag_info)
    try:
        sql1 = "delete from tag_mark_frames_mpca"
        cursor.execute(sql1)
        timestamps = time.time()
        for keys, values in tag_frames.items():
            name = convert_tag_name(keys)
            sql2 = "insert into tag_mark_frames_mpca (timestamps, tag_name, mark_frames) values (%d, %s, %d)" \
                   % (timestamps, '"{}"'.format(name), values)
            cursor.execute(sql2)
        db.commit()
        print("insert into tag_mark_frames_mpca success!")
    except:
        db.rollback()
        print("Error: unable to insert into tag_mark_frames_mpca")
    db.close()
