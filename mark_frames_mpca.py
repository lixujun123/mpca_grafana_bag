# -*-coding:utf-8-*-
import os
import json
import time
import pymysql

def read_json(json_file):
    with open(json_file) as json_data:
        data = json.load(json_data)
    return data


# 将tag_name转换为中文
def convert_tag_name(tag_name):
    config_file_path = './config.json'
    config_info = read_json(config_file_path)
    tag_label = ['global_tag', 'local_tag']
    for tag in tag_label:
        tag_info = config_info[tag]
        tags = tag_info['tags']
        for tag_spec in tags:
            if tag_spec['tag_name'] == tag_name:
                return tag_spec['label']


# 获取mpca_data_set下,含有具体日期的文件夹路径
def get_mpca_mark_frames_dict(mpca_dir):
    # 获取包含文件label_mate.json的bag包
    has_mate_dir = []
    for folder in os.listdir(mpca_dir):
        mpca_dir_info = os.path.join(mpca_dir, folder)
        for i in os.listdir(mpca_dir_info):
            merge_folder_path = os.path.join(mpca_dir_info, i)
            mate_file_path = os.path.join(merge_folder_path, 'label_mate.json')
            if os.path.exists(mate_file_path):
                has_mate_dir.append(merge_folder_path)
    # print(has_mate_dir)
    # 获取bag包的日期
    day_dir = []
    for i in has_mate_dir:
        dir_date = os.path.basename(i).split('_')[0]
        if dir_date not in day_dir:
            day_dir.append(dir_date)
    # print(day_dir)
    day_dir_dict = {}
    for i in has_mate_dir:
        dir_date = os.path.basename(i).split('_')[0]
        if dir_date not in day_dir_dict.keys():
            day_dir_dict[dir_date] = [i]
        else:
            day_dir_dict[dir_date].append(i)
    # print(day_dir_dict)
    get_date_mark_frames(day_dir_dict)


# 获取具体日期的抽帧数量
def get_date_mark_frames(day_dir_dict):
    mark_frames = []
    for key, values in day_dir_dict.items():
        day_mark_frames = []
        num_lidar = 0
        for i in values:
            mate_file_path = os.path.join(i, 'label_mate.json')
            lidar = read_json(mate_file_path)
            lidar_total = lidar['total']
            num_lidar += len(lidar_total)
        day_mark_frames.append(key)
        day_mark_frames.append(num_lidar)
        mark_frames.append(day_mark_frames)
    # print("mark_frames: " + str(len(mark_frames)))
    # print(mark_frames)
    insert_mark_frames_mpca(mark_frames)


# 将mpca中lidar的抽帧数量写入数据库表mark_frames_mpca中
def insert_mark_frames_mpca(data_list):
    db = pymysql.connect("localhost", "root", "123", "grafana_bag", charset='utf8')
    cursor = db.cursor()
    try:
        sql1 = "delete from mark_frames_mpca"
        cursor.execute(sql1)
        sum_mark_frames = 0
        for data in data_list:
            sql2 = "insert into mark_frames_mpca (dir_date, frames) values (%s, %d)" % (data[0], data[1])
            cursor.execute(sql2)
            sum_mark_frames += data[1]
        timestamps = time.time()
        sql3 = "insert into mark_frames_mpca_sum (timestamps, sum_frames) values (%d, %d)" % (timestamps, sum_mark_frames)
        cursor.execute(sql3)
        db.commit()
        print("insert into mark_frames_mpca success!")
    except:
        print("connect database fail")
        db.rollback()
    db.close()

