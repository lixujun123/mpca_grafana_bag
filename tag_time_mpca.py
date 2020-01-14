# -*-coding:utf-8-*-
import os
import time
import MySQLdb
from mark_frames_mpca import read_json, convert_tag_name


# 获取mpca_data_set目录下，包含tag_meta.json文件的目录中该json文件的路径
def get_tag_meta_file_path(mpca_dir):
    tag_meta_path = []
    for folder in os.listdir(mpca_dir):
        mpca_dir_info = os.path.join(mpca_dir, folder)
        for i in os.listdir(mpca_dir_info):
            merge_folder_path = os.path.join(mpca_dir_info, i)
            tag_meta_file_path = os.path.join(merge_folder_path, 'tag_meta.json')
            if os.path.exists(tag_meta_file_path):
                tag_meta_path.append(tag_meta_file_path)
    return tag_meta_path


# 获取tag_meta_path中，所有tag的时长
def get_tag_time(mpca_dir):
    tag_meta_path = get_tag_meta_file_path(mpca_dir)
    tag_info = {}
    for i in tag_meta_path:
        tag_info_mpca = read_json(i)
        tag_meta_info = get_tag_time_info(tag_info_mpca)
        # print(tag_meta_info)
        if len(tag_info) > 0:
            tag_name = tag_info.keys()
            for tag in tag_meta_info.keys():
                if tag in tag_name:
                    tag_info[tag] = int(tag_info[tag]) + int(tag_meta_info[tag])
                else:
                    tag_info[tag] = tag_meta_info[tag]
        else:
            tag_info = tag_meta_info
    insert_tag_info_mpca(tag_info)
    # print(tag_info)


# 获取单个json文件的内容
def get_tag_time_info(tag_info_mpca):
    tag_info = {}
    for tag in tag_info_mpca.keys():
        tag_info_dict = tag_info_mpca[tag]
        if len(tag_info_dict) > 0:
            for i in tag_info_dict.keys():
                tag_specific_info = tag_info_dict[i]
                if len(tag_specific_info) > 0:
                    tag_time = 0
                    for tag_spec in tag_specific_info:
                        tag_time += (float(tag_spec['stop_time']) - float(tag_spec['start_time']))
                else:
                    continue
                tag_info[i] = int(tag_time)
    return tag_info


# 将tag_info的内容存入表tag_time_mpca中
def insert_tag_info_mpca(tag_info):
    db = MySQLdb.connect("localhost", "root", "123", "grafana_bag", charset='utf8')
    cursor = db.cursor()
    # print(tag_info)
    try:
        sql1 = "delete from tag_time_mpca"
        cursor.execute(sql1)
        timestamps = time.time()
        for keys, values in tag_info.items():
            values /= 60
            name = convert_tag_name(keys).encode("utf-8")
            sql2 = "insert into tag_time_mpca (timestamps, tag_name, tag_time) values (%d, %s, %d)" \
                   % (timestamps, '"{}"'.format(name), values)
            cursor.execute(sql2)
        db.commit()
        print("insert into tag_time_mpca success!")
    except:
        db.rollback()
        print("Error: unable to insert into tag_time_mpca")
    db.close()






