# -*-coding:utf-8-*-
import os
import time
import MySQLdb
from mark_frames_mpca import read_json, convert_tag_name
from tag_time_mpca import get_tag_meta_file_path


# 获取tag_meta.json文件中的经纬度信息
def get_tag_location_info(mpca_dir):
    tag_meta_path = get_tag_meta_file_path(mpca_dir)
    tag_info = []
    for file in tag_meta_path:
        # print(file)
        tag_meta_info = read_json(file)
        if len(tag_meta_info['local_tag']) > 0:
            local_tag = tag_meta_info['local_tag']
            for tag in local_tag.keys():
                local_tag_info = local_tag[tag]
                if len(local_tag_info) > 0:
                    # print("tag: " + tag)
                    for tag_spec in local_tag_info:
                        if 'latitude' in tag_spec.keys():
                            tag_location_info = [tag, tag_spec['latitude'], tag_spec['longitude']]
                            tag_info.append(tag_location_info)
    # print(tag_info)
    # print(len(tag_info))
    # print(len(tag_meta_path))
    insert_tag_woldmap_mpca(tag_info)
    return tag_info


# 将tag location信息放入表tag_worldmap_mpca中
def insert_tag_woldmap_mpca(tag_info):
    db = MySQLdb.connect("localhost", "root", "123", "grafana_bag", charset='utf8')
    cursor = db.cursor()
    try:
        sql1 = "delete from tag_worldmap_mpca"
        cursor.execute(sql1)
        timestamps = time.time()
        for tag in tag_info:
            # print(tag[0])
            # name = (convert_tag_name(tag[0])).encode("utf-8")
            # print(name)
            sql2 = "insert into tag_worldmap_mpca values (%s, %s, %s, %d, %d)" \
                   % ('"{}"'.format(tag[1]), '"{}"'.format(tag[2]), '"{}"'.format(tag[0]), 1, timestamps)
            cursor.execute(sql2)
        db.commit()
        print("insert into tag_worldmap_mpca success!")
    except:
        db.rollback()
        print("Error: unable to insert into tag_worldmap_mpca")
    db.close()