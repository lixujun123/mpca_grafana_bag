# -*-coding:utf-8-*-
import time
import os
import MySQLdb
from mark_frames_mpca import get_mpca_mark_frames_dict


# 获取mpca_data_set目录下，bag包的数量和时长
def get_mpca_bag_info(mpca_dir):
    bag_info_mpca = []
    bag_num = 0
    bag_time = 0
    for folder in os.listdir(mpca_dir):
        mpca_dir_info = os.path.join(mpca_dir, folder)
        for i in os.listdir(mpca_dir_info):
            merge_folder_path = os.path.join(mpca_dir_info, i)
            bag_num += 1
            bag_time += int(os.path.basename(merge_folder_path).split('_')[-1])
    bag_info_mpca.append(bag_num)
    bag_info_mpca.append(bag_time)
    # print(bag_info_mpca)
    insert_bag_info_mpca(bag_info_mpca)


# 将mpca_data_set目录下，bag包的数量和时长写入表bag_info_mpca中
def insert_bag_info_mpca(bag_info_list):
    db = MySQLdb.connect("localhost", "root", "123", "grafana_bag", charset='utf8')
    cursor = db.cursor()
    try:
        sql1 = "select bag_num, bag_time from bag_info_mpca"
        cursor.execute(sql1)
        results = cursor.fetchall()
        new_bag_num = bag_info_list[0]
        new_bag_time = int(bag_info_list[1]) / 3600
        if len(results) > 0:
            last_bag_num = results[-1][0]
            last_bag_time = results[-1][1]
        else:
            last_bag_num = new_bag_num
            last_bag_time = new_bag_time
        timestamps = time.time()
        if new_bag_num != last_bag_num or new_bag_time != last_bag_time:
            sql2 = "insert into bag_info_mpca values (%d, %d, %d)" % (timestamps, new_bag_num, new_bag_time)
            cursor.execute(sql2)
        else:
            sql3 = "insert into bag_info_mpca values (%d, %d, %d)" % (timestamps, last_bag_num, last_bag_time)
            cursor.execute(sql3)
        db.commit()
        print("insert into bag_info_mpca success!")
    except:
        print("Error: unable to insert into bag_info_mpca")
        db.rollback()
    db.close()



