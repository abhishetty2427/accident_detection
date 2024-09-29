#!/usr/bin/env python
from dbconnection import DBConnection
import time
import cv2
import os
class Buffer:
    def add_to_buffer(self):
        count = 0
        db = DBConnection()
        conn = db.get_connection()
        mycursor = conn.cursor()
        mycursor.execute("SELECT path FROM vidbuffer")
        vid_buffer_items = mycursor.fetchall()
        for row in vid_buffer_items:
            should_continue = 1
            path = row[0]
            vidcap = cv2.VideoCapture(path)
            success,image = vidcap.read()
            while success :
                img_path = "../buffer/frame"+str(count)+".jpg"
                cv2.imwrite(img_path, image)
                db1 = DBConnection()
                conn1 = db1.get_connection()
                mycursor1 = conn1.cursor()
                mycursor1.execute("INSERT INTO `buffer`(`path`) VALUES (%s);", [img_path])
                conn1.commit()
                count = count + 1
                for _ in range(25):
                    success,image = vidcap.read()
                mycursor1.execute("SELECT continue_buffer FROM smbool where flag_var = 0")
                row1 = mycursor1.fetchone()
                should_continue = row1[0]
                print("should continue",should_continue)
                mycursor1.execute("UPDATE smbool set continue_buffer = 1 where flag_var = 0")
                conn1.commit()
                if should_continue is 0:
                    break
            mycursor1.execute("DELETE FROM `vidbuffer` WHERE `path` = %s",[path])
            conn1.commit()

