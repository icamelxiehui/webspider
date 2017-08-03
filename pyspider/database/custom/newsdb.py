#!/usr/bin/python
# -*- coding: utf-8 -*-
"""
File Name: newsdb.py
Author: xiehui
mail: 372623335@qq.com
Created Time: 五  7/28 11:10:21 2017
"""

import pymongo
import sqlite3
import os


class NewsMongoDb(object):
    def __init__(self, table_name):
        server = "localhost"
        port = 27017
        db_name = "big_data"

        con = pymongo.MongoClient(server, port)
        db = con[db_name]
        self.collection = db[table_name]	

    def save(self, result):
        self.collection.update(
                {'url':result['url']},{"$set":result},upsert=True
                )

class NewsSqliteDb(object):
    def __init__(self):
        dbpath = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.realpath(os.path.dirname(__file__))))))) + "/Web/SearchEngine/news.db"
        print dbpath
        self.conn = sqlite3.connect(dbpath)
        self.cur = self.conn.cursor()

    def save(self, result):
        sql_query = "REPLACE INTO engine_news ({0}) VALUES ({1})".format (', '.join(result.keys()),
                                                                ', '.join(['?'] * len(result.keys())))
        self.cur.execute(sql_query, result.values())
        self.conn.commit()

class NewsDb(object):

    def __init__(self, table_name):
        self.news_mongo = NewsMongoDb(table_name)
        #self.news_sqlite = NewsSqliteDb()

    def process(self, result):
        self.news_mongo.save(result)
        #self.news_sqlite.save(result)

if __name__ == "__main__":
    news_db = NewsDb()
    news_db.process({"url":"www","image":"kk", "title":"","content":"","pubtime":"", "author":"", "description":"", "website":""})
