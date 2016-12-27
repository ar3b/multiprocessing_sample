#-*- coding: utf-8 -*-#
__author__ = 'enbry'

import sqlite3

class DB(object):

    conn = None
    c = None

    def __init__(self):
        self.conn = sqlite3.connect(
            "./test.sqlite"
        )
        self.conn.row_factory = sqlite3.Row
        self.c = self.conn.cursor()

    def execute(self, query):
        self.c.execute(query)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.c.close()
        self.conn.close()