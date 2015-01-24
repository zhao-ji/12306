#!/usr/bin/env python
# coding: utf-8
# @NightWish in Dec.31.2014

'''
analyze the password
'''

import logbook
import pymongo
import re
# import redis

MONGO = pymongo.MongoClient(host='127.0.0.1', port=27017)['12306']
# RDS = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)

TOTAL = 131653


def analyze_password_length():
    '''
    analyze the length of password
    '''
    for i in range(6, 17):
        password_compile = re.compile(r'^\S{{{}}}$'.format(i))
        password_doc = {'password': password_compile}
        count = MONGO.user.find(password_doc).count()
        percentage = float(count) / TOTAL
        length_doc = {'_id': i, 'count': count, 'percentage': percentage}
        MONGO.password.len.save(length_doc)
        logbook.info(length_doc)


if __name__ == '__main__':
    analyze_password_length()
