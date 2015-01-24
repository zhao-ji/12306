#!/usr/bin/env python
# coding: utf-8
# @NightWish in Dec.30.2014

'''
analyze the suffix of mail
'''

import logbook
import pymongo
import redis

MONGO = pymongo.MongoClient(host='127.0.0.1', port=27017)['12306']
RDS = redis.StrictRedis(host='127.0.0.1', port=6379, db=0)


def process(doc):
    '''
    count the suffix of mail
    '''
    mail = doc['mail_0']
    if '@' not in mail:
        logbook.error(mail)
        return
    suffix = mail.split('@')[1]
    RDS.setnx(suffix, 0)
    RDS.incr(suffix)
    logbook.info('process: {}'.format(mail))

def save():
    '''
    save the result in redis
    '''
    random_suffix = RDS.randomkey()
    while random_suffix:
        count = int(RDS.get(random_suffix))
        doc = {'_id':random_suffix, 'count':count}
        MONGO.mail.suffix.save(doc)
        RDS.delete(random_suffix)
        random_suffix = RDS.randomkey()


if __name__ == '__main__':
    CURSOR = MONGO.user.find({}, {'mail_0':1})
    map(process, CURSOR)
    save()
