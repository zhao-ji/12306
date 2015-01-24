#!/usr/bin/env python
# coding: utf-8
# @NightWish in Dec.30.2014

import logbook
from pymongo import MongoClient
from redis import StrictRedis

m_12306 = MongoClient(host='127.0.0.1', port=27017)['12306']
m_phone = MongoClient(host='127.0.0.1', port=27017)['phone']
r_db = StrictRedis(host='127.0.0.1', port=6379, db=0)

def count(doc):
    phone_num = doc['phone']
    if len(phone_num) > 12 or not phone_num.isdigit():
        logbook.warn('anomaly phone num: {}'.format(phone_num))
        return
    phone_position = int(phone_num[:7])
    position_doc = m_phone.phone.find_one({'_id':phone_position})
    if not position_doc:
        logbook.warn('can not select the num: {}'.format(phone_num))
        return
    key_list = [
        'province:{}'.format(position_doc['province'].encode('utf8')),
        'city:{}'.format(position_doc['city'].encode('utf8')),]
    msetnx_dict = {item: 0 for item in key_list}
    r_db.msetnx(msetnx_dict)
    incr = lambda key: r_db.incr(key)
    map(incr, key_list)

def save():
    province_keys = r_db.keys('province:*')
    counts = r_db.mget(province_keys)
    for province, count in zip(province_keys, counts):
        m_12306.phone.province.save({
            'province': province.decode('utf8').split(':')[1],
            'count': int(count)})
        r_db.delete(province)
    city_keys = r_db.keys('city:*')
    counts = r_db.mget(city_keys)
    for city, count in zip(city_keys, counts):
        m_12306.phone.city.save({
            'city': city.decode('utf8').split(':')[1],
            'count':int(count)})
        r_db.delete(city)


if __name__ == '__main__':
    cursor = m_12306.user.find()
    # count the position
    map(count, cursor)
    # save the data to mongo
    save()
