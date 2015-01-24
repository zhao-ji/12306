#!/usr/bin/env python
# coding: utf-8
# @NightWish in Dec.29.2014

import logbook
from pymongo import MongoClient
from redis import StrictRedis

m_db = MongoClient(host='127.0.0.1', port=27017)['12306']
r_db = StrictRedis(host='127.0.0.1', port=6379, db=0)


def analysis(doc):
    '''
    analysis the user info:
        birth position
        birth time
        man or famale
        mail
    '''
    # analysis the ID card
    # get the use-ful info
    card = doc['card']
    if len(card) is 18:
        position_num = [card[:2], card[:4], card[:6]]
        year, month = card[6:10], card[10:12]
        gender = card[16]
    elif len(card) is 15:
        position_num = [card[:2], card[:4], card[:6]]
        year, month = card[6:8], card[8:10]
        gender = card[14]
    # convert to use-ful info
    try:
        position_code = [int('{:0<6}'.format(item))
                         for item in position_num]
        year, month = int(year), int(month)
        gender = int(gender) % 2
    except StandardError, error_info:
        logbook.error('error:{}, card:{}'.format(error_info, card))
        return
    # prepare doc
    key_list = []
    key_list.extend(position_code)
    key_list.append('year:{}'.format(year))
    key_list.append('month:{}'.format(month))
    key_list.append('female') \
        if gender is 0 \
        else key_list.append('male')
    msetnx_dict = {item: 0 for item in key_list}
    # count
    r_db.msetnx(msetnx_dict)
    incr = lambda key: r_db.incr(key)
    map(incr, key_list)


def save_position(level):
    # dump the all province data into mongo

    # get all position key
    if level is 'province':
        positions = r_db.keys('??0000')
    elif level is 'city':
        positions = r_db.keys('????00')

    # produce every position
    for position in positions:
        # get the count of position
        count = r_db.get(position)
        # get the position info
        position_info = m_db.position.find_one({'_id': int(position)})
        if not position_info:
            position_info = {
                '_id': int(position), 'position': '未知'.decode('utf8')}

        # create the mongo doc
        position_info.update({'count': int(count)})
        # save the doc
        # clean the data
        if level is 'province':
            ret = m_db.card.province.save(position_info)
            r_db.move(position, 2)
        elif level is 'city':
            parent_pos = int(position) / 10000 * 10000
            parent_info = m_db.position.find_one({'_id': parent_pos})
            position_info['position'] = '{parent} {self}'.format(
                parent=parent_info['position'].encode('utf8'),
                self=position_info['position'].encode('utf8'),)
            ret = m_db.card.city.save(position_info)
            r_db.move(position, 3)
        logbook.info(ret)


def save_birth(level):
    # select the all birth info
    if level is 'year':
        birth = r_db.keys('year:*')
    elif level is 'month':
        birth = r_db.keys('month:*')
    # save the birth data
    for item in birth:
        # get the count of birth
        count = int(r_db.get(item))
        # save the data
        _id = int(item.split(':')[1])
        if level is 'year':
            m_db.birth.year.save({'_id': _id, 'count': count})
        elif level is 'month':
            m_db.birth.month.save({'_id': _id, 'count': count})
        r_db.delete(item)


def save_gender():
    male_count = r_db.get('male')
    ret = m_db.gender.save({'_id': 'male', 'count': male_count})
    logbook.info(ret)
    female_count = r_db.get('female') or 0
    ret = m_db.gender.save({'_id': 'female', 'count': female_count})
    logbook.info(ret)
    r_db.delete('male')
    r_db.delete('female')


def save_county():
    while r_db.randomkey:
        key = r_db.randomkey()
        if not key.isdigit():
            logbook.warn(key)
            continue
        count = int(r_db.get(key))
        parent_id = int(key) / 100 * 100
        parent = m_db.card.city.find_one({'_id': parent_id})
        if not parent:
            parent = {
                '_id': int(parent_id), 'position': '未知'.decode('utf8')}
        self_doc = m_db.position.find_one({'_id': key})
        if not self_doc:
            self_doc = {
                '_id': int(key), 'position': '未知'.decode('utf8')}
        self_doc['count'] = count
        self_doc['position'] = '{parent} {self}'.format(
            parent=parent['position'].encode('utf8'),
            self=self_doc['position'].encode('utf8'),)
        ret = m_db.card.county.save(self_doc)
        logbook.info(ret)
        r_db.delete(key)


if __name__ == '__main__':
    cursor = m_db.user.find()
    map(analysis, cursor)
    logbook.info('start process province')
    save_position('province')
    logbook.info('start process city')
    save_position('city')
    save_birth('month')
    save_birth('year')
    save_gender()
    logbook.info('start process county')
    save_county()
