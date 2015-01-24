#!/usr/bin/env python
# coding: utf-8

import logbook
from pymongo import MongoClient

m = MongoClient(host='127.0.0.1', port=27017)['12306']

def save(line):
    '''
    save the code-position to mongo
    '''

    # format the fileline
    info = line.strip('\r\n').strip(' ').split(' ')
    info[0] = int(info[0])
    info[1] = info[1].decode('gb2312').encode('utf8')
    if len(info) > 2:
        logbook.warn(info)

    # save the info to mongo
    try:
        doc = {'_id': info[0], 'position':info[1]}
        ret = m.position.save(doc)
    except StandardError, error_info:
        logbook.error(error_info)
        logbook.warn('code:{}, position:{}, original:'.format(
            info[0], info[1], info,
            ))
    else:
        logbook.info(ret)
    finally:
        del line, doc

if __name__ == '__main__':
    with open('/home/nightwish/Downloads/card.txt') as text:
        for line in text:
            save(line)
    logbook.info('import over')
