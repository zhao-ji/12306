#! /usr/bin/env python
# -*- utf-8 -*-

from argparse import ArgumentParser

WEIGHT = [7,9,10,5,8,4,2,1,6,3,7,9,10,5,8,4,2]

def check(card):
    cal_tuple = zip(WEIGHT, card[:-1])
    cal = map(
        lambda item: item[0] * int(item[1]), 
        cal_tuple)
    print (12 - sum(cal) % 11) % 11


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument('card', help='please input you card ID')
    args = parser.parse_args()
    print len(args.card)
    check(args.card)
