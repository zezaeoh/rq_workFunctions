# -*- coding: utf-8 -*-
from __future__ import (absolute_import, division, print_function,
                        unicode_literals)
from datetime import datetime
from botocore.exceptions import ClientError
from koshort.stream import naver
from collections import OrderedDict

import os
import boto3
import sys
import urllib3
import json
import time
import pandas as pd
import re
import pymysql.cursors
import settings


openApiURL = None
accessKeys = None
accessKey = None
analysisCode = None
http = None
mem_morp_table_list = None
mem_dictionary = None
mem_preview_content_list = None
mem_today_chart = None
n_keyword = None
conn = None


def dynamo_pipe_line(item, table_name):
    connection = boto3.resource('dynamodb')
    table = connection.Table(table_name)
    store = connection.Table('indexes')
    status = 200
    cur_time = datetime.now()
    index = 1
    if os.path.isfile('/var/log/%s.log' % table_name):
        with open('/var/log/%s.log' % table_name, mode='rt', encoding='utf-8') as f:
            s = f.read()
            if s:
                d = s.split()
                cur_time = datetime.strptime(d[0], "%Y%m%d%H%M%S")
                index = int(d[1])
    else:
        try:
            tr = store.get_item(
                Key={
                    'table': table_name
                }
            )
        except ClientError as _e:
            print(_e.response['Error']['Message'], file=sys.stderr)
        else:
            if 'Item' in tr:
                index = int(tr['Item']['index'])
    item['r_id'] = index
    index += 1
    table.put_item(Item=item)
    if (datetime.now() - cur_time).seconds > 60:
        store.put_item(
            Item={
                'table': table_name,
                'index': index
            }
        )
        status = 500
        cur_time = datetime.now()
    with open('/var/log/%s.log' % table_name, mode='wt', encoding='utf-8') as f:
        f.write(' '.join([cur_time.strftime("%Y%m%d%H%M%S"), str(index)]))
    return index-1, status


def process_413(text, _tp, request_json, responses):
    text1 = text[:len(text) // 2]
    text2 = text[len(text) // 2:]
    request_json['argument']['text'] = text1
    response1 = http.request(
        "POST",
        openApiURL,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        body=json.dumps(request_json)
    )
    if '413' in str(response1.status):
        process_413(text1, _tp, request_json, responses)
    elif '429' in str(response1.status):
        while '429' in str(response1.status):
            time.sleep(3)
            response1 = http.request(
                "POST",
                openApiURL,
                headers={"Content-Type": "application/json; charset=UTF-8"},
                body=json.dumps(request_json)
            )
        responses.append(response1)
    else:
        responses.append(response1)
    request_json['argument']['text'] = text2
    response2 = http.request(
        "POST",
        openApiURL,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        body=json.dumps(request_json)
    )
    if '413' in str(response2.status):
        process_413(text2, _tp, request_json, responses)
    elif '429' in str(response2.status):
        while '429' in str(response2.status):
            time.sleep(3)
            response2 = http.request(
                "POST",
                openApiURL,
                headers={"Content-Type": "application/json; charset=UTF-8"},
                body=json.dumps(request_json)
            )
        responses.append(response2)
    else:
        responses.append(response2)


def morp_process(text, _tp, is_counting=False):
    if not text.strip():
        return {}

    morphemes_list = []
    morphemes_cnt = {}
    global accessKey

    request_json = {
        "access_key": accessKey,
        "argument": {
            "text": text,
            "analysis_code": analysisCode
        }
    }
    response = http.request(
        "POST",
        openApiURL,
        headers={"Content-Type": "application/json; charset=UTF-8"},
        body=json.dumps(request_json)
    )
    responses = []

    if '200' not in str(response.status):
        if '413' in str(response.status):
            process_413(text, _tp, request_json, responses)
        elif '429' in str(response.status):
            time.sleep(3)
            return morp_process(text, _tp, is_counting)
        else:
            return {}
    else:
        responses.append(response)

    for rp in responses:
        data2 = pd.read_json(str(rp.data, "utf-8"), typ='series')
        if data2['result'] == -1:
            if not accessKeys:
                global flag
                flag = True
                print(data2['reason'], file=sys.stderr)
                return {'end': 1}
            else:
                print(data2['reason'], file=sys.stderr)
                accessKey = accessKeys.pop(0)
                return morp_process(text, _tp, is_counting)
        return_object = data2['return_object']
        sentences = return_object['sentence']

        for sentence in sentences:
            # 형태소 문석기 결과 수집 및 정렬
            morphological_analysis_result = sentence['morp']
            for morphemeInfo in morphological_analysis_result:
                if 'lemma' not in morphemeInfo or 'type' not in morphemeInfo:
                    continue
                morphemes_list.append([morphemeInfo['lemma'], morphemeInfo['type']])
                if is_counting and (morphemeInfo['type'] == 'NNG' or morphemeInfo['type'] == 'NNP'):
                    if morphemeInfo['lemma'] in morphemes_cnt:
                        morphemes_cnt[morphemeInfo['lemma']]['cnt'] += 1
                    else:
                        morphemes_cnt[morphemeInfo['lemma']] = {'type': morphemeInfo['type'], 'cnt': 1}
    _data = {
        'morp': morphemes_list
    }
    if is_counting:
        tmp_list = [[a, morphemes_cnt[a]['type'], morphemes_cnt[a]['cnt']] for a in morphemes_cnt]
        tmp_list.sort(reverse=True, key=lambda t: t[2])
        _data['count'] = tmp_list
    return _data


def add_preview(_item, _index, _cnt_list):
    global mem_preview_content_list
    i_list = [_index]
    if 'title' in _item:
        i_list.append(_item['title'])
    else:
        i_list.append(None)
    if int(_item['media']) == 100:
        if re.findall(r'\d+/\d+/\d+ \d+:\d+', _item['date']):
            i_list.append(re.sub(r'(\d+)/(\d+)/(\d+) (\d+):(\d+)', r'\1-\2-\3 \4:\5', _item['date']))
        else:
            i_list.append(_item['date'])
    else:
        i_list.append(_item['date'])
    if 'writer' not in _item:
        i_list.append('익명')
    else:
        i_list.append(_item['writer'])
    i_list.append(int(_item['media']))
    if 'content' in _item:
        _item['content'] = _item['content'].strip()
        if len(_item['content']) > 30:
            i_list.append(_item['content'][:30] + '...')
        else:
            i_list.append(_item['content'])
    else:
        i_list.append(None)
    i_list.append(_item['url'])
    for i in range(8):
        if i < len(_cnt_list):
            i_list.append(_cnt_list[i][0])
            i_list.append(_cnt_list[i][2])
        else:
            i_list.append(None)
            i_list.append(0)
    mem_preview_content_list.append(i_list)


def get_dictionary_from_mysql():
    global conn, mem_dictionary
    with conn.cursor() as cursor:
        sql = "SELECT * FROM dumy_dictionary"
        cursor.execute(sql)
        for row in cursor.fetchall():
            if row[1] not in mem_dictionary:
                mem_dictionary[row[1]] = {row[2]: [row[0], row[3], row[4]]}
            elif row[2] not in mem_dictionary[row[1]]:
                mem_dictionary[row[1]][row[2]] = [row[0], row[3], row[4]]


def add_new_dictionary_item(morp, m_type, senti_score, spam_score):
    global conn, mem_dictionary
    with conn.cursor() as cursor:
        sql = 'INSERT INTO dumy_dictionary (morp, m_type, senti_score, spam_score) VALUES (%s, %s, %s, %s)'
        cursor.execute(sql, (morp, m_type, senti_score, spam_score))
        if morp in mem_dictionary:
            mem_dictionary[morp][m_type] = [cursor.lastrowid, senti_score, spam_score]
        else:
            mem_dictionary[morp] = {m_type: [cursor.lastrowid, senti_score, spam_score]}
        return cursor.lastrowid


def get_mem_dictionary_d_id(_item):
    global mem_dictionary
    if _item[0] in mem_dictionary and _item[1] in mem_dictionary[_item[0]]:
        return mem_dictionary[_item[0]][_item[1]][0]
    return add_new_dictionary_item(_item[0], _item[1], 0, 0)


def add_morp_in_mem_morp_table(_index, morp_list, _media, _tp):
    global mem_morp_table_list, n_keyword, mem_today_chart
    for a in morp_list:
        for filter_key in n_keyword:
            if a[0] == filter_key[1]:
                if _tp == 0:
                    mem_today_chart[_media][filter_key[0]]['c_cnt'] += 1
                else:
                    mem_today_chart[_media][filter_key[0]]['t_cnt'] += 1
                if not mem_today_chart[_media][filter_key[0]]['r_id']:
                    mem_today_chart[_media][filter_key[0]]['r_id'] = _index
                break
        d_id = get_mem_dictionary_d_id(a)
        mem_morp_table_list.append([_index, _tp, d_id])


def add_preview_to_mysql():
    global conn, mem_preview_content_list
    with conn.cursor() as cursor:
        sql = '''
        INSERT IGNORE INTO
        test_content (r_id, title, date, writer, media, content, url, morp1, cnt1, morp2, cnt2, morp3, cnt3, morp4, cnt4, morp5, cnt5, morp6, cnt6, morp7, cnt7, morp8, cnt8)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        '''
        for i_list in mem_preview_content_list:
            cursor.execute(sql, i_list)


def add_morp_table_to_mysql():
    global conn, mem_morp_table_list
    with conn.cursor() as cursor:
        sql = 'INSERT INTO test_morp_table (r_id, ctype, d_id) VALUES (%s, %s, %s)'
        for i_list in mem_morp_table_list:
            cursor.execute(sql, i_list)


def add_today_chart_to_mysql():
    global conn, mem_today_chart
    with conn.cursor() as cursor:
        sql = 'INSERT INTO today_chart (n_id, media, t_cnt, c_cnt, cycle, r_id) VALUES (%s, %s, %s, %s, %s, %s)'
        for media in mem_today_chart:
            for n_id in mem_today_chart[media]:
                cursor.execute(sql, (n_id, media, mem_today_chart[media][n_id]['t_cnt'],
                                     mem_today_chart[media][n_id]['c_cnt'], mem_today_chart[media][n_id]['cycle'],
                                     mem_today_chart[media][n_id]['r_id']))


def get_curr_n_keyword():
    global conn
    tmp_list = []
    re_list = []
    date = datetime.now().strftime('%Y-%m-%d')
    index, key = naver.get_current_trend()
    for i, a in zip(index, key):
        tmp_list.append([int(i), a])
        if int(i) >= 5:
            break
    with conn.cursor() as cursor:
        for a in tmp_list:
            data = morp_process(a[1], 3)
            if 'morp' in data:
                for morp in data['morp']:
                    sql = 'INSERT INTO n_keyword (n_keyword, n_order, date) VALUES (%s, %s, %s)'
                    cursor.execute(sql, (morp[0], a[0], date))
                    re_list.append([cursor.lastrowid, morp, a[0], date])
            elif 'end' in data:
                return None
            else:
                return None
    return re_list


def get_saved_n_keyword():
    global conn
    re_list = []
    date = None
    with conn.cursor() as cursor:
        sql = 'SELECT * FROM n_keyword ORDER BY n_id DESC'
        cursor.execute(sql)
        for row in cursor.fetchall():
            if not date:
                date = row[3]
            elif date != row[3]:
                break
            re_list.append(row)
    return re_list


def init_mem_today_chart(_cycle):
    global mem_today_chart, n_keyword
    media_list = [100, 200, 300, 400]
    mem_today_chart = OrderedDict()
    for com in media_list:
        mem_today_chart[com] = OrderedDict()
        for filter_key in n_keyword:
            mem_today_chart[com][filter_key[0]] = {'t_cnt': 0, 'c_cnt': 0, 'cycle': _cycle, 'r_id': None}


def process_main(table_name, cycle, is_first=False):
    connection = boto3.resource('dynamodb')
    table = connection.Table(table_name)
    indexes = connection.Table('indexes')
    index = 1
    end_index = 1
    status = 200

    try:
        tr = indexes.get_item(
            Key={
                'table': 'processing'
            }
        )
    except ClientError as _e:
        print(_e.response['Error']['Message'], file=sys.stderr)
    else:
        if 'Item' in tr:
            index = int(tr['Item']['index'])

    if os.path.isfile('/var/log/%s.log' % table_name):
        with open('/var/log/%s.log' % table_name, mode='rt', encoding='utf-8') as f:
            s = f.read()
            if s:
                d = s.split()
                end_index = int(d[1])
    else:
        try:
            tr = indexes.get_item(
                Key={
                    'table': table_name
                }
            )
        except ClientError as _e:
            print(_e.response['Error']['Message'], file=sys.stderr)
        else:
            if 'Item' in tr:
                end_index = int(tr['Item']['index'])

    if index == end_index:
        print("No items to process!", file=sys.stderr)
        return 909

    global openApiURL, accessKeys, accessKey, analysisCode, http, mem_morp_table_list,\
        mem_dictionary, mem_preview_content_list, mem_today_chart, n_keyword, conn

    openApiURL = "http://aiopen.etri.re.kr:8000/WiseNLU"
    accessKeys = settings.ACCESSKEYS
    accessKey = accessKeys.pop(0)
    analysisCode = "ner"
    http = urllib3.PoolManager()

    host = settings.HOST
    user = settings.USER
    password = settings.PASSWORD
    db = settings.DB
    charset = 'utf8mb4'

    mem_morp_table_list = []
    mem_dictionary = {}
    mem_preview_content_list = []

    try:
        conn = pymysql.connect(
            host=host,
            user=user,
            password=password,
            db=db,
            charset=charset
        )
    except Exception as e:
        print(str(e), file=sys.stderr)
        print("ERROR: Unexpected error: Could not connect to MySql instance.", file=sys.stderr)
        status = 404
        return status

    if is_first:
        n_keyword = get_curr_n_keyword()
    else:
        n_keyword = get_saved_n_keyword()
    if not n_keyword:
        print('loading current naver keyword error!', file=sys.stderr)
        return 808
    init_mem_today_chart(cycle)
    get_dictionary_from_mysql()

    try:
        while True:
            try:
                tr = table.get_item(
                    Key={
                        'r_id': index
                    }
                )
            except ClientError as e:
                print(e.response['Error']['Message'], file=sys.stderr)
                status = 707
                return status
            else:
                if 'Item' in tr:
                    item = tr['Item']
                    media = int(item['media'])
                    tp_cont = 0
                    tp_title = 1
                    cnt_list = []
                    cont_data = None
                    title_data = None
                    if 'content' in item:
                        data = morp_process(item['content'], tp_cont, is_counting=True)
                        if 'morp' in data:
                            cont_data = data['morp']
                        elif 'end' in data:
                            break
                        if 'count' in data:
                            cnt_list = data['count']
                    if 'title' in item:
                        data = morp_process(item['title'], tp_title)
                        if 'morp' in data:
                            title_data = data['morp']
                    add_preview(item, index, cnt_list)
                    if cont_data:
                        add_morp_in_mem_morp_table(index, cont_data, media, tp_cont)
                    if title_data:
                        add_morp_in_mem_morp_table(index, title_data, media, tp_title)
                index += 1
                if index >= end_index:
                    print('ending condition met!', file=sys.stderr)
                    break
    except urllib3.exceptions.MaxRetryError:
        print('There was a problem with the API server!', file=sys.stderr)
        status = 505
    except Exception as e:
        print('I got an unexpected error!', file=sys.stderr)
        print(str(e), file=sys.stderr)
        status = 606
    finally:
        add_preview_to_mysql()
        add_morp_table_to_mysql()
        add_today_chart_to_mysql()
        conn.commit()
        conn.close()
        indexes.put_item(
            Item={
                'table': 'processing',
                'index': index
            }
        )
        print('indexing over', file=sys.stderr)
        return status
