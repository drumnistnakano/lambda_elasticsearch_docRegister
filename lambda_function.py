import json
import os
import boto3
import requests
import base64
import urllib
import re
import sys

# S3オブジェクトの取得
s3 = boto3.client('s3')

def read_file(bucket_name, file_key):
    response = s3.get_object(Bucket=bucket_name, Key=file_key)
    return response[u'Body'].read()

def encodeFile(bucket_name, file_key):
    not_enc = read_file(bucket_name, file_key)
    return base64.b64encode(not_enc).decode("ascii")

def trimJson(doc_id, enc_txt):
    # インデッウス：document、タイプ：fileで登録
    jsonList = []
    dict_dat = { 'data' : f'{enc_txt}' }
    dict_index = { "index" : { "_index" : "document", "_type" : "file", "_id" : f'{doc_id}', "pipeline": "attachment" } }
    json_dat = json.dumps(dict_dat)
    json_index = json.dumps(dict_index)

    jsonList.append(json_index)
    jsonList.append(json_dat)
    return '\n'.join(jsonList) + '\n'

def lambda_handler(event, context):
    # バケット, keyの取得
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')

    # バケット内フォルダからドキュメントidを取得
    id_List = re.findall('document/(.*)/.*', key)
    id = id_List[0]

    # base64でエンコーディング
    enc_txt = encodeFile(bucket, key)

    # json形式に整形
    str_json = trimJson(id, enc_txt)

    # request_bulk.jsonにリストの中身を出力
    with open("/tmp/request_bulk.json", mode="w") as f:
        f.write(str_json)

    # POSTリクエストラインの設定
    post_url = os.environ.get('ES_HOST') + "/_bulk?refresh=false"
    headers = { "Content-Type" : "application/x-ndjson"  }
    data = open('/tmp/request_bulk.json', 'r').read()

    # ElasticSearchにPOST発行
    resp_post = requests.post(post_url, data=data, headers=headers)
    print(resp_post.status_code, 'post_code')
    print(resp_post.content, 'post-cont')

    # リフレッシュ
    refresh_url = os.environ.get('ES_HOST') + "/document/_refresh"
    resp_refresh = requests.post(refresh_url)
    print(resp_refresh.status_code, 'refresh-code')
    print(resp_refresh.content, 'refresh-cont')
