#!/usr/bin/python3
import csv
import pandas as pd
import MySQLdb
import mojimoji
import jaconv
import datetime
import numpy
from phpserialize import loads

# DataFrame型で取得
csvData = pd.read_csv('./src/file/fm_sample.csv')

# 全件表示
# print(csvData)

# カラム名表示
print(csvData.columns.values)

# キー名表示
print(csvData.index.values)

# 範囲指定して表示
# print(csvData[0:3])

# 行数取得
print(csvData.shape[0])

# DataFrameのループ
# for columnName in csvData:
#     print(type(columnName))
#     print(columnName)
#     print('======\n')

# DataFrameのループ　全列データ取得
# for columnName, item in csvData.iteritems():
#     print(type(columnName))
#     print(columnName)
#     print('~~~~~~')
#
#     print(type(item))
#     print(item)
#     print('------')
#
#     print('======\n')

# print('---')
# output = pd.DataFrame({'aa': [], 'bb': []})
# output.loc[1] = ['aa1', 'bb1']
# print(output)
# print('---')


# DataFrameのループ　全行データ取得
# for index, row in csvData.iterrows():
    # print(type(index))
    # print(index)
    # print('-----')
    #
    # print(type(row))
    # print(row)
    # print('------')
    # print(row['PCメール'])
    # print(row[2]) # PCメール
    # output[index] =


# 接続する
con = MySQLdb.connect(user='root', passwd='mysql', host='mysql', db='db_olive', charset='utf8')
# カーソルを取得する
cur= con.cursor()

# クエリを実行する
sql_baby_ages = "select id, name from baby_ages"
cur.execute(sql_baby_ages)
# 実行結果をすべて取得する
result_baby_ages = cur.fetchall()
print(result_baby_ages)
print(type(result_baby_ages)) # <class 'tuple'>
print(result_baby_ages[1][0]) # id
print(result_baby_ages[1][1]) # name
print(type(result_baby_ages[1][0])) # <class 'int'>
print(type(result_baby_ages[1][1])) # <class 'str'>

dict_baby_ages = {}
# ループ
for id, name in result_baby_ages:
    # print(id, name)
    # 名前をキーにしてidを代入する
    dict_baby_ages[name] = id

# print(dict)
# print(dict['6ヶ月'])
# print(type(dict['6ヶ月'])) # <class 'int'>

# 生後をidに変換
def convert_baby_ages(age):
    key = ''
    if str(age).find('歳') != -1:
        # 最初の2文字を切り取る
        key = str(age)[:2]
    elif str(age).find('ヶ月') != -1:
        # 歳が入ってない場合はそのまま使う
        key = str(age)

    if key != '':
        return dict_baby_ages[key]
    else:
        return ''


# 列でデータを扱う
output = pd.DataFrame()
# DataFrameのループ　全列データ取得
for columnName, item in csvData.iteritems():
    if columnName == '〒':
        output['zip_code'] = item
    # elif columnName == 'DM配信':
    #     output['can_receive_mail'] = list(map(lambda x: True if x == '希望' else False, item))
    elif columnName == 'PCメール':
        output['pc_mail'] = item
    elif columnName == 'P顧客ID':
        # Kを消した数値をIDにする
        output['id'] = list(map(lambda x: str(x).strip("K"), item))
    elif columnName == 'Web検索':
        output['searchd_by'] = item
    elif columnName == 'お子様':
        # 「人」を除外して半角数字にする
        output['children_count'] = list(map(lambda x: mojimoji.zen_to_han(str(x).strip("人")), item))
    elif columnName == 'カルテNo':
        output['card_number'] = item
    elif columnName == 'サイズ':
        # TODO:IDに変換する必要あり？
        output['size_id'] = item
    elif columnName == 'サンキューレター':
        output['is_receive_thank_you_letter'] = list(map(lambda x: False if x == '未送付' else True, item))
    elif columnName == 'ふりがな姓':
        # カタカナへ変換
        output['first_kana'] = list(map(lambda x: jaconv.hira2kata(str(x)), item))
    elif columnName == 'ふりがな名':
        # カタカナへ変換
        output['last_kana'] = list(map(lambda x: jaconv.hira2kata(str(x)), item))
    elif columnName == 'メール配信':
        output['can_receive_mail'] = list(map(lambda x: True if x == '希望' else False, item))
    elif columnName == 'メモ':
        output['comment'] = item
    elif columnName == '携帯メール':
        output['phone_mail'] = item
    elif columnName == '携帯電話':
        output['tel'] = item
    elif columnName == '住所（建物名）':
        # あとで連結ようで使う
        output['building'] = item
    elif columnName == '個性心理学':
        # TODO:idにする？
        output['zoomancy_id'] = item
    elif columnName == '固定電話':
        output['fixed_line_tel'] = item
    elif columnName == '最寄駅':
        # TODO:IDに変換する必要あり
        output['nearest_station_id'] = item
    elif columnName == '住所（市区町村）':
        output['city'] = item
    # elif columnName == '氏名':
        # output['first_name'] = item
    elif columnName == '初来院日':
        output['first_visited_at'] = item
    elif columnName == '職業':
        # TODO:IDに変換する必要あり？
        output['occupation_id'] = item
    elif columnName == '診察券受渡':
        output['has_registration_card'] = list(map(lambda x: True if x == '済' else False, item))
    elif columnName == '姓':
        output['first_name'] = item
    elif columnName == '名':
        output['last_name'] = item
    elif columnName == '性別':
        output['gender'] = item
    elif columnName == '生後':
        # TODO:idにする？
        output['baby_age_id'] = list(map(convert_baby_ages, item))
    elif columnName == '生年月日':
        output['birthday'] = item
    elif columnName == '知人の紹介':
        output['introducer'] = item
    elif columnName == '都道府県':
        output['prefecture'] = item
    elif columnName == '年齢':
        output['age'] = item
    elif columnName == '住所（番地）':
        output['address'] = item
    elif columnName == '来店経緯':
        # TODO:idにする？
        output['visit_reason_id'] = item

#     print(type(columnName))
    # print(columnName)
    # print(item)
#     print('~~~~~~')
#
#     print(type(item))
#     print(item)
#     print('------')
#
#     print('======\n')

# 住所を整形してセットする
output['address'] = output['address'].str.cat(output['building'], sep=' ')
output = output.drop(columns='building')

# 共通の値をセット
output['first_visit_store_id'] = ''
output['last_visit_store_id'] = ''
output['last_visited_at'] = ''
output['created_at'] = ''
output['updated_at'] = ''
output['email'] = ''
output['encrypted_password'] = ''
output['reset_password_token'] = ''
output['reset_password_sent_at'] = ''
output['remember_created_at'] = ''
output['provider'] = ''
output['uid'] = ''
output['tokens'] = ''
output['allow_password_change'] = ''

# customersテーブル順に並べ替え
# output.loc[:,['first_name',
output = output[[
              'id',
              'first_name',
              'last_name',
              'first_kana',
              'last_kana',
              'tel',
              'fixed_line_tel',
              'pc_mail',
              'phone_mail',
              'can_receive_mail',
              'birthday',
              'zip_code',
              'prefecture',
              'city',
              'address',
              'comment',
              'first_visit_store_id',
              'last_visit_store_id',
              'first_visited_at',
              'last_visited_at',
              'card_number',
              'introducer',
              'searchd_by',
              'has_registration_card',
              'children_count',
              'created_at',
              'updated_at',
              'occupation_id',
              'zoomancy_id',
              'baby_age_id',
              'size_id',
              'visit_reason_id',
              'nearest_station_id',
              'email',
              'encrypted_password',
              'reset_password_token',
              'reset_password_sent_at',
              'remember_created_at',
              'provider',
              'uid',
              'tokens',
              'allow_password_change',
              'is_receive_thank_you_letter',
              'gender',
              'age',
              ]]

# indexをidにする
output = output.set_index('id')

# DataFrameのループ　全行データ取得
for index, row in output.iterrows():
    # uidに代入
    uid = ''
    # pc_mailがあればセット
    if type(row['pc_mail']) is str:
        uid = row['pc_mail']
    # phone_mailを優先して上書き
    if type(row['phone_mail']) is str:
        uid = row['phone_mail']

    # uidを上書きする
    output.at[index, 'uid'] = uid

# csvに出力
# output.to_csv('customers.csv')
print(output['baby_age_id'])
# print(output.iloc[1][2])

