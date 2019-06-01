#!/usr/bin/python3
import csv
import pandas as pd
import MySQLdb
import mojimoji
import jaconv
import datetime
import numpy
from phpserialize import loads
from functools import partial

# ワードプレスから名前とメールの一覧データから取得
dict_mail = {}
wordpressData = pd.read_csv('./src/file/wordpress.csv')
customersData = pd.DataFrame()
# ワードプレスのデータを元にする
for columnName, item in wordpressData.iteritems():
    if columnName == 'First Name':
        customersData['first_name'] = item
    elif columnName == 'Last Name':
        customersData['last_name'] = item
    elif columnName == 'Email':
        customersData['email'] = item
        customersData['uid'] = item
    elif columnName == 'User ID':
        customersData['id'] = item


for index, row in wordpressData.iterrows():
    name = row['Last Name'] + row['First Name']
    dict_mail[name] = row['Email']

# 子供を数字に変換
def convert_children_count(children_str):
    # 文字列が入っている場合は変換
    if type(children_str) is str:
        return mojimoji.zen_to_han(str(children_str).strip("人"))
    else:
        # 文字列が入ってない場合は空文字に変換
        return ''

# DB接続
con = MySQLdb.connect(user='root', passwd='mysql', host='mysql', db='db_olive', charset='utf8')
# カーソルを取得する
cur= con.cursor()

# 生後の情報をDBから取得
sql_baby_ages = "select id, name from baby_ages"
cur.execute(sql_baby_ages)
# 実行結果をすべて取得する
result_baby_ages = cur.fetchall()
# 生後の情報を配列にまとめる
dict_baby_ages = {}
for id, name in result_baby_ages:
    # 名前をキーにしてidを代入する
    dict_baby_ages[name] = id

# 生後をidに変換
error_baby_ages = []
def convert_baby_ages(age):
    key = ''
    if str(age).find('歳') != -1:
        # 最初の2文字を切り取る
        key = str(age)[:2]
    elif str(age).find('ヶ月') != -1:
        # 歳が入ってない場合はそのまま使う
        key = str(age)

    if key != '' and key in dict_baby_ages:
        return dict_baby_ages[key]
    else:
        # keyがからじゃない場合は出力する
        if key != '':
            error_baby_ages.append(key)

        return ''

# サイズ情報をDBから取得する
sql_sizes = "select id, name from sizes"
cur.execute(sql_sizes)
# 実行結果をすべて取得する
result_sizes = cur.fetchall()
# サイズの情報を配列にまとめる
dict_sizes = {}
for id, name in result_sizes:
    # 名前csvファイルに合わせて微調整
    if name == 'S〜M':
        re_name = 'S-M'
    elif name == 'M〜L':
        re_name = 'M-L'
    else:
        re_name = name

    dict_sizes[re_name] = id

# 動物占い情報をDBから取得する
sql_zoomancies = "select id, name from zoomancies"
cur.execute(sql_zoomancies)
# 実行結果をすべて取得する
result_zoomancies = cur.fetchall()
# サイズの情報を配列にまとめる
dict_zoomancies = {}
for id, name in result_zoomancies:
    # 全角スペースを半角スペースに変換(csvが半角スペースになっているので)
    re_name = name.replace("　", " ")
    dict_zoomancies[re_name] = id

# 動物占いをIDに変換
error_zoomancies = []
def convert_zoomancies(zoomancies):
    if zoomancies in dict_zoomancies:
        return dict_zoomancies[zoomancies]
    else:
        # 変換できない場合はから文字にする
        return ''
        # DBに入ってないものを出力
        if type(zoomancies) is str:
            error_zoomancies.append(zoomancies)

# 職業情報をDBから取得する
sql_occupations = "select id, name from occupations"
cur.execute(sql_occupations)
# 実行結果をすべて取得する
result_occupations = cur.fetchall()
# サイズの情報を配列にまとめる
dict_occupations = {}
for id, name in result_occupations:
    dict_occupations[name] = id

# 職業情報をIDに変換
error_occupations = []
def convert_occupations(occupations):
    if occupations in dict_occupations:
        return dict_occupations[occupations]
    else:
        # 変換できない場合はから文字にする
        return ''
        # DBに入ってないものを出力
        if type(occupations) is str:
            error_occupations.append(occupations)

# 最寄駅をDBから取得する
sql_nearest_stations = "select id, name from nearest_stations"
cur.execute(sql_nearest_stations)
# 実行結果をすべて取得する
result_nearest_stations = cur.fetchall()
# サイズの情報を配列にまとめる
dict_nearest_stations = {}
for id, name in result_nearest_stations:
    dict_nearest_stations[name] = id

# 最寄駅をIDに変換
error_nearest_stations = []
def convert_nearest_stations(nearest_stations):
    if nearest_stations in dict_nearest_stations:
        return dict_nearest_stations[nearest_stations]
    else:
        # 変換できない場合はから文字にする
        return ''
        # DBに入ってないものを出力
        if type(nearest_stations) is str:
            error_nearest_stations.append(nearest_stations)

# 来店経緯をDBから取得する
sql_visit_reasons = "select id, name from visit_reasons"
cur.execute(sql_visit_reasons)
# 実行結果をすべて取得する
result_visit_reasons = cur.fetchall()
# サイズの情報を配列にまとめる
dict_visit_reasons = {}
for id, name in result_visit_reasons:
    dict_visit_reasons[name] = id

# 来店経緯をIDに変換
error_visit_reasons = []
def convert_visit_reasons(visit_reasons):
    if visit_reasons in dict_visit_reasons:
        return dict_visit_reasons[visit_reasons]
    else:
        # 変換できない場合はから文字にする
        return ''
        # DBに入ってないものを出力
        if type(visit_reasons) is str:
            error_visit_reasons.append(visit_reasons)

# csvに入っている日付からDBに入れる用の日付に変換する
def convert_date(date, prefix):
    # 文字列かつ"/"が含まれる場合に分割処理をする
    if type(date) is str and date.count('/') == 2:
        # "/"で分割する
        date_list = str(date).split('/')
        # print(date_list)
        year = prefix + date_list[2]
        month = date_list[0]
        day = date_list[1]
        return datetime.date(int(year), int(month), int(day))
    else:
        # DBにcsvインポートするようにNULLにで返す
        return 'NULL'

# DataFrame型で取得
# csvData = pd.read_csv('./src/file/fm_sample.csv')
csvData = pd.read_csv('./src/file/fm.csv')
# 列でデータを扱う
output = pd.DataFrame()
# DataFrameのループ　全列データ取得
for columnName, item in csvData.iteritems():
    if columnName == '〒':
        output['zip_code'] = item
    elif columnName == 'DM配信':
        # インポート用に1,0にする
        output['can_receive_dm_mail'] = list(map(lambda x: 1 if x == '希望' else 0, item))
    elif columnName == 'PCメール':
        output['pc_mail'] = item
    elif columnName == 'P顧客ID':
        # Kを消した数値をIDにする
        output['id'] = list(map(lambda x: str(x).strip("K"), item))
    elif columnName == 'Web検索':
        output['searchd_by'] = item
    elif columnName == 'お子様':
        output['children_count'] = list(map(convert_children_count, item))
    elif columnName == 'カルテNo':
        output['card_number'] = item
    elif columnName == 'サイズ':
        output['size_id'] = list(map(lambda x: dict_sizes[x] if x in dict_sizes else '', item))
    elif columnName == 'サンキューレター':
        # インポート用に1,0にする
        output['is_receive_thank_you_letter'] = list(map(lambda x: 0 if x == '未送付' else 1, item))
    elif columnName == 'ふりがな姓':
        # カタカナへ変換
        output['first_kana'] = list(map(lambda x: jaconv.hira2kata(str(x)), item))
    elif columnName == 'ふりがな名':
        # カタカナへ変換
        output['last_kana'] = list(map(lambda x: jaconv.hira2kata(str(x)), item))
    elif columnName == 'メール配信':
        # インポート用に1,0にする
        output['can_receive_mail'] = list(map(lambda x: 1 if x == '希望' else 0, item))
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
        output['zoomancy_id'] = list(map(convert_zoomancies, item))
        output['zoomancy_id_origin'] = item
    elif columnName == '固定電話':
        output['fixed_line_tel'] = item
    elif columnName == '最寄駅':
        output['nearest_station_id'] = list(map(convert_nearest_stations, item))
        output['nearest_station_id_origin'] = item
    elif columnName == '住所（市区町村）':
        output['city'] = item
    # elif columnName == '氏名':
        # この項目はすでに他であるので使わない
        # output['first_name'] = item
    elif columnName == '初来院日':
        output['first_visited_at'] = list(map(partial(convert_date, prefix='20'), item))
    elif columnName == '職業':
        output['occupation_id'] = list(map(convert_occupations, item))
        output['occupation_id_origin'] = item
    elif columnName == '診察券受渡':
        # インポート用に1,0にする
        output['has_registration_card'] = list(map(lambda x: 1 if x == '済' else 0, item))
    elif columnName == '姓':
        output['first_name'] = item
    elif columnName == '名':
        output['last_name'] = item
    elif columnName == '性別':
        output['gender'] = item
    elif columnName == '生後':
        output['baby_age_id'] = list(map(convert_baby_ages, item))
        output['baby_age_id_origin'] = item
    elif columnName == '生年月日':
        output['birthday'] = list(map(partial(convert_date, prefix='19'), item))
    elif columnName == '知人の紹介':
        output['introducer'] = item
    elif columnName == '都道府県':
        output['prefecture'] = item
    elif columnName == '年齢':
        output['age'] = item
    elif columnName == '住所（番地）':
        # あとで連結ようで使う
        output['banchi'] = item
    elif columnName == '来店経緯':
        output['visit_reason_id'] = list(map(convert_visit_reasons, item))
        output['visit_reason_id_origin'] = item


# 住所を整形してセットする
output['address'] = output['prefecture'].str.cat(output['city'], sep=' ').str.cat(output['banchi'], sep=' ').str.cat(output['building'], sep=' ')
# 不要な一時カラムは削除する
output = output.drop(columns='building')
output = output.drop(columns='banchi')

# 必要なものだけ共通の値をセット
# output['first_visit_store_id'] = ''
# output['last_visit_store_id'] = ''
# output['last_visited_at'] = datetime.date.today()
output['created_at'] = datetime.datetime.today()
output['updated_at'] = datetime.datetime.today()
output['email'] = ''
output['encrypted_password'] = ''
# output['reset_password_token'] = ''
# output['reset_password_sent_at'] = datetime.datetime.today()
# output['remember_created_at'] = datetime.datetime.today()
output['provider'] = ''
output['uid'] = ''
# output['tokens'] = ''
# output['allow_password_change'] = ''


# indexをidにする
output = output.set_index('id')

# FMのデータを名前をキーにして連想配列にする
dict_fm = {}
# 姓名を逆にしたバージョンの連想配列も保持する
dict_fm_revers = {}
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

    name = str(row['first_name']).strip() + str(row['last_name']).strip()
    # PCメール、携帯メールがなくてワードプレスのメールアドレスがある場合はそれを使う
    # if uid == '' and name in dict_mail:
    # →ワードプレスのメールを優先する
    if name in dict_mail:
        uid = dict_mail[name]
        # emailはワードプレスのメールをセットする
        output.at[index, 'email'] = dict_mail[name]

    # uid,emailを上書きする
    output.at[index, 'uid'] = uid
    # output.at[index, 'email'] = uid

    # provider,uidでユニークにする必要があるためproviderに連番をセットする
    # output.at[index, 'provider'] = index

    dict_fm[name] = {}
    # dict_fm_revers[name] = {}

    dict_fm[name]['first_kana'] = row['first_kana']
    dict_fm[name]['last_kana'] = row['last_kana']
    dict_fm[name]['tel'] = row['tel']
    dict_fm[name]['fixed_line_tel'] = row['fixed_line_tel'] if type(row['fixed_line_tel']) is str else ''
    dict_fm[name]['can_receive_mail'] = row['can_receive_mail']
    dict_fm[name]['can_receive_dm_mail'] = row['can_receive_dm_mail']
    dict_fm[name]['birthday'] = row['birthday']
    dict_fm[name]['zip_code'] = row['zip_code']
    dict_fm[name]['prefecture'] = row['prefecture']
    dict_fm[name]['city'] = row['city']
    dict_fm[name]['address'] = row['address'] if type(row['address']) is str else ''
    dict_fm[name]['comment'] = row['comment'] if type(row['comment']) is str else ''
    dict_fm[name]['first_visited_at'] = row['first_visited_at']
    dict_fm[name]['card_number'] = row['card_number']
    dict_fm[name]['introducer'] = row['introducer'] if type(row['introducer']) is str else ''
    dict_fm[name]['searchd_by'] = row['searchd_by'] if type(row['searchd_by']) is str else ''
    dict_fm[name]['has_registration_card'] = row['has_registration_card']
    dict_fm[name]['children_count'] = row['children_count']
    dict_fm[name]['created_at'] = row['created_at']
    dict_fm[name]['updated_at'] = row['updated_at']
    dict_fm[name]['occupation_id'] = str(row['occupation_id'])
    dict_fm[name]['zoomancy_id'] = str(row['zoomancy_id'])
    dict_fm[name]['baby_age_id'] = str(row['baby_age_id'])
    dict_fm[name]['size_id'] = str(row['size_id'])
    dict_fm[name]['visit_reason_id'] = str(row['visit_reason_id'])
    dict_fm[name]['nearest_station_id'] = str(row['nearest_station_id'])
    dict_fm[name]['encrypted_password'] = row['encrypted_password']
    dict_fm[name]['provider'] = index
    dict_fm[name]['is_receive_thank_you_letter'] = row['is_receive_thank_you_letter']
    dict_fm[name]['gender'] = row['gender']
    dict_fm[name]['age'] = row['age']
    # 一時的に使うやつ
    dict_fm[name]['zoomancy_id_origin'] = row['zoomancy_id_origin'] if type(row['zoomancy_id_origin']) is str else ''
    dict_fm[name]['nearest_station_id_origin'] = row['nearest_station_id_origin'] if type(row['nearest_station_id_origin']) is str else ''
    dict_fm[name]['occupation_id_origin'] = row['occupation_id_origin'] if type(row['occupation_id_origin']) is str else ''
    dict_fm[name]['baby_age_id_origin'] = row['baby_age_id_origin'] if type(row['baby_age_id_origin']) is str else ''
    dict_fm[name]['visit_reason_id_origin'] = row['visit_reason_id_origin'] if type(row['visit_reason_id_origin']) is str else ''


# csvに出力
# output.to_csv('customers.csv')
# print(output['children_count'])
# print(output.iloc[1][2])

# DBにインサート 今回はcsvファイルからインサートする
# for index, row in output.iterrows():
#     print(index)
#     sql_insert = """
#         insert into customers(
#             id,
#             first_name,
#             birthday,
#             created_at,
#             updated_at,
#             uid,
#             email
#         )
#         values(%s, %s, %s, %s, %s, %s, %s)
#                  """
#     cur.execute(sql_insert, (index, row['first_name'], '1900-01-01', row['created_at'], row['updated_at'], row['uid'], row['email']))
#     # 実行結果をすべて取得する
#     result = cur.fetchall()
#     print(result)

# 重複削除
# print(set(error_baby_ages))
# print(set(error_zoomancies))
# print(set(error_nearest_stations))
# print(set(error_occupations))
# print(set(error_visit_reasons))



# ワードプレスのデータに合わせて最終的なファイルを出力する
for index, row in customersData.iterrows():
    # wordpressName = str(row['first_name']) + str(row['last_name'])
    # wordpressNameReverse = str(row['last_name']) + str(row['first_name'])
    wordpressName = str(row['first_name']).strip() + str(row['last_name']).strip()
    wordpressNameReverse = str(row['last_name']).strip() + str(row['first_name']).strip()
    keyname = ''
    # 名前でデータを補完
    if wordpressName in dict_fm:
        keyname = wordpressName
    # 姓名逆バージョンで補完
    elif wordpressNameReverse in dict_fm:
        keyname = wordpressNameReverse

    if keyname != '':
        customersData.at[index, 'first_kana'] = dict_fm[keyname]['first_kana']
        customersData.at[index, 'last_kana'] = dict_fm[keyname]['last_kana']
        customersData.at[index, 'tel'] = dict_fm[keyname]['tel']
        customersData.at[index, 'fixed_line_tel'] = dict_fm[keyname]['fixed_line_tel']
        customersData.at[index, 'can_receive_mail'] = dict_fm[keyname]['can_receive_mail']
        customersData.at[index, 'can_receive_dm_mail'] = dict_fm[keyname]['can_receive_dm_mail']
        customersData.at[index, 'birthday'] = dict_fm[keyname]['birthday']
        customersData.at[index, 'zip_code'] = dict_fm[keyname]['zip_code']
        customersData.at[index, 'prefecture'] = dict_fm[keyname]['prefecture']
        customersData.at[index, 'city'] = dict_fm[keyname]['city']
        customersData.at[index, 'address'] = dict_fm[keyname]['address']
        customersData.at[index, 'comment'] = dict_fm[keyname]['comment']
        customersData.at[index, 'first_visited_at'] = dict_fm[keyname]['first_visited_at']
        customersData.at[index, 'card_number'] = dict_fm[keyname]['card_number']
        customersData.at[index, 'introducer'] = dict_fm[keyname]['introducer']
        customersData.at[index, 'searchd_by'] = dict_fm[keyname]['searchd_by']
        customersData.at[index, 'has_registration_card'] = dict_fm[keyname]['has_registration_card']
        customersData.at[index, 'children_count'] = dict_fm[keyname]['children_count']
        customersData.at[index, 'created_at'] = dict_fm[keyname]['created_at']
        customersData.at[index, 'updated_at'] = dict_fm[keyname]['updated_at']
        customersData.at[index, 'occupation_id'] = dict_fm[keyname]['occupation_id']
        customersData.at[index, 'zoomancy_id'] = dict_fm[keyname]['zoomancy_id']
        customersData.at[index, 'baby_age_id'] = dict_fm[keyname]['baby_age_id']
        customersData.at[index, 'size_id'] = dict_fm[keyname]['size_id']
        customersData.at[index, 'visit_reason_id'] = dict_fm[keyname]['visit_reason_id']
        customersData.at[index, 'nearest_station_id'] = dict_fm[keyname]['nearest_station_id']
        customersData.at[index, 'encrypted_password'] = dict_fm[keyname]['encrypted_password']
        customersData.at[index, 'provider'] = dict_fm[keyname]['provider']
        customersData.at[index, 'is_receive_thank_you_letter'] = dict_fm[keyname]['is_receive_thank_you_letter']
        customersData.at[index, 'gender'] = dict_fm[keyname]['gender']
        customersData.at[index, 'age'] = dict_fm[keyname]['age']
        # 一時的に使うやつ
        customersData.at[index, 'zoomancy_id_origin'] = dict_fm[keyname]['zoomancy_id_origin']
        customersData.at[index, 'nearest_station_id_origin'] = dict_fm[keyname]['nearest_station_id_origin']
        customersData.at[index, 'occupation_id_origin'] = dict_fm[keyname]['occupation_id_origin']
        customersData.at[index, 'baby_age_id_origin'] = dict_fm[keyname]['baby_age_id_origin']
        customersData.at[index, 'visit_reason_id_origin'] = dict_fm[keyname]['visit_reason_id_origin']

    # provider,uidでユニークにする必要があるためproviderに連番をセットする
    customersData.at[index, 'provider'] = index


# ワードプレスのデータに合わせて最終的なファイルを出力する
for index, row in customersData.iterrows():
    # コメントに追加するやつ
    comment = row['comment']
    if row['zoomancy_id'] == '':
        if row['zoomancy_id_origin'] != '':
            zoomancy_id_comment = ' 個性心理学:' + str(row['zoomancy_id_origin'])
            comment = comment + zoomancy_id_comment
            # print('----')

    if row['occupation_id'] == '':
        if row['occupation_id_origin'] != '':
            zoomancy_id_comment = ' 職業:' + str(row['occupation_id_origin'])
            comment = comment + zoomancy_id_comment

    if row['baby_age_id'] == '':
        if row['baby_age_id_origin'] != '':
            zoomancy_id_comment = ' 生後:' + str(row['baby_age_id_origin'])
            comment = comment + zoomancy_id_comment

    if row['visit_reason_id'] == '':
        if row['visit_reason_id_origin'] != '':
            zoomancy_id_comment = ' 来店経緯:' + str(row['visit_reason_id_origin'])
            comment = comment + zoomancy_id_comment

    customersData.at[index, 'comment'] = comment


# 不要な一時カラムは削除する
customersData = customersData.drop(columns='zoomancy_id_origin')
customersData = customersData.drop(columns='nearest_station_id_origin')
customersData = customersData.drop(columns='occupation_id_origin')
customersData = customersData.drop(columns='baby_age_id_origin')
customersData = customersData.drop(columns='visit_reason_id_origin')

# 共通の値をセット
customersData['created_at'] = datetime.datetime.today()
customersData['updated_at'] = datetime.datetime.today()

# indexをidにする
customersData = customersData.set_index('id')
# customersテーブル順に並べ替え
customersData = customersData[[
              # 'id',
              'first_name',
              'last_name',
              'first_kana',
              'last_kana',
              'tel',
              'fixed_line_tel',
              # 'pc_mail',
              # 'phone_mail',
              'can_receive_mail',
              'can_receive_dm_mail',
              'birthday',
              'zip_code',
              'prefecture',
              'city',
              'address',
              'comment',
              # 'first_visit_store_id',
              # 'last_visit_store_id',
              'first_visited_at',
              # 'last_visited_at',
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
              # 'reset_password_token',
              # 'reset_password_sent_at',
              # 'remember_created_at',
              'provider',
              'uid',
              # 'tokens',
              # 'allow_password_change',
              'is_receive_thank_you_letter',
              'gender',
              'age',
              ]]

customersData.to_csv('wordpress_customers.csv')