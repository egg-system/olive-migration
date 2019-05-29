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
csvData = pd.read_csv('./src/file/wordpress.csv')
for index, row in csvData.iterrows():
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
            print(key)

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
def convert_zoomancies(zoomancies):
    if zoomancies in dict_zoomancies:
        return dict_zoomancies[zoomancies]
    else:
        # DBに入ってないものを出力
        if type(zoomancies) is str:
            print(zoomancies)

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
def convert_occupations(occupations):
    if occupations in dict_occupations:
        return dict_occupations[occupations]
    else:
        # DBに入ってないものを出力
        if type(occupations) is str:
            print(occupations)

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
def convert_nearest_stations(nearest_stations):
    if nearest_stations in dict_nearest_stations:
        return dict_nearest_stations[nearest_stations]
    else:
        # DBに入ってないものを出力
        if type(nearest_stations) is str:
            print(nearest_stations)

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
def convert_visit_reasons(visit_reasons):
    if visit_reasons in dict_visit_reasons:
        return dict_visit_reasons[visit_reasons]
    else:
        # DBに入ってないものを出力
        if type(visit_reasons) is str:
            print(visit_reasons)

# csvに入っている日付からDBに入れる用の日付に変換する
def convert_date(date, prefix):
    # print(type(date))
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
        # 日付が入ってない場合はデフォルト値をセットする(後からDBをNULLに修正する)
        return '9999-12-31'

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
    elif columnName == '固定電話':
        output['fixed_line_tel'] = item
    elif columnName == '最寄駅':
        output['nearest_station_id'] = list(map(convert_nearest_stations, item))
    elif columnName == '住所（市区町村）':
        output['city'] = item
    # elif columnName == '氏名':
        # この項目はすでに他であるので使わない
        # output['first_name'] = item
    elif columnName == '初来院日':
        output['first_visited_at'] = list(map(partial(convert_date, prefix='20'), item))
    elif columnName == '職業':
        output['occupation_id'] = list(map(convert_occupations, item))
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

    name = str(row['first_name']) + str(row['last_name'])
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
    output.at[index, 'provider'] = index


# csvに出力
output.to_csv('customers.csv')
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
