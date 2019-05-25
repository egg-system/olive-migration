# olive-migration

### コマンド
* build
```
$ docker-compose up -d --build
```

* スクリプト実行
```
$ docker-compose exec script bash -c "python src/migration.py"
```

* ログイン
```
$ docker-compose exec script bash
```

* DB確認
```
$ docker-compose exec script bash
$ mysql -u root -p
```

* docker上のファイルをコピー
```
(例)
$ sudo docker cp c1d6285eb718:/root/customers.csv .
```