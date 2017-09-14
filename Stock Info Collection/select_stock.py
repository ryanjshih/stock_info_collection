# coding=utf-8

import psycopg2
import datetime
from pandas.tseries.offsets import BDay


def set_date():
    while True:
        today = datetime.datetime.now().strftime(format("%Y%m%d"))  # Get system date with YYYYMMDD format.
        selected_date = input('請輸入欲查詢日期或按Enter選擇今天：')
        if selected_date == '':
            selected_date = today
            break
        else:
            if not selected_date.isnumeric() or int(selected_date[0:4]) < 2000 or selected_date > today:
                print("日期錯誤")
            else:
                break
    return selected_date


def select_stock(force, select_buy_sell, selected_date, cur):
    limit = 0
    force_sort = 0
    if force == '外資':
        force_sort = '3'
        limit = 50
    elif force == '投信':
        force_sort = '4'
        limit = 30
    else:
        print("法人輸入錯誤。")
        quit()
    print(selected_date, force, '買賣超前' + str(limit) + '名：')

    cur.execute('select stock_number, '
                'stock_name, '
                'foreign_capital_buy - foreign_capital_sell, '
                'trust_buy - trust_sell '
                'from stock where trading_date = \'' + selected_date + '\' '
                'order by ' + force_sort + ' ' + select_buy_sell
                + ' limit \'' + str(limit) + '\'')
    rows = cur.fetchall()
    print('代號'.rjust(8) + '\t名稱'.rjust(12) + '外資買超'.rjust(10) + '投信買超'.rjust(8))
    for row in rows:
        for item in row:
            print(str(item).rjust(10), '\t', end='')
        print()


def compare_forces(select_buy_sell, selected_date, cur):
    cur.execute('select t1.stock_number, t1.stock_name, "外資買超", "投信買超" from '
                '(select stock_number, stock_name, trust_buy - trust_sell as \"投信買超\" '
                'from stock where trading_date = \'20170908\' '
                'order by 3 ' + select_buy_sell +
                ' limit 30) t1 '
                'inner join '
                '(select stock_number, stock_name, foreign_capital_buy - foreign_capital_sell as \"外資買超\" '
                'from stock '
                'where trading_date = \'' + selected_date + '\' '
                'order by foreign_capital_buy - foreign_capital_sell ' + select_buy_sell +
                ' limit 50) t2 '
                'on t1.stock_number = t2.stock_number;')
    rows = cur.fetchall()
    print('外資、投信共同買賣超：')
    print('代號'.rjust(8) + '\t名稱'.rjust(12) + '外資買超'.rjust(10) + '投信買超'.rjust(8))
    for row in rows:
        for item in row:
            print(str(item).rjust(10), '\t', end='')
        print()


def select_individual_stock(selected_date, cur):
    selected_stock_number = input('請輸入股票代號，或輸入 Q 離開：')
    if selected_stock_number in ['Q', 'q', 'Ｑ', 'ｑ']:
        print('掰掰～祝您獲利滿滿！')
        quit()
    else:
        selected_date_in_date_form = datetime.datetime.strptime(selected_date, '%Y%m%d')

        prices_list = []
        try:
            for i in range(0, 6):
                target_date = (selected_date_in_date_form - BDay(i)).strftime('%Y%m%d')
                cur.execute('SELECT closing_price FROM stock WHERE stock_number= \'' + selected_stock_number + '\' '
                            'AND trading_date = \'' + target_date + '\'')
                prices = cur.fetchone()
                for p in prices:
                    prices_list.append(p)

            cur.execute('SELECT stock_number, stock_name, foreign_capital_buy - foreign_capital_sell, '
                        'trust_buy - trust_sell, closing_price, ' 
                        '\'' + str(prices_list[0]*2 - prices_list[2]) + '\'  AS \"A\" ,'
                        '\'' + str(prices_list[1]*2 - prices_list[3]) + '\'  AS \"B\" ,'
                        '\'' + str(prices_list[2]*2 - prices_list[4]) + '\'  AS \"C\" '
                        'FROM stock '
                        'WHERE stock_number = \'' + selected_stock_number + '\' '
                        'AND trading_date = \'' + selected_date + '\'')
            rows = cur.fetchone()
            print('代號'.rjust(10) + '名稱'.rjust(12) + '外資買超'.rjust(9) + '投信買超'.rjust(9),
                  '收盤價'.rjust(8), 'A'.rjust(8), 'B'.rjust(11), 'C'.rjust(11))
            for item in rows:
                print(str(item).rjust(12), end='')
            print()
        except TypeError:
            print("查無此股票")


def main(inquired_date):
    connection = psycopg2.connect(database="StockInfoCollection", user='ryanjshih', password='ryanjshih',
                                  host="stockinfocollection.cykru4k1tbli.ap-northeast-2.rds.amazonaws.com", port="5432")
    cursor = connection.cursor()

    while True:
        buy_sell = input("請輸入 B / S 選擇買賣超排行：")
        if buy_sell in ['B', 'b', 'Ｂ']:
            buy_sell = 'desc'
            break
        elif buy_sell in ['S', 's', 'Ｓ']:
            buy_sell = 'asc'
            break
        else:
            print("請重新輸入")

    select_stock('外資', buy_sell, inquired_date, cursor)
    select_stock('投信', buy_sell, inquired_date, cursor)
    compare_forces(buy_sell, inquired_date, cursor)
    while True:
        select_individual_stock(inquired_date, cursor)

main(set_date())
