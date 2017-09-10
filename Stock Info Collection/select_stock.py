import psycopg2
import stock_crawler


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
        cur.execute('SELECT * FROM stock WHERE stock_number=\'' + selected_stock_number + '\''
                    ' AND trading_date = \'' + selected_date + '\'')
        rows = cur.fetchall()
        if len(rows) == 0:
            print("查無此股票")
        for row in rows:
            print(row)


def main():
    connection = psycopg2.connect(database="StockInfoCollection", user='ryanjshih', password='ryanjshih',
                                  host="stockinfocollection.cbxgmr3mhdgg.ap-northeast-1.rds.amazonaws.com", port="5432")
    cursor = connection.cursor()

    inquired_date = stock_crawler.set_date()

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

main()
