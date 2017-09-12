import psycopg2
import urllib.request
import csv
import datetime
import decimal


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


def get_file(selected_date):  # Get inquired date or use today as default
    url = 'http://www.tse.com.tw/fund/T86?response=csv&date=' + selected_date + '&selectType=ALL'  # 證交所
    urllib.request.urlretrieve(url, 'TWSEtemp.csv')
    roc_date = str(int(selected_date[0:4]) - 1911) + '/' + selected_date[4:6] + '/' + selected_date[6:8]
    url = 'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_download.php?l=zh-tw&se=EW&t=D&d=' \
          + roc_date + '&s=0,asc'  # 櫃買中心
    urllib.request.urlretrieve(url, 'TPEXtemp.csv')
    url = 'http://www.twse.com.tw/exchangeReport/MI_INDEX?response=csv&date=' \
          + selected_date + '&type=ALLBUT0999'  # 證交所股價
    urllib.request.urlretrieve(url, 'TWSEpricetemp.csv')
    url = 'http://www.tpex.org.tw/web/stock/aftertrading/otc_quotes_no1430/stk_wn1430_download.php?l=zh-tw&d='\
          + roc_date + '&se=EW&s=0,asc,0'  # 櫃買中心股價
    urllib.request.urlretrieve(url, 'TPEXpricetemp.csv')
    print('成功取得檔案')


def read_stock_price_into_database(file, selected_date, conn, cur):
    with open(file, newline='') as f:
        reader = csv.reader(f)
        for line in reader:
            if len(line) > 0:
                if line[0].strip().isdigit() and len(line[0].strip()) == 4:
                    opening_price = ''
                    highest_price = ''
                    lowest_price = ''
                    closing_price = ''
                    try:
                        if file == 'TWSEpricetemp.csv':
                            opening_price = line[5].strip().replace(',', '')
                            highest_price = line[6].strip().replace(',', '')
                            lowest_price = line[7].strip().replace(',', '')
                            closing_price = line[8].strip().replace(',', '')
                        elif file == 'TPEXpricetemp.csv':
                            opening_price = line[4].strip().replace(',', '')
                            highest_price = line[5].strip().replace(',', '')
                            lowest_price = line[6].strip().replace(',', '')
                            closing_price = line[2].strip().replace(',', '')
                        else:
                            print('錯誤的檔案名稱')
                            quit()
                    except decimal.InvalidOperation:
                        print('Cannot transform to decimal')
                        pass
                    try:
                        temp_dict = {'opening_price': decimal.Decimal(opening_price),
                                     'highest_price': decimal.Decimal(highest_price),
                                     'lowest_price': decimal.Decimal(lowest_price),
                                     'closing_price': decimal.Decimal(closing_price)}
                        for K in temp_dict.keys():
                            cur.execute('UPDATE stock SET ' + K + '=' + str(temp_dict[K])
                                        + ' where stock_number = \'' + line[0].strip() + '\''
                                        + ' and trading_date = \'' + selected_date + '\';')
                        print(line)
                    except Exception as e:
                        print(line)
                        print(e)
                        pass
                conn.commit()


def read_file_into_database(file, selected_date, conn, cur):
    with open(file, newline='') as f:
        reader = csv.reader(f)
        for line in reader:
            if len(line) > 0:
                if line[0].strip().isdigit() and len(line[0].strip()) == 4:
                    cur.execute('DELETE FROM stock WHERE trading_date = \'' + selected_date + '\' '
                                'AND stock_number = \'' + line[0] + '\'')
                    try:
                        cur.execute('INSERT INTO stock (trading_date, stock_number, stock_name, foreign_capital_buy, '
                                    'foreign_capital_sell, trust_buy, trust_sell) '
                                    'VALUES (' + selected_date + ','           # 交易日期
                                    + line[0].strip().replace(',', '') + ','   # 股票代號
                                    + '\'' + line[1].strip() + '\'' + ','      # 股票名稱
                                    + line[2].strip().replace(',', '') + ','   # 外資買股
                                    + line[3].strip().replace(',', '') + ','   # 外資賣股
                                    + line[5].strip().replace(',', '') + ','   # 投信買股
                                    + line[6].strip().replace(',', '') + ')')  # 投信賣股
                        conn.commit()
                        print(line)
                    except Exception as e:
                        print(line)
                        print('其他錯誤', e)
                        pass


def main(inquired_date):
    get_file(inquired_date)

    connection = psycopg2.connect(database="StockInfoCollection", user='ryanjshih', password='ryanjshih',
                                  host="stockinfocollection.cbxgmr3mhdgg.ap-northeast-1.rds.amazonaws.com", port="5432")
    print("成功建立資料庫連線，開始寫入股票資料...")
    cursor = connection.cursor()

    read_file_into_database('TPEXtemp.csv', inquired_date, connection, cursor)
    read_file_into_database('TWSEtemp.csv', inquired_date, connection, cursor)
    read_stock_price_into_database("TWSEpricetemp.csv", inquired_date, connection, cursor)
    read_stock_price_into_database("TPEXpricetemp.csv", inquired_date, connection, cursor)

    connection.close()
    print("執行完畢！")

main(set_date())
