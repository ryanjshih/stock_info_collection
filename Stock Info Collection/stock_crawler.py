import psycopg2
import urllib.request
import operator
import csv
import datetime
import locale

def set_date():
    today = datetime.datetime.now().strftime(format("%Y%m%d"))  # Get system date with YYYYMMDD format.
    inquired_date = input('請輸入欲查詢日期或按Enter選擇今天：')
    if inquired_date == '':
        inquired_date = today
    else:
        if int(inquired_date[0:4]) < 2000 or inquired_date > today:
            print("日期錯誤")
    return inquired_date

def get_file(inquired_date):                                                 # Get inquired date or use today as default
    url = 'http://www.tse.com.tw/fund/T86?response=csv&date=' + inquired_date + '&selectType=ALL' #證交所
    urllib.request.urlretrieve(url, 'TWSEtemp.csv')
    ROC_date = str(int(inquired_date[0:4]) - 1911) + '/' + inquired_date[4:6] + '/' + inquired_date[6:8]
    url = 'http://www.tpex.org.tw/web/stock/3insti/daily_trade/3itrade_hedge_download.php?l=zh-tw&se=EW&t=D&d=' + ROC_date + '&s=0,asc' #櫃買中心
    urllib.request.urlretrieve(url, 'TPEXtemp.csv')

def read_file_into_database(file):
    conn = psycopg2.connect(database="StockInfoCollection", user='ryanjshih', password='ryanjshih',
                            host="stockinfocollection.cbxgmr3mhdgg.ap-northeast-1.rds.amazonaws.com", port="5432")
    print("SUCCESS!")
    cur = conn.cursor()
    with open(file, newline='') as f:
        reader = csv.reader(f)
        count_dupkey_error = 0
        for line in reader:
            if line[0].strip().isdigit() and len(line[0].strip()) == 4 or len(line) < 1:
                try:
                    cur.execute('INSERT INTO stock (trading_date, '
                                'stock_number, '
                                'stock_name, '
                                'foreign_capital_buy, '
                                'foreign_capital_sell, '
                                'trust_buy, '
                                'trust_sell) '
                                'VALUES (' + inquired_date + ','
                                + line[0].strip().replace(',', '') + ','
                                + '\'' + line[1].strip() + '\'' + ','
                                + line[2].strip().replace(',', '') + ','
                                + line[3].strip().replace(',', '') + ','
                                + line[5].strip().replace(',', '') + ','
                                + line[6].strip().replace(',', '') + ')')
                    conn.commit()
                    print(line)
                except psycopg2.IntegrityError:
                    print('股票及交易日期以存在')
                    count_dupkey_error += 1
                    print(line)
                    print('重覆筆數：', count_dupkey_error)
                    if count_dupkey_error > 50:
                        print('50筆以上重覆，請重新確認日期')
                        break
                    conn.rollback()
                except Exception as e:
                    print('其他錯誤')
                    z = e
                    print(z)
                    print(line)
                    conn.rollback()
                    pass


        conn.close()

# Porgramme starts here
inquired_date = set_date()
get_file(inquired_date)
read_file_into_database('TPEXtemp.csv')
read_file_into_database('TWSEtemp.csv')































''''
# File handling
file = open("temp.csv", "r")
temp = csv.reader(file)
slist = list(temp)
fix_list = []
for each in slist:
    if len(each) > 1:
        if len(each[0]) == 4:  # Strip the unwanted comment in the original CSV file.
                fix_list.append(each)
fix_list = fix_list[1:]        # Strip header.

stock_index = []               # the list for only the stock codes.
for s in fix_list:
    stock_index.append(s[0])
stock_index = sorted(stock_index)
fix_list = sorted(fix_list, key=operator.itemgetter(0))
stock_dict = dict(zip(stock_index, fix_list))   # Key = stock code, value = everything else.

user_function = input("請輸入「S」搜尋賣超／「B」搜尋賣超 (預設為買超) : ")
if user_function == 'S' or user_function == 's':
    print('取得賣超排行...')
else:
    print('取得買超排行...')

report = open(today + str(user_function).capitalize() + '.txt', 'w')
foreign_list = []
trust_list = []

# Sort for different capitals
def checkForce(column, rank, force, function):
    buy_sell = True
    if function == 'S' or function == 's':
        buy_sell = False
    elif function == 'B' or function == 'b':
        buy_sell = True

    for f in fix_list:
        f[column] = int(f[column].replace(",",""))

    result = sorted(fix_list, key=operator.itemgetter(column), reverse=buy_sell)

    report.write(force +' 前 ' + str(rank) + ': \n')
    print(force +' 前 ' + str(rank) + ': ')
    for r in result[0:rank+1]:
        report.write(r[0] + ',')
        print(r[0] + ',', end='')
        if force == '外資':
            foreign_list.append(r[0])
        elif force == '投信':
            trust_list.append(r[0])
    report.write('\n')
    print()

def checkBoth():
    report.write('兩者同時： ' + '\n')
    print('兩者同時： ')
    for r in (set(trust_list).intersection(foreign_list)):
        report.write(r + ',')
        print(r + ',', end='')
    print()

checkForce(4, 50, '外資', user_function)
checkForce(7, 30, '投信', user_function)
checkBoth()

while True:
    user_selection = input("請輸入股票代碼（或按 Q 離開） ")

    if user_selection == 'q' or user_selection == 'Q':
        print('掰掰～祝您獲利滿滿！')
        break

    try:
        if stock_dict[user_selection]:
            pass

        title_length = []

        for f in slist[1][:8]:
            print(f, end='\t')
            title_length.append(len(f))
        print()

        for (f, l) in zip(stock_dict[user_selection],title_length):
                if f == stock_dict[user_selection][1]:
                    print(str(f).strip().rjust(4, '　'), end='\t')
                    continue
                print(str(f).strip().rjust(l*2, ' '), end='\t')
        print()

    except KeyError:
        print('＊股票代碼輸入錯誤＊')

'''