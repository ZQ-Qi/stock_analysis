'''
从新浪财经获取股票每日股价信息
'''

import urllib.request
import pymysql
import settings
import os


def get_csv(stockid):
    if stockid[0:1] == "6":
        stock_code = "0" + stockid
    else:
        stock_code = "1" + stockid
    print("<get_csv> is trying to download data {}".format(stock_code))
    url = "http://quotes.money.163.com/service/chddata.html?code={}&fields=TCLOSE;HIGH;LOW;TOPEN;LCLOSE;CHG;PCHG;TURNOVER;VOTURNOVER;VATURNOVER;TCAP;MCAP".format(stock_code)
    print(url)
    file_path = "dl_datapath/{}.csv".format(stockid)
    urllib.request.urlretrieve(url, file_path)

def get_filename_by_path():
    path = "dl_datapath"
    files_list = os.listdir(path)
    filename_list = []
    for item in files_list:
        filename_list.append(item[:-4])
    return filename_list

def download_by_db():
    conn = pymysql.connect(host=settings.host, port=settings.port, user=settings.user,
                           password=settings.password,db=settings.db, charset='utf8')
    cur = conn.cursor()

    sql = "select distinct gpdm from zfss;"
    stock_list = []
    try:
        cur.execute(sql)
        results = cur.fetchall()
        for row in results:
            stock_list.append(row[0])
    except pymysql.Error as e:
        print("ERROR: {}".format(e))

    finished_list = get_filename_by_path()
    task_list = [x for x in stock_list if x not in finished_list]

    cur.close()
    conn.close()

    for item in task_list:
        get_csv(item)






# get_csv('002566')
# get_csv('600121')
download_by_db()