#!/usr/bin/python
import pymysql

def get_deal(dbname, root):
    sps = 'f01606675'
    #db = pymysql.connect(host='localhost', user='root', password='123456', database='ecology_fil')
    db = pymysql.connect(host='localhost', user='root', password='123456', database=dbname)
    cursor = db.cursor()

    for sp in sps.split(','):
        sql = "select data_id, proposal_cid from client_deals where provider = %s and state = 7 order by id desc"
        cursor.execute(sql, (sp,))
        deals = cursor.fetchall()
        for deal in deals:
            sql = "select filename from data where id = %s"
            cursor.execute(sql, (deal[0],))
            data = cursor.fetchone()
            if data:
                print(root + "/" + data[0] + ".car", deal[1])
            else:
                pass
                #print(dbname, deal)

for db in [["saofil", "/root/deals2"], ["epeng_fil", "/root/deals2/epeng"], ["epeng1_fil", "/root/deals2/epeng"], ["ecology_fil", "/root/deals2/ecology"]]:
    get_deal(db[0], db[1])