# results to oracle db insert
# author : HyunHo Noh  
# -*- coding: utf-8 -*-
import splunk.Intersplunk

import cx_Oracle
import os
import sys              # arguments 처리
import gzip             # gzip 처리
import csv              # csv 처리
import re

def dbconn(id, passwd, host, db):
    try:
        connString = "%s/%s@%s/%s" % (id, passwd, host, db)
        os.environ["NLS_LANG"] = ".AL32UTF8" 

        return cx_Oracle.connect(connString)
    except Exception, e:
        return false

###
### CSV File read
###
def csvread():
    contentStrings =''

    gzopen = gzip.open(sys.argv[8],'rb')
    cr = csv.reader(gzopen)
    cr.next()                                       # next row(second row)
    for rows in cr: 
        contentStrings=type(cr)
        #for row in rows:
        #    contentStrings = contentStrings+" "+row     # contents merge
        #contentStrings = contentStrings + "\n"
    return contentStrings

# db connect
id     = 'mos'    
passwd = '1'
host   = '127.0.0.1'
db     = 'XE'
con    = dbconn(id, passwd, host, db)
cur = con.cursor()


field = sys.argv[1]

#if len(sys.argv)>1:
#    for fields in sys.argv:

try:
    results,dummyresults,settings = splunk.Intersplunk.getOrganizedResults()
    rows=[]
    for r in results:
        m=re.findall(r"\$(\w+)\$",field)
        #row=(r['TIME'], r['action'], r['method'])
        r['result']=",".join(m)
        #rows.append(row)
        
    #cur.prepare("insert into DBX (DTM, ACTION, METHOD) values(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS'), :2, :3)")
    #cur.executemany(None, rows)
    #con.commit()
    #cur.close()
    #con.close() 

except:
    import traceback
    stack =  traceback.format_exc()
    results = splunk.Intersplunk.generateErrorResults("Error : Traceback: " + str(stack))

splunk.Intersplunk.outputResults( results )
