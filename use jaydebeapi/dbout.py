# results to oracle db insert
# author : HyunHo Noh  
# -*- coding: utf-8 -*-
import splunk.Intersplunk

import os
import gzip             # gzip 처리
import csv              # csv 처리
import re
import sys              # arguments 처리
import logging, logging.handlers

###
### Splunk logging setup
### http://dev.splunk.com/view/splunk-extensions/SP-CAAAEA9
###
def setup_logging():
    logger = logging.getLogger('splunk.sms')
    SPLUNK_HOME = os.environ['SPLUNK_HOME']

    LOGGING_DEFAULT_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log.cfg')
    LOGGING_LOCAL_CONFIG_FILE = os.path.join(SPLUNK_HOME, 'etc', 'log-local.cfg')
    LOGGING_STANZA_NAME = 'python'
    LOGGING_FILE_NAME = "dbout.log"
    BASE_LOG_PATH = os.path.join('var', 'log', 'splunk')
    LOGGING_FORMAT = "%(asctime)s %(levelname)-s\t%(module)s:%(lineno)d - %(message)s"
    splunk_log_handler = logging.handlers.RotatingFileHandler(os.path.join(SPLUNK_HOME, BASE_LOG_PATH, LOGGING_FILE_NAME), mode='a')
    splunk_log_handler.setFormatter(logging.Formatter(LOGGING_FORMAT))
    logger.addHandler(splunk_log_handler)
    splunk.setupSplunkLogger(logger, LOGGING_DEFAULT_CONFIG_FILE, LOGGING_LOCAL_CONFIG_FILE, LOGGING_STANZA_NAME)
    return logger

class dboutput():
    id     = 'mos'
    passwd = '1'
    host   = '10.0.0.20'
    db     = 'XE'
    def __init__(self):
        self.ostype=os.name
        # print self.ostype
        if(self.ostype=="posix"):
            # module 을 불러오기 위해서 lib 디렉토리도 추가
            sys.path.append('./lib/nix')
        if(self.ostype=="nt"):
            sys.path.append('.\lib\win64')

        import jpype
        # jaydebeapi에서 oracle을 사용하기 위해서 jre 경로를 추가.
        if(self.ostype=="posix"):
            # module 을 불러오기 위해서 lib 디렉토리도 추가
            os.environ['JAVA_HOME'] = '/usr/lib/jvm/java-1.6.0-openjdk-1.6.0.0.x86_64/jre'
            jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=./lib/ojdbc6.jar')
        if(self.ostype=="nt"):  # 윈도우
            os.environ['JAVA_HOME'] = 'C:\Program Files\Java\jre7' # 위치에 맞게 수정해야 함.
            jpype.startJVM(jpype.getDefaultJVMPath(), '-Djava.class.path=.\lib\ojdbc6.jar')

    def dbconn(self):
        import jaydebeapi

        try:
            # connString = 'jdbc:oracle:thin:mos/1@10.0.0.20:1521:XE'
            connString = "jdbc:oracle:thin:%s/%s@%s/%s" % (self.id, self.passwd, self.host, self.db)
            # print connString
            conn =  jaydebeapi.connect('oracle.jdbc.driver.OracleDriver', connString )
            return conn
        except Exception, e:
            logger.error(e)

    def stopJVM(self):
        jpype.shutdownJVM()
        
###
### DB Insert
###
def do_run(cur, tablename, rows, fields, insert_query, place_holders, sqltype="auto"):
    #DB 출력하는 구문. executemany를 이용함
    try:
        if sqltype=="auto": # 기본 설정시
            insert_query = "INSERT IGNORE INTO `{0}` ({1}) VALUES ({2})".format(tablename, ",".join(fields), place_holders)
        logger.info("insert query : %s" % insert_query)
        cur.executemany(insert_query, rows)
        result = cur.rowcount
    except Exception, e:
        logger.error (e)
    return result

# 변수 초기화
place_holders = fields = insert_query = ""

# 기본 입력시 필드 명 가져오기
field = {}
if len(sys.argv)>1:
    for i in range(1, len(sys.argv)):
        field[i]=sys.argv[i]
    fieldvalues = field.values()
    fieldlen = len(fieldvalues)-1

# <--------- Main  ----------->
if __name__ == '__main__':
    logger = setup_logging()


    #if len(sys.argv)>1:
    #    for fields in sys.argv:

    try:
        results,dummyresults,settings = splunk.Intersplunk.getOrganizedResults()
        keywords, options             = splunk.Intersplunk.getKeywordsAndOptions()
        tablename                     = options.get("tablename", "default_table")
        sqltype                       = options.get("type", "auto")
        rows=[]

        if sqltype=="sql":
            findargs  = re.compile("\$(\w+)\$")                         # $fieldname$를 추출하기 위한 정규식
            findwords = findargs.findall(fieldvalues[fieldlen])         # 모두 추출한 내용을 list로 저장
            insert_query = re.sub("\$\w+\$","?", fieldvalues[fieldlen]) # fieldvalues[fieldlen] 값에 insert query 위치. 보통 명령어 마지막에 기록하도록 설정.
            logger.info("findwords : %s" % findwords)

        connect = dboutput()
        con = connect.dbconn()
        cur = con.cursor()

        for r in results:
            place_holders = ', '.join(['?'] * len(r.keys()))
            if sqltype=="sql":                  # 입력한 구문이 필드만 있는것인지 sql인지 확인
                row = [r[x] for x in findwords] # query에서 찾은 필드 목록을 키로해서 r에서 값을 list형태로 만듬
            else:
                fields = r.keys()
                row = r.values()
            rows.append(row)
        runresult = do_run(cur, tablename, rows, fields, insert_query, place_holders, sqltype)

        # cur.prepare("insert into DBX (DTM, ACTION, METHOD) values(TO_TIMESTAMP(:1, 'YYYY-MM-DD HH24:MI:SS'), :2, :3)")
        # cur.executemany("insert into DBX (DTM, ACTIONS, METHODS) values(TO_TIMESTAMP(?, 'YYYY-MM-DD HH24:MI:SS'), ?, ?)", rows)


    except:
        import traceback
        stack =  traceback.format_exc()
        results = splunk.Intersplunk.generateErrorResults("Error : Traceback: " + str(stack))
    finally:
        if con:
            con.commit()
            cur.close()
            con.close()
            # connect.stopJVM()

    splunk.Intersplunk.outputResults( results )
