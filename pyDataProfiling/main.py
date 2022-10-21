import pandas as pd
import pyodbc
import sys
import cx_Oracle
from .DictionaryCollect import *
from .SQLCreate import *
from .ProfilingExecute import *
        
class DBSet:
    """ 프로파일링 SQL 저장 및 리포지터리 관리 클래스 """
    def __init__(self):
        ProfilingDB = pd.DataFrame(None, columns=['DB번호', 'DB명', 'dbtype', 'ip', 'port', 'dbname', 'userid', 'password'])
        self.ProfilingDB = ProfilingDB.astype({'DB번호':int, 'DB명':str, 'dbtype':str, 'ip':str, 'port':int, 'dbname':str, 'userid':str, 'password':str}).set_index('DB번호')
        
        self.TiberoConnString = r'Driver={driver};SERVER={server};PORT={port};DB={dbname};UID={uid};PWD={pwd};'
        self.OracleConnString = '{접속계정명}/{비밀번호}@{IP주소}:{접속포트명}/{서비스명}'
        self.RepositoryDBInfo = dict()
        self.ProfilingDBInfo = dict()
    
    
    def RepositoryDBSetting(self, dbtype:str, ip:str, port:int, dbname:str, userid:str, password:str):
        """ 리포지터리 DB를 셋팅 """        
        dbtype = dbtype.upper()
        self.RepositoryDBInfo['dbtype'] = dbtype
        if dbtype not in ['TIBERO', 'ORACLE']:
            assert False, '리포지터리 데이터베이스 유형(TIBERO)만 사용 가능'

        if dbtype == 'TIBERO':
            self.RepositoryDBInfo['ConnectionString'] = self.TiberoConnString.format(driver='{Tibero 6 ODBC Driver}', server=ip, port=str(port), dbname=dbname, uid=userid, pwd=password)
            self.RepositoryDBInfo['Connector'] = pyodbc.connect
        elif dbtype== 'ORACLE':
            self.RepositoryDBInfo['ConnectionString'] = self.OracleConnString.format(접속계정명=userid, 비밀번호=password, IP주소=ip, 접속포트명=str(port), 서비스명=dbname)
            self.RepositoryDBInfo['Connector'] = cx_Oracle.connect

    def RepositoryDBConnectGet(self):
        """ 리포지터리 DB에 접속
            처리방안 검토 필요
             1) timeout으로 인한 접속단절 방지 필요? """
        self.RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])
        return self.RepConn
    
    def ProfilingDBSetting(self, DB번호:int, DB명:str, dbtype:str, ip:str, port:int,  dbname:str, userid:str, password:str):
        """ 프로파일링을 수행할 DB 정의함 """
        ProfilingDB = {'DB번호':DB번호, 'DB명':DB명, 'dbtype':dbtype.upper(), 'ip':ip, 'port':port, 'dbname':dbname, 'userid':userid, 'password':password}
        
        ### DB에 기록
        try:
            cur = self.RepConn.cursor()
            Deletesql = "DELETE 프로파일링_DB정보 WHERE DB번호 = ?"
            cur.execute(Deletesql, DB번호)
            Insertsql = """INSERT INTO 프로파일링_DB정보 (DB번호, DB명, 접속계정명, 접속비밀번호, DB종류, IP주소, 접속포트, 서비스명)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?)"""
            cur.execute(Insertsql, DB번호, DB명, userid, password, dbtype.upper(), ip, port, dbname)
            cur.execute('commit')
        except:
            _, val, _ = sys.exc_info()
            print('DB정보 리포지터리 입력 오류 발생 :', str(val))
            cur.execute('rollback')
        
        try:
            self.ProfilingDB = self.ProfilingDB.drop(DB번호)
        except:
            pass
        finally:
            self.ProfilingDB = self.ProfilingDB.reset_index().append(ProfilingDB, ignore_index=True).astype({'DB번호':int}).set_index('DB번호')

        return self.ProfilingDB
    
    def ProfilingDBLoad(self):
        sql = """SELECT DB번호
                      , DB명
                      , DB종류 as "dbtype"
                      , IP주소 as "ip"
                      , 접속포트 as "port"
                      , 서비스명 as "dbname"
                      , 접속계정명 as "userid"
                      , 접속비밀번호 as "password"
                   FROM 프로파일링_DB정보"""
        self.ProfilingDB = pd.read_sql(sql, self.RepConn).astype({'DB번호':int}).set_index('DB번호')
        return self.ProfilingDB

    def ProfilingDBConnectGet(self, DB번호):
        """ 딕셔너리 수집 대상 DB 접속 하기 """
        ProfilingDB = self.ProfilingDB.loc[DB번호].to_dict()
        self.ProfilingDBInfo['DB번호'] = DB번호
        self.ProfilingDBInfo['dbtype'] = ProfilingDB['dbtype']
        if ProfilingDB['dbtype'] == 'TIBERO':
            self.ProfilingDBInfo['ConnectionString'] = self.TiberoConnString.format(driver='{Tibero 6 ODBC Driver}', server=ProfilingDB['ip'], port=str(ProfilingDB['port']), \
                                                dbname=ProfilingDB['dbname'], uid=ProfilingDB['userid'], pwd=ProfilingDB['password'])
            self.ProfilingDBInfo['Connector'] = pyodbc.connect
        elif ProfilingDB['dbtype'] == 'ORACLE':
            self.ProfilingDBInfo['ConnectionString'] = self.OracleConnString.format(접속계정명=ProfilingDB['userid'], 비밀번호=ProfilingDB['password'], IP주소=ProfilingDB['ip'], \
                                                접속포트명=str(ProfilingDB['port']), 서비스명=ProfilingDB['dbname'])
            self.ProfilingDBInfo['Connector'] = cx_Oracle.connect
        
        ProfConn = self.ProfilingDBInfo['Connector'](self.ProfilingDBInfo['ConnectionString'])

        return ProfConn                
                
class DataProfiling(DBSet):
    """ 프로파일링 메인 클래스 """
    def __init__(self):
        super().__init__()
        self.SQLCreator = SQLCreate() ## SQL 생성 프로그램 
        self.DictCollector = DictionaryCollect() ## 
        self.Executer = ProfilingExecute()
    
    def RepositoryDBConnect(self):
        RepConn = self.RepositoryDBConnectGet()
        self.SQLCreator.RepositoryDBConnect(RepConn)
        self.DictCollector.RepositoryDBConnect(RepConn)
        self.Executer.RepositoryDBInfoGet(self.RepositoryDBInfo)
        
    def ProfilingDBConnect(self, DB번호):
        self.DB번호 = DB번호
        ProfConn = self.ProfilingDBConnectGet(DB번호)
        self.ProfConn = ProfConn
        
        self.DictCollector.ProfilingDBConnect(ProfConn)
        self.DictCollector.ProfilingDBSetting(DB번호)
        
        self.Executer.ProfilingDBInfoGet(self.ProfilingDBInfo)
