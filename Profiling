#import random
#from IPython.display import clear_output
import cx_Oracle
import pandas as pd
import pyodbc
import numpy as np
import uuid
import sys

import warnings
import json
import datetime
import math


from threading import Thread
#from tqdm import tqdm
from tqdm.notebook import tqdm
from IPython.core.display import display, HTML
import time
from copy import copy

display(HTML("<style>.container{width:90% !important;}</style>"))


### 프로파일링 실행 클래스 정의
class PROFILINGEXECUTER:
    def __init__(self):
        self.ProfilingDbTarget = None
        self.profiling_insert_sql = """INSERT INTO {INSERT테이블명} \n     {컬럼정의문구} \n     VALUES {바인딩문구}"""
        
        self.Runners = []
    
    def ProfilingRepositoryConnection(self):
        """ 프로파일링 리포지터리 DB 접속 요청 """
        ### Tibero로 변경
        self.ConnString = 'Driver={driver};SERVER={server};PORT={port};DB={dbname};UID={uid};PWD={pwd};'\
            .format(driver='{Tibero 6 ODBC Driver}', server='10.5.147.177', port='1521', dbname='DATAWARE', uid='dtwareadm', pwd='Encore##5868')
        self.dqconn = pyodbc.connect(self.ConnString)
        self.dqconn2 = pyodbc.connect(self.ConnString)
                 

    def ProfilingDatabaseTargeting(self, **kwargs):
        """ 프로파일링 대상 DB 정보 생성 """
        sql = """SELECT DB번호, DB명, 접속계정명, 접속비밀번호, DBMS접속방식, DB종류
                      , IP주소, 접속포트, 서비스명, 프로파일링최대허용접속수
                      , 프로파일링수행허용여부
                      , 프로파일링담당자명
                      , 스키마정보수집여부
                      , CASE WHEN DB종류 = 'TIBERO' THEN 'Driver={driver};SERVER='||IP주소||';PORT='||접속포트||';DB='||서비스명||';UID='||접속계정명||';PWD='||접속비밀번호||';'
                             WHEN DB종류 = 'ORACLE' THEN 접속계정명||'/'||접속비밀번호||'@'||IP주소||':'||접속포트||'/'||서비스명
                             END AS 접속문자열
                      , 프로파일링허용시작시각
                      , 프로파일링허용종료시각
                   FROM DB정보
                  WHERE 1=1
                    {추가조건}
                    ORDER BY DB번호"""

        for k, v in kwargs.items():
            pass

        if k == '담당자명':
            exec_sql = sql.format(driver='{Tibero 6 ODBC Driver}', 추가조건='AND 프로파일링담당자명 = ?')    
            self.ProfilingDbTarget = pd.read_sql(sql=exec_sql, con=self.dqconn, params=[v])
        
        elif k == '프로파일링대상' and v == '전체':
            exec_sql = sql.format(driver='{Tibero 6 ODBC Driver}', 추가조건='')
            self.ProfilingDbTarget = pd.read_sql(sql=exec_sql, con=self.dqconn)
        
        elif k == '프로파일링대상' and v != '전체':
            exec_sql = sql.format(driver='{Tibero 6 ODBC Driver}', 추가조건='')    
            fullset_df = pd.read_sql(sql=exec_sql, con=self.dqconn)
            df_temp = fullset_df.head(0)
            for 대상DB in v.split(','):
                df_temp = df_temp.append(fullset_df[fullset_df['DB명'] == 대상DB])                
            self.ProfilingDbTarget = df_temp
        
        elif k == '프로파일링수행허용여부':
            exec_sql = sql.format(driver='{Tibero 6 ODBC Driver}', 추가조건='AND 프로파일링수행허용여부 = ?')    
            self.ProfilingDbTarget = pd.read_sql(sql=exec_sql, con=self.dqconn, params=[v])
        
        elif k == 'DB번호':
            exec_sql = sql.format(driver='{Tibero 6 ODBC Driver}', 추가조건='AND DB번호 IN ({DB번호리스트})'.format(DB번호리스트=v))    
            self.ProfilingDbTarget = pd.read_sql(sql=exec_sql, con=self.dqconn)
        else:
            assert '프로파일링 대상 조건이 필요합니다!'
            
            
        self.ProfilingDbTarget['DB번호'] = self.ProfilingDbTarget['DB번호'].astype(int)
        self.ProfilingDbTarget['접속포트'] = self.ProfilingDbTarget['접속포트'].astype(int)
        self.ProfilingDbTarget['프로파일링최대허용접속수'] = self.ProfilingDbTarget['프로파일링최대허용접속수'].astype(int)
            
    def ConnectionTest(self):
        """프로파일링 대상 DB 접속 테스트"""
        UpdateSQL = "UPDATE DB정보 SET 접속테스트성공여부 = ?, 접속테스트오류내용 = ? WHERE DB번호 = ?"
        for idx, DB번호, 접속문자열, DBMS접속방식 in self.ProfilingDbTarget[['DB번호', '접속문자열', 'DBMS접속방식']].to_records():
            print(DB번호, 접속문자열)
            try:
                if DBMS접속방식 == 'ODBC':
                    test_conn = pyodbc.connect(접속문자열)
                elif DBMS접속방식 == 'OracleClient':
                    test_conn = cx_Oracle.connect(접속문자열)
                else:
                    test_conn = pyodbc.connect(접속문자열)

                self.dqconn.execute(UpdateSQL, 'Y', '', int(DB번호))
                self.dqconn.execute('COMMIT')

                test_conn.close()

            except:
                _, exc_obj,_ = sys.exc_info()
                error_message = str(exc_obj).replace("'","").replace('"','')
                self.dqconn.execute(UpdateSQL, 'N', error_message, int(DB번호))
                self.dqconn.execute('COMMIT')

    
    def ProfilingSQLTypeVerify(self, 프로파일링SQL유형번호):
        """ INSERT 유형이 다양하기 때문에, INSERT 구문을 다양하게 정의 """
        sql = """SELECT INSERT테이블명, INSERT구문컬럼정의문구, INSERT구문바인딩문구
                   FROM 프로파일링SQL유형
                  WHERE 프로파일링SQL유형번호 = ? """
        
        cur = self.dqconn.cursor()
        cur.execute(sql, (프로파일링SQL유형번호))
        INSERT테이블명, INSERT구문컬럼정의문구, INSERT구문바인딩문구 = cur.fetchone()
        
        return INSERT테이블명, INSERT구문컬럼정의문구, INSERT구문바인딩문구
        
    def JSONExecute(self, sql_info):
        """ json을 파싱하여 데이터 처리"""
        sql_info = json.loads(sql_info)
        ### 컬럼분석정보 프로파일링 결과를 삭제해야 할 경우
        if sql_info['targettable'] == '컬럼분석정보' or sql_info['targettable'] == '샘플데이터수집정보':
            delete_format_sql = """DELETE {targettable}
                    WHERE DB번호 = ?
                    AND 스키마명 = ?
                    AND 테이블명 = ?
                    AND 컬럼명 = ? """
            delete_execute_sql = delete_format_sql.format(targettable = sql_info['targettable'])
            cur = self.dqconn.cursor()
            for column in sql_info['column']:
                column = column.replace("\"","")
                cur.execute(delete_execute_sql, sql_info['dbno'], sql_info['schema'], sql_info['table'], column)
            cur.execute('commit')
    
    
    def ConnectionGet(self, DBMS접속방식, 접속문자열):
        if DBMS접속방식 == 'ODBC':
            conn = pyodbc.connect(접속문자열)
        if DBMS접속방식 == 'OracleClient':
            conn = cx_Oracle.connect(접속문자열)
        return conn
        
    
    def ProfilingSession(self, DB번호, DBMS접속방식, 접속문자열):
        """하나의 세션을 띄우는 단위로써, Background에서 실행함 """
        ErrorYN = True
        
        while_cnt = 200
        #while True:
        for _ in range(while_cnt): ### 10번씩만 수행되도록 변경
            ### Tibero SQL 오류 발생시 커넥션 종료 현상 발생하여 아래와 같이 처리함
            ### 반복문마다 커넥션을 새로 할 경우, 장애 발생 소지 있음
            
            if ErrorYN == True:
                prod_conn = self.ConnectionGet(DBMS접속방식, 접속문자열)
                dqconn = pyodbc.connect(self.ConnString)
                
                ErrorYN = False
            
            ### 접속대상 DB에 대해 접속 종료시 대처하기 위한 Try처리
            call_uuid = str(uuid.uuid1())
            
            ### 현재 실행할 건수가 남아있는지 체크하고, 없으면 반복문 수행 멈춤
            #sql_repo_conn = self.sql_repo_pool.acquire()
            #cur = sql_repo_conn.cursor()
            cur = dqconn.cursor()
            WaitTotalCountSQL = """SELECT COUNT(*)
                                     FROM 프로파일링SQL
                                    WHERE DB번호 = to_number(?)
                                      AND 프로파일링실행상태 = '대기'"""
            cur.execute(WaitTotalCountSQL, str(DB번호))
            WaitTotalCount, = cur.fetchone()
            if WaitTotalCount == 0.:
                break
                
            #프로파일링을 멈취야 할 경우나, 중간에 멈춰야 하는 경우 체크
            cur = dqconn.cursor()
            ProfilingStopConfirmSQL = """SELECT NVL(프로파일링수행허용여부, 'N'), 프로파일링허용종료시각
                                           FROM DB정보
                                          WHERE DB번호 = to_number(?)
                                            """
            cur.execute(ProfilingStopConfirmSQL, str(DB번호))
            ProfilingStopConfirmYN, ProfilingConfirmTime = cur.fetchone()
            
            ## 프로파일링이 허용되지 않은 경우
            if ProfilingStopConfirmYN != 'Y':
                break
            
            ## 프로파일링이 시간이 아닌 경우
            now = time.localtime()
            nowtime = "%02d%02d%02d" % (now.tm_hour, now.tm_min, now.tm_sec)
            if nowtime >= ProfilingConfirmTime:
                break

            
            ### 프로파일링 대상 쿼리 조회
            cur = dqconn.cursor()
            WaitSQL = """SELECT 프로파일링SQL번호, 프로파일링SQL유형번호, 프로파일링SQL정보
                           FROM (SELECT *
                                   FROM 프로파일링SQL
                                  WHERE DB번호 = to_number(?)
                                    AND 프로파일링실행상태 = '대기'
                                  ORDER BY 실행순서)
                           WHERE ROWNUM = 1"""

            cur.execute(WaitSQL, str(DB번호))
            프로파일링SQL번호, 프로파일링SQL유형번호, 프로파일링SQL정보 = cur.fetchone()
            프로파일링SQL정보 = str(프로파일링SQL정보)

            
            ### 세션지정 SQL
            cur = dqconn.cursor()
            UpdateSQL1 = """UPDATE 프로파일링SQL
                                SET 프로파일링실행상태 = '실행'
                                  , 프로파일링시작일시 = SYSDATE
                                  , 프로파일링트랜잭션아이디 = ?
                              WHERE 프로파일링SQL번호 = ?
                                AND 프로파일링실행상태 = '대기'
                                AND 프로파일링트랜잭션아이디 IS NULL"""
            cur.execute(UpdateSQL1, call_uuid, 프로파일링SQL번호)
            cur.execute('commit')
            cur.close()

            
            ### 세션확정 확인
            cur = dqconn.cursor()
            MySessionVefirySQL = """SELECT COUNT(*)
                                  FROM 프로파일링SQL
                                 WHERE 프로파일링SQL번호 = ?
                                   AND 프로파일링트랜잭션아이디 = ?
                                   AND 프로파일링실행상태 = '실행'"""
            cur.execute(MySessionVefirySQL, 프로파일링SQL번호, call_uuid)
            VefifyResult = cur.fetchone()
            
            cur = dqconn.cursor()
            proflingquerycallSQL = """SELECT 프로파일링SQL 
                                        FROM 프로파일링SQL분할
                                       WHERE 프로파일링SQL번호 = ?
                                       ORDER BY SQL순서"""
            
            cur.execute(proflingquerycallSQL, 프로파일링SQL번호)
            result = cur.fetchall()
            프로파일링SQL = "".join([row[0] for row in result])
            cur.close()
            
            ### update한 결과가 내가 실행한 세션이 맞다면
            ### Prodcution DB에서 SQL을 수행시키는 로직
            if VefifyResult[0] == 1:
                try:
                    json_프로파일링SQL정보 = json.loads(프로파일링SQL정보)
                    
                    prod_cur = prod_conn.cursor()
                    sql_execute_status, profiling_result, info = self.SQLExecuter(prod_cur, 프로파일링SQL)
                    prod_cur.close()

                    if sql_execute_status == '완료' and len(profiling_result) > 0:
                        INSERT테이블명, 컬럼정의문구, 바인딩문구 = self.ProfilingSQLTypeVerify(프로파일링SQL유형번호)
                        exec_profiling_insert_sql = self.profiling_insert_sql.format(INSERT테이블명=INSERT테이블명, 컬럼정의문구=컬럼정의문구, 바인딩문구=바인딩문구)

                        self.JSONExecute(프로파일링SQL정보) ###결과 적재전 중복 데이터 삭제, DataDupicate 방지
                        cur = dqconn.cursor()
                        cur.executemany(exec_profiling_insert_sql, profiling_result)
                        cur.execute('commit')
                    elif sql_execute_status == '완료' and len(profiling_result) == 0:
                        sql_execute_status = '검토대상'
                        info['오류내용'] = '데이터 0건'
                    
                    
                except:
                    info = dict()
                    sql_execute_status = '오류'
                    _, exc_obj,_ = sys.exc_info()
                    info['오류내용'] = 'Repository Execution 오류 발생 : \n' + str(exc_obj)
                    
                finally:
                    ### 실행 상태 기록
                    #sql_repo_conn = self.sql_repo_pool.acquire()
                    #cur = sql_repo_conn.cursor()
                    cur = dqconn.cursor()
                    UpdateSQL2 = """UPDATE 프로파일링SQL
                                    SET 프로파일링실행상태 = ?
                                      , 프로파일링종료일시 = SYSDATE
                                      , 프로파일링트랜잭션아이디 = ''
                                      , 오류내용 = ?
                                  WHERE 프로파일링SQL번호 = ?
                                    AND 프로파일링실행상태 = '실행'
                                    AND 프로파일링트랜잭션아이디 = ?"""

                    cur.execute(UpdateSQL2, sql_execute_status, info['오류내용'], 프로파일링SQL번호, call_uuid)
                    cur.execute('commit')
                    cur.close()
                    
                    time.sleep(2)
                    print('DB번호:', DB번호, 'table명:',json_프로파일링SQL정보['table'],' 프로파일링 종료')
                    ## 오류 발생시 커넥션을 다시 하기 위한 용도
                    if sql_execute_status == '오류':
                        ErrorYN = True


    def SQLExecuter(self, cur, 프로파일링SQL):
        """ 세션에서 SQL을 처리할 수 있도록 하는 함수 """
        """ cur은 수집대상 DB """
        info = dict()
        try:
            cur.execute(프로파일링SQL)
            result = cur.fetchall()  ### 2차원 배열
            sql_execute_status = '완료'
            info['오류내용'] = ''
            
        except:
            result = [[]]
            sql_execute_status = '오류'
            _, exc_obj,_ = sys.exc_info()
            info['오류내용'] =  'Production Execution 오류 발생 : \n' + str(exc_obj)
            ### 오류 처리 내용 
        finally:
            return sql_execute_status, result, info
        
    def RunnningVerify(self, DB번호):
        VerifySQL = """SELECT COUNT(*)
        FROM 프로파일링SQL
        WHERE DB번호 = ?
        AND 프로파일링실행상태 = '실행'
        """
        
        cur = self.dqconn.cursor()
        cur.execute(VerifySQL, int(DB번호))
        VerifyCnt, = cur.fetchone()
        
        result = True if VerifyCnt == 0 else False
        return result
        
    def ProfilingProcessInitial(self):
        """ 실행중인 프로세스 초기화, 작업하다가 강제로 멈춘 경우 """
        for idx, DB번호 in self.ProfilingDbTarget[['DB번호']].to_records():
            result = self.RunnningVerify(DB번호)
            assert False if result == True else True, "수행중인 프로파일링 프로세스가 없습니다"
            
            UpdateSQL = """UPDATE 프로파일링SQL
            SET 프로파일링실행상태 = '대기',
                프로파일링시작일시 = Null,
                프로파일링종료일시 = Null,
                프로파일링트랜잭션아이디 = Null,
                오류내용 = Null
            WHERE 프로파일링실행상태 = '실행'
            AND DB번호 = ?"""
            
            self.dqconn.execute(UpdateSQL, int(DB번호))
        self.dqconn.execute('COMMIT')
        
    
    def MultiRunner(self):
        """ 병렬 수행을 위한 Multi Thread 처리 """
        for idx, DB번호, DBMS접속방식, 접속문자열, 최대허용수, 허용시작시각 in self.ProfilingDbTarget[['DB번호', 'DBMS접속방식', '접속문자열', '프로파일링최대허용접속수', '프로파일링허용시작시각']].to_records():
            result = self.RunnningVerify(DB번호)
            assert result, "현재 수행중인 프로파일링 프로세스가 존재합니다." 
            while True:
                now = time.localtime()
                nowtime = "%02d%02d%02d" % (now.tm_hour, now.tm_min, now.tm_sec)
                if nowtime > 허용시작시각:
                    break
                time.sleep(10)
                
            for i in range(최대허용수):
                runner = Thread(target=self.ProfilingSession, args=(DB번호, DBMS접속방식, 접속문자열,))
                runner.start()
                self.Runners.append(runner)
                print("프로세스 {0} 시작".format(i+1))
                
                
class PROFILINGSQLCREATOR(PROFILINGEXECUTER):
    def __init__(self):
        super().__init__()
        
    def ProfilingTargeting(self, target, 증분여부='Y'):
        """ 컬럼 프로파일링 대상 SQL을 생성해야 할 대상 지정 """
        
        self.targetDB = self.ProfilingDbTarget['DB번호'].to_list()
        targetDB = ", ".join(map(str, self.targetDB))
        
        ProfilingTargetingSQL = """SELECT DB번호
                                 , 스키마명
                                 , 테이블명
                                 , 테이블코멘트
                                 , 컬럼명
                                 , 컬럼코멘트
                                 , 데이터타입
                                 , ROW_NUMBER() OVER(PARTITION BY DB번호, 스키마명, 테이블명, TRUNC(컬럼순서/10) ORDER BY 컬럼순서)-1 AS 컬럼순서
                                 , NULL여부
                                 , PK여부
                                 , 길이
                                 , 소수점길이
                                 , TRUNC(컬럼순서/10) AS 테이블그룹핑번호
        FROM (SELECT A.DB번호     AS DB번호
                   , A.스키마명     AS 스키마명
                   , A.테이블명     AS 테이블명
                   , A.테이블코멘트   AS 테이블코멘트
                   , B.컬럼명       AS 컬럼명
                   , B.컬럼코멘트    AS 컬럼코멘트
                   , B.데이터타입    AS 데이터타입
                   , ROW_NUMBER() OVER(PARTITION BY A.DB번호, A.스키마명, A.테이블명 ORDER BY B.컬럼순서) AS 컬럼순서
                   , B.NULL여부    AS NULL여부
                   , B.PK여부      AS PK여부
                   , B.길이        AS 길이
                   , B.소수점길이    AS 소수점길이
                   , TRUNC(B.컬럼순서/10) AS 테이블그룹핑번호
                FROM 테이블정보 A, 컬럼정보 B, 스키마정보 C
               WHERE A.DB번호 = B.DB번호 
                 AND A.스키마명 = B.스키마명
                 AND A.테이블명 = B.테이블명
                 AND A.DB번호 = C.DB번호
                 AND A.스키마명 = C.스키마명
                 AND C.수집대상여부 = 'Y'
                 AND A.DB번호 IN ({대상DB})
                 AND {데이터타입지정조건문}
                 {증분여부로직})"""
        
        컬럼프로파일링 = " B.데이터타입 NOT IN ('BLOB', 'RAW', 'CLOB', 'NCLOB', 'LONG', 'NBLOB')"
        샘플프로파일링 = " B.데이터타입 NOT IN ('BLOB', 'RAW', 'CLOB', 'NCLOB', 'LONG', 'NBLOB')" 
                          #" B.데이터타입 IN ('CHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR', 'NVARCHAR2', 'NCHAR', '')" 
        데이터값프로파일링 = """ B.데이터타입 IN ('CHAR', 'VARCHAR', 'VARCHAR2', 'NVARCHAR', 'NVARCHAR2', 'NCHAR') 
        AND B.데이터값조사대상여부 = 'Y'"""
        증분여부로직 = """AND (A.DB번호, A.스키마명, A.테이블명, B.컬럼명) NOT IN (SELECT D.DB번호, D.스키마명, D.테이블명, E.컬럼명
                                                                                             FROM 초기테이블정보 D, 초기컬럼정보 E
                                                                                            WHERE D.DB번호 = E.DB번호 
                                                                                              AND D.스키마명 = E.스키마명
                                                                                              AND D.테이블명 = E.테이블명)"""
        
        증분여부로직 = 증분여부로직 if 증분여부 == 'Y' else ""
            
        
        if target=='컬럼프로파일링':
            SQL = ProfilingTargetingSQL.format(대상DB = targetDB, 데이터타입지정조건문 = 컬럼프로파일링, 증분여부로직 = 증분여부로직)
        elif target=='샘플프로파일링':
            SQL = ProfilingTargetingSQL.format(대상DB = targetDB, 데이터타입지정조건문 = 샘플프로파일링, 증분여부로직 = 증분여부로직)
        elif target=='데이터값프로파일링':
            SQL = ProfilingTargetingSQL.format(대상DB = targetDB, 데이터타입지정조건문 = 데이터값프로파일링, 증분여부로직 = 증분여부로직)

        schema_info_df = pd.read_sql(sql=SQL, con=self.dqconn)
        schema_info_df[['DB번호', '컬럼순서', '길이', '소수점길이', '테이블그룹핑번호']] = schema_info_df[['DB번호', '컬럼순서', '길이', '소수점길이', '테이블그룹핑번호']].fillna(0).astype(np.int32)
        self.schema_info_df = schema_info_df
        self.schema_info_df_idx01 = schema_info_df.set_index(['DB번호', '스키마명', '테이블명', '테이블그룹핑번호'])
        self.table_info_df = schema_info_df[['DB번호', '스키마명', '테이블명', '테이블그룹핑번호']].drop_duplicates()
        
    
    def ProfilingSQLOrdering(self):    
        ExecuteOrderingSQL = """MERGE INTO 프로파일링SQL A
                                     USING (SELECT DB번호, 스키마명, 테이블명, 테이블사이즈
                                                 , ROW_NUMBER() OVER(PARTITION BY DB번호 ORDER BY 테이블사이즈) AS 실행순서
                                              FROM 테이블정보
                                             WHERE DB번호 = ?) B
                                        ON (A.DB번호 = B.DB번호 AND A.대상스키마명 = B.스키마명 AND A.대상테이블명 = B.테이블명)
                                      WHEN MATCHED THEN
                                    UPDATE SET A.실행순서 = B.실행순서"""
        
        cur = self.dqconn.cursor()
        for DB번호 in self.targetDB:
            cur.execute(ExecuteOrderingSQL, DB번호)
        cur.execute('commit')  
        
    def ProfilingSQLHistoryRecord(self, SQLType):
        insert_sql = """INSERT INTO 프로파일링SQL생성내역 (DB번호, 프로파일링SQL유형번호, 생성일시)
                         Values (?, ?, SYSDATE)"""
        cur = self.dqconn.cursor()
        for DB번호 in self.targetDB:
            try:
                cur.execute(insert_sql, int(DB번호), SQLType)
            except:
                cur.execute('Rollback')
                assert False, 'DB {DB번호} : 해당 SQL은 이미 생성되었습니다. 프로파일링 관리자와 상담하세요.'.format(DB번호=DB번호)
                
        cur.execute('Commit')
    
    def ProfilingSQLCreation(self, target):
        if target=='컬럼프로파일링':
            self.ProfilingSQLHistoryRecord(1)
            self.ColumnProfilingSQLCreation()
        elif target=='샘플프로파일링':
            self.ProfilingSQLHistoryRecord(2)
            self.DataProfilingSQLCreation(estimate = 'Sample')
        elif target=='데이터값프로파일링':
            self.ProfilingSQLHistoryRecord(3)
            self.DataProfilingSQLCreation(estimate = 'Full')
        
        self.ProfilingSQLOrdering()
        

    def DataProfilingSQLCreation(self, estimate = 'Sample'):
        warnings.filterwarnings('ignore')
        profilingTableInsertSQL = """INSERT INTO 프로파일링SQL (프로파일링SQL번호, 프로파일링SQL유형번호, 프로파일링실행상태, DB번호, 프로파일링SQL정보, 대상스키마명, 대상테이블명)
                                     VALUES (?, ?, '대기', ?, ?, ?, ?) """
        cur = self.dqconn.cursor()
        
        if estimate == 'Sample':
            condition = 'WHERE ROWNUM <= 100000'
            targettable = '샘플데이터수집정보'
            프로파일링SQL유형번호 = 2
        elif estimate == 'Full':
            condition = ''
            targettable = '데이터값수집정보'
            프로파일링SQL유형번호 = 3
        
        ##테이블별 Loop 실행
        for idx, DB번호, 스키마명, 테이블명, 테이블그룹핑번호  in tqdm(self.table_info_df[['DB번호','스키마명','테이블명', '테이블그룹핑번호']].drop_duplicates().to_records(), desc='{estimate} 데이터프로파일링 SQL 생성중'.format(estimate=estimate)):
            column_list = []
            decode_column_name = "DECODE(B.NO, "
            str_decode_column_value = "DECODE(B.NO, 99, Null, "
            num_decode_column_value = "DECODE(B.NO, 99, TO_NUMBER(Null), "
            date_decode_column_value = "DECODE(B.NO, 99, TO_DATE(Null, 'YYYYMMDD'), "
            ColumnCount = 0
            
            ##컬럼별 Loop 실행
            for _, _, _, _, 테이블코멘트, 컬럼명, 컬럼코멘트, 데이터타입, 컬럼순서, NULL여부, PK여부, 길이, 소수점길이 in self.schema_info_df_idx01.loc[DB번호, 스키마명, 테이블명, 테이블그룹핑번호].to_records():
                ColumnCount += 1
                컬럼명cp = '"{컬럼명}"'.format(컬럼명=컬럼명)
                column_list.append(컬럼명cp)

                decode_column_name = decode_column_name + str(ColumnCount) + ", '" + 컬럼명 + "', "
                
                if 데이터타입 == 'VARCHAR' or 데이터타입 == 'CHAR' or 데이터타입 == 'VARCHAR2' or 데이터타입 == 'NVARCHAR2' or 데이터타입 == 'NVARCHAR' or 데이터타입 == 'NCHAR':
                    str_decode_column_value = str_decode_column_value + str(ColumnCount) + ', A."' + 컬럼명 + '", '
                elif 데이터타입 == 'NUMBER' or 데이터타입 == 'FLOAT':
                    num_decode_column_value = num_decode_column_value + str(ColumnCount) + ', A."' + 컬럼명 + '", '
                elif 데이터타입 == 'DATE' or 데이터타입[:9] == 'TIMESTAMP':
                    date_decode_column_value = date_decode_column_value + str(ColumnCount) + ', A."' + 컬럼명 + '", '
                    

            decode_column_name = decode_column_name[0:-2] + ")"
            str_decode_column_value = str_decode_column_value[0:-2] + ")"
            num_decode_column_value = num_decode_column_value[0:-2] + ")"
            date_decode_column_value = date_decode_column_value[0:-2] + ")"
                
            sql_text = "SELECT " + ", ".join(column_list) + ", COUNT(*) AS 발생건수 \n        FROM " + 스키마명 + "." + 테이블명 + " \n" \
            + " {condition} ".format(condition=condition) \
            + "       GROUP BY " + ", ".join(column_list)

            sql_text = """ SELECT C.DB번호, C.스키마명, C.테이블명, C.컬럼명
                                , SUBSTRB(C.문자형데이터값, 1, 4000) 문자형데이터값
                                , C.숫자형데이터값
                                , C.날짜형데이터값
                                , C.출현건수
                                , R_NUM AS 출현순위
                             FROM (SELECT {DB번호} as DB번호
                                    , '{스키마명}' as 스키마명
                                    , '{테이블명}' as 테이블명
                                    , {DECODE컬럼명} as 컬럼명
                                    , {문자형_DECODE컬럼값} AS 문자형데이터값
                                    , {숫자형_DECODE컬럼값} AS 숫자형데이터값
                                    , {날짜형_DECODE컬럼값} AS 날짜형데이터값
                                    , SUM(발생건수) as 출현건수
                                    , ROW_NUMBER() OVER(PARTITION BY {DECODE컬럼명}
                                    					ORDER BY SUM(발생건수) DESC) AS R_NUM
                                  FROM ({sql_text} ) A
                                     , (SELECT ROWNUM AS NO FROM DUAL CONNECT BY ROWNUM <= {NO}) B
                                  GROUP BY B.NO
                                         , {문자형_DECODE컬럼값}
                                         , {숫자형_DECODE컬럼값}
                                         , {날짜형_DECODE컬럼값}) C
                                WHERE C.R_NUM <= 10
            """.format(DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명, sql_text=sql_text, NO=str(ColumnCount), \
                       DECODE컬럼명=decode_column_name, 문자형_DECODE컬럼값=str_decode_column_value, \
                       숫자형_DECODE컬럼값=num_decode_column_value, 날짜형_DECODE컬럼값=date_decode_column_value)

            info = dict()
            info['dbno'] = str(DB번호)
            info['schema'] = 스키마명
            info['table'] = 테이블명
            info['column'] = column_list
            info['targettable'] = targettable
            
            프로파일링SQL번호 = self.ProfilingSQLSeqCreation()
            cur.execute(profilingTableInsertSQL, 프로파일링SQL번호, 프로파일링SQL유형번호, int(DB번호), json.dumps(info), 스키마명, 테이블명)
            self.SQLSplitInsert(프로파일링SQL번호, int(DB번호), sql_text)

        cur.execute('commit')     
        
    def ProfilingSQLSeqCreation(self):
        sql = """select SEQ_프로파일링SQL.NEXTVAL from dual"""
        cur = self.dqconn.cursor()
        cur.execute(sql)
        seq, = cur.fetchone()
        return seq


    def ColumnProfilingSQLCreation(self):
        warnings.filterwarnings('ignore')
        profilingTableInsertSQL = """INSERT INTO 프로파일링SQL (프로파일링SQL번호, 프로파일링SQL유형번호, 프로파일링실행상태, DB번호, 프로파일링SQL정보, 대상스키마명, 대상테이블명)
                                     VALUES (?, 1, '대기', ?, ?, ?, ?) """        

        cur = self.dqconn2.cursor()
        
        ### 테이블 레벨 작업
        for idx, DB번호, 스키마명, 테이블명, 테이블그룹핑번호 in tqdm(self.table_info_df.to_records(), desc='컬럼프로파일링 SQL 생성중'):
            info = dict()
            decode_col_list = ['컬럼명', 'NOTNULL건수_', 'NULL건수_', 'UNIQUE건수_','최소길이_', '최대길이_', '최대BYTE길이_', '문자열최대값_', '문자열최소값_', '정수값길이_','소수점길이_', '숫자형최대값_', '숫자형최소값_', '날짜최대값_', '날짜최소값_']
            full_sql_text = "SELECT COUNT(*) 총건수 \n"
            decode_array = ['DECODE(B.NO, ' for _ in range(15)]
            
            ## 컬럼별로 반복작업
            column_list = []
            for _, _, _, _, 테이블코멘트, 컬럼명, 컬럼코멘트, 데이터타입, 컬럼순서, NULL여부, PK여부, 길이, 소수점길이 in self.schema_info_df_idx01.loc[DB번호, 스키마명, 테이블명, 테이블그룹핑번호].to_records():
                ### SQL 트래킹용 로그정보
                
                column_list.append(컬럼명)
                
                ## 전체 컬럼
                col_condition = ''
                col_idx = str(컬럼순서).rjust(2, "0")
                col_condition += ' ,  COUNT("'+ 컬럼명 +'") AS NOTNULL건수_' + col_idx + '\n' #1
                col_condition += ' ,  COUNT(*) - COUNT("'+ 컬럼명 +'") AS NULL건수_' + col_idx + '\n' #2
                col_condition += ' ,  COUNT(DISTINCT "'+ 컬럼명 +'") AS UNIQUE건수_' + col_idx + '\n' #3

                decode_array[0] += str(컬럼순서) + ", '" + 컬럼명 + "', "
                
                for i in range(1, 4):
                    decode_array[i] += str(컬럼순서) + ', ' + decode_col_list[i] + col_idx + ', '

                ## 문자열 컬럼
                if 데이터타입 == 'VARCHAR' or 데이터타입 == 'CHAR' or 데이터타입 == 'VARCHAR2' or 데이터타입 == 'NVARCHAR2' or 데이터타입 == 'NVARCHAR' or 데이터타입 == 'NCHAR':
                    col_condition += ' ,  MIN(LENGTH("'+ 컬럼명 +'")) AS 최소길이_' + col_idx + '\n' #4
                    col_condition += ' ,  MAX(LENGTH("'+ 컬럼명 +'")) AS 최대길이_' + col_idx + '\n' #5
                    col_condition += ' ,  MAX(LENGTHB("'+ 컬럼명 +'")) AS 최대BYTE길이_' + col_idx + '\n' #6
                    col_condition += ' ,  SUBSTRB(MAX("'+ 컬럼명 +'"), 1, 4000) AS 문자열최대값_' + col_idx + '\n' #7
                    col_condition += ' ,  SUBSTRB(MIN("'+ 컬럼명 +'"), 1, 4000) AS 문자열최소값_' + col_idx + '\n' #8

                    for i in range(4, 9): #4,5,6,7,8
                        decode_array[i] += str(컬럼순서) + ', ' + decode_col_list[i] + col_idx + ', '

                ## 숫자열 컬럼
                elif 데이터타입 == 'NUMBER' or 데이터타입 == 'FLOAT':
                    col_condition += ' ,  MAX(LENGTH(TRUNC(TO_CHAR("'+컬럼명+'")))) AS 정수값길이_' + col_idx + '\n' #9
                    col_condition += ' ,  MAX(LENGTH(REPLACE(TO_CHAR("'+컬럼명+'" -  TRUNC("'+컬럼명+"\")), '.'))) AS 소수점길이_" + col_idx + '\n' #10
                    col_condition += ' ,  MAX('+컬럼명+") AS 숫자형최대값_" + col_idx + '\n' #12
                    col_condition += ' ,  MIN('+컬럼명+") AS 숫자형최소값_" + col_idx + '\n' #13

                    for i in range(9, 13): #
                        decode_array[i] += str(컬럼순서) + ', ' + decode_col_list[i] + col_idx + ', '

                ## 날짜형 컬럼
                elif 데이터타입 == 'DATE' or 데이터타입[:9] == 'TIMESTAMP'  :
                    col_condition += " ,  LEAST(MAX(\""+ 컬럼명 +"\"), TO_DATE('9999/12/31', 'YYYY/MM/DD')) AS 날짜최대값_" + col_idx + '\n' #13
                    col_condition += " ,  GREATEST(MIN(\""+ 컬럼명 +"\"), TO_DATE('0001/01/01', 'YYYY/MM/DD')) AS 날짜최소값_" + col_idx + '\n' #14

                    for i in range(13, 15):
                        decode_array[i] += str(컬럼순서) + ', ' + decode_col_list[i] + col_idx + ', '

                full_sql_text += col_condition
            
            info['dbno'] = str(DB번호)
            info['schema'] = 스키마명
            info['table'] = 테이블명            
            info['column'] = column_list
            info['targettable'] = '컬럼분석정보'
            
            ## WHERE 절 생성
            full_sql_text = "(" + full_sql_text + 'from ' + 스키마명 + '.' + 테이블명 + " WHERE ROWNUM <= 100000) A \n"
            full_sql_text = full_sql_text + ", (SELECT ROWNUM-1 NO FROM DUAL CONNECT BY ROWNUM <= {NO}) B".format(NO=str(컬럼순서+1))

            ## DECODE문 생성
            decode_full_syntax = ''
            for i, decode_syntax in enumerate(decode_array):
                if i == 1:
                    decode_full_syntax += '총건수, \n '

                if len(decode_syntax) == 13:
                    decode_syntax = decode_syntax + '99999, Null)'
                else:
                    decode_syntax = decode_syntax[:-2] + ")"
                decode_full_syntax += decode_syntax + ', \n '

            decode_full_syntax = decode_full_syntax[:-4]

            ## 최종 SQL 생성
            final_sql = "SELECT " + str(DB번호) + " AS DB번호, '" + 스키마명 + "' AS 스키마명, '" + 테이블명 + "' AS 테이블명, \n "+ decode_full_syntax + ' \n from ' + full_sql_text
            
            프로파일링SQL번호 = self.ProfilingSQLSeqCreation()
            self.vrfy = (프로파일링SQL번호, final_sql, info)
            cur.execute(profilingTableInsertSQL, 프로파일링SQL번호, int(DB번호), json.dumps(info), 스키마명, 테이블명) #, 
            self.SQLSplitInsert(프로파일링SQL번호, int(DB번호), final_sql)
        cur.execute('commit')
        
    def SQLSplitInsert(self, 프로파일링SQL번호, DB번호, SQL): 
        cur = self.dqconn.cursor()  #, Tibero
        point = 0
        size = 1000
        
        split_cnt = math.ceil(len(SQL) / size)
        sql_list = []
        
        for i in range(split_cnt):
            sql_list.append([프로파일링SQL번호, i, SQL[point:point+size]])
            point += size
            
        insert_sql = """INSERT INTO 프로파일링SQL분할 (프로파일링SQL번호, SQL순서, 프로파일링SQL)        
                        values (?, ?, ?)"""
        
        #cur.executemany(insert_sql, sql_list)
        
        for sql_line in sql_list:
            cur.execute(insert_sql, sql_line)
        
        cur.execute('commit')
        
    def ProfilingSQLCleansing(self):
        """ 작성 중! 사용하지 마세요!"""
        sql = "SELECT 프로파일링SQL번호, 프로파일링SQL정보 FROM 프로파일링SQL"
        cur = self.dqconn.cursor()
        cur.execute(sql)
        result = cur.fetchall()

        sql_info_list = []

        for 프로파일링SQL번호, 프로파일링SQL정보 in result:
            sql_info = json.loads(프로파일링SQL정보)

            for column in sql_info['column']:
                row_data = copy(sql_info)
                row_data['profilingSQLNo'] = 프로파일링SQL번호
                row_data['column'] = column

                sql_info_list.append(row_data)
                
        df = pd.DataFrame(columns=['profilingSQLNo', 'dbno', 'schema', 'table', 'column', 'targettable'])        
        df = df.append(sql_info_list, ignore_index=True)
        df['profilingSQLNo'] = df['profilingSQLNo'].astype(int)
        dup_check_df = df.groupby(['dbno' , 'schema', 'table', 'column', 'targettable']).count()
        
        
        
""" 프로파일링을 위해 DBMS 카탈로그 수집기 """
class PROFILINGCOLLECTOR(PROFILINGEXECUTER):
    def __init__(self):
        super().__init__()
        
    def TargetDBCollect(self):
        """딕셔너리를 수집할 대상 DB의 스키마 정보 수집 여부를 확인함"""
        for idx, DB번호, DBMS접속방식, 접속문자열, DB명 in self.ProfilingDbTarget[['DB번호', 'DBMS접속방식', '접속문자열', 'DB명']].to_records():
            #try:
            cur = self.dqconn.cursor()
            cur.execute("SELECT 스키마정보수집여부 FROM DB정보 WHERE DB번호 = ?", int(DB번호))
            collect_yn, = cur.fetchone()
            ## 수집완료일경우 False를 전달
            collect_yn = False if collect_yn == 'Y' else True
            assert collect_yn, 'DB번호:{0} |DB명:{1} | 데이터 수집된 데이터베이스'.format(DB번호, DB명)
            print('DB번호:{0} |DB명:{1} | 데이터 수집 시작'.format(DB번호, DB명))
            self.CatalogCollector(DB번호, DBMS접속방식, 접속문자열)
            #except AssertionError:
#                print('DB번호:{0} |DB명:{1} | 데이터 수집된 데이터베이스'.format(DB번호, DB명))
#            except:
#                print('DB번호:{0} |DB명:{1} | 데이터 수집오류 발생'.format(DB번호, DB명))
#                _, exc_obj, _ = sys.exc_info()
#                error_message = str(exc_obj).replace("'","").replace('"','')
#                print(error_message)
        
    def CatalogCollector(self, DB번호, DBMS접속방식, 접속문자열):
        if DBMS접속방식 == 'ODBC':
            conn = pyodbc.connect(접속문자열)
        if DBMS접속방식 == 'OracleClient':
            conn = cx_Oracle.connect(접속문자열)
        
        ##### 리포지터리에서 제외대상 스키마를 추출
        cur = self.dqconn.cursor()
        cur.execute("SELECT 제외대상스키마명 FROM 제외대상스키마정보")
        result = cur.fetchall()
        schema_list = "', '".join([row[0] for row in result])
        
        
        
        ##### 기존 정보 삭제
        #self.dqconn.execute("delete 스키마정보 where DB번호 = ?", int(DB번호))
        #self.dqconn.execute("delete 테이블정보 where DB번호 = ?", int(DB번호))
        #self.dqconn.execute("delete 컬럼정보 where DB번호 = ?", int(DB번호))
        #self.dqconn.execute("Commit")


        ##### 스키마 정보 조회
        SchemaCollectorSQL = """SELECT {DB번호}, USERNAME, 'Y' FROM ALL_USERS WHERE USERNAME NOT IN ('{제외대상}') """.format(제외대상 = schema_list, DB번호=str(DB번호))
        cur = conn.cursor()
        cur.execute(SchemaCollectorSQL)
        result = cur.fetchall()
        self.schema_result = result
        
        ##### 스키마 정보 INSERT
        schema_catalog_insert_sql = """INSERT INTO 스키마정보 (DB번호, 스키마명, 수집대상여부)
                                        VALUES (?, ?, ?)"""
        
        cur = self.dqconn.cursor()
        for row in tqdm(result, desc='스키마 정보 수집중'):
            cur.execute(schema_catalog_insert_sql, row)
        cur.execute('commit')
        cur.close()           
        
        ##### 테이블 정보 조회
        """ 스키마명, 테이블명, 테이블코멘트"""
        SegmentViewName = 'ALL_SEGMENTS' if DBMS접속방식 == 'ODBC' else 'DBA_SEGMENTS'
        TableCollectorSQL = """SELECT {DB번호}, A.OWNER, A.TABLE_NAME, B.COMMENTS, NVL(C.MBYTES, 0)
                                FROM ALL_TABLES A
                                   , ALL_TAB_COMMENTS B
                                   , (SELECT OWNER, SEGMENT_NAME, SUM(BYTES)/1024/1024 AS MBYTES
										FROM {SegmentViewName}
									   GROUP BY OWNER, SEGMENT_NAME) C
                                WHERE A.OWNER = B.OWNER
                                AND A.TABLE_NAME = B.TABLE_NAME
                                AND A.OWNER = C.OWNER(+)
                                AND A.TABLE_NAME = C.SEGMENT_NAME(+)
                                AND A.OWNER NOT IN ('{제외대상}')
                                """.format(제외대상 = schema_list, DB번호= str(DB번호), SegmentViewName=SegmentViewName)
        
        cur = conn.cursor()
        cur.execute(TableCollectorSQL)
        result = cur.fetchall()
        self.table_result = result
        cur.close()
        
        ##### 테이블 정보 INSERT
        table_catalog_insert_sql = """INSERT INTO 테이블정보 (DB번호, 스키마명, 테이블명, 테이블코멘트, 테이블사이즈)
                                      VALUES (?, ?, ?, ?, ?)"""
        
        cur = self.dqconn.cursor()
        for row in tqdm(result, desc='테이블 정보 수집중'):
            cur.execute(table_catalog_insert_sql, row)
        cur.execute('commit')
        cur.close()

        ##### 컬럼 정보 조회
        """ 컬럼 정보 수집"""
        ColumnCollectorSQL = """SELECT {DB번호}, D.OWNER, D.TABLE_NAME, D.COLUMN_NAME, D.COMMENTS, D.DATA_TYPE, D.COLUMN_ID, D.NULLABLE, DECODE(E.OWNER, NULL, NULL, 'Y') AS PK_YN, D.DATA_LENGTH, D.DATA_SCALE
                                  FROM (SELECT A.OWNER, A.TABLE_NAME, A.COLUMN_NAME, B.COMMENTS, A.DATA_TYPE, A.COLUMN_ID, A.NULLABLE,
                                               CASE WHEN A.DATA_PRECISION > 0 THEN A.DATA_PRECISION
                                                    ELSE A.DATA_LENGTH END AS DATA_LENGTH, A.DATA_SCALE, C.CONSTRAINT_NAME
                                          FROM  ALL_TAB_COLUMNS A
                                             , ALL_COL_COMMENTS B
                                             , ALL_CONSTRAINTS C
                                         WHERE A.OWNER = B.OWNER
                                           AND A.TABLE_NAME = B.TABLE_NAME
                                           AND A.COLUMN_NAME = B.COLUMN_NAME
                                           AND A.OWNER = C.OWNER(+)
                                           AND A.TABLE_NAME = C.TABLE_NAME(+)
                                           AND C.CONSTRAINT_TYPE(+) = 'P'
                                           AND A.OWNER NOT IN ('{제외대상}')
                                           AND NOT EXISTS (SELECT 'X' 
                                                             FROM ALL_VIEWS X
                                                             WHERE A.OWNER = X.OWNER
                                                               AND A.TABLE_NAME = X.VIEW_NAME)  ) D
                                     , ALL_CONS_COLUMNS E
                                  WHERE D.OWNER = E.OWNER(+)
                                    AND D.TABLE_NAME = E.TABLE_NAME(+)
                                    AND D.COLUMN_NAME = E.COLUMN_NAME(+)
                                    AND D.CONSTRAINT_NAME = E.CONSTRAINT_NAME(+)
                                  ORDER BY D.OWNER, D.TABLE_NAME, D.COLUMN_ID""".format(제외대상 = schema_list, DB번호= str(DB번호))
        
        cur = conn.cursor()
        cur.execute(ColumnCollectorSQL)
        result = cur.fetchall()
        self.column_result = result
        cur.close()
        
        ##### 컬럼 정보 INSERT
        column_catalog_insert_sql = """INSERT INTO 컬럼정보 (DB번호, 스키마명, 테이블명, 컬럼명, 컬럼코멘트, 데이터타입, 컬럼순서, NULL여부, PK여부, 길이, 소수점길이)
                                                  VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) """
        
        cur = self.dqconn.cursor()
        for row in tqdm(result, desc='컬럼 정보 수집중'):
            cur.execute(column_catalog_insert_sql, row)
        
        
        ### 스키마정보수집여부 상태 Y로 변경
        collect_status_update_sql = """UPDATE DB정보
                                          SET 스키마정보수집여부 = 'Y'
                                        WHERE DB번호 = ?"""
        cur.execute(collect_status_update_sql, int(DB번호))

        cur.execute('commit')
        cur.close()
