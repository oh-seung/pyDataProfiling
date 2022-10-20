import sys
from threading import Thread
import numpy as np


class ProfilingExecute:
    """ 프로파일링 실행 클래스 """
    def ProfilingDBInfoGet(self, ProfilingDBInfo):
        self.ProfilingDBInfo = ProfilingDBInfo
        self.DB번호 = ProfilingDBInfo['DB번호']

    def RepositoryDBInfoGet(self, RepositoryDBInfo):
        self.RepositoryDBInfo = RepositoryDBInfo
        self.RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])
import sys
from threading import Thread
import numpy as np


class ProfilingExecute:
    """ 프로파일링 실행 클래스 """
    def ProfilingDBInfoGet(self, ProfilingDBInfo):
        self.ProfilingDBInfo = ProfilingDBInfo
        self.DB번호 = ProfilingDBInfo['DB번호']

    def RepositoryDBInfoGet(self, RepositoryDBInfo):
        self.RepositoryDBInfo = RepositoryDBInfo
        self.RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])


    
    def sqltypeCall(self):
        """ 적재 SQL INSERT 구조를 생성하는 문구"""
        sql = """SELECT SQL유형번호, 적재테이블명, 적재컬럼목록
        FROM 프로파일링_SQL유형 """
        RepCur = self.RepConn.cursor()
        RepCur.execute(sql)
        _result = RepCur.fetchall()

        result = dict()
        for sql유형번호, 적재테이블명, 적재컬럼목록 in _result:
            적재바인딩문구 = ", ".join(['?' for _ in 적재컬럼목록.replace(" ", '').split(',')])
            sql = """INSERT INTO {적재테이블명} ({적재컬럼목록})
            values ({적재바인딩문구})""".format(적재테이블명=적재테이블명, 적재컬럼목록=적재컬럼목록, 적재바인딩문구=적재바인딩문구)
            _row_dict = {'적재테이블명':적재테이블명, '적재컬럼목록':적재컬럼목록, '적재바인딩문구':적재바인딩문구, 'SQL':sql}
            result[int(sql유형번호)] = _row_dict
        
        self.InsertSqls = result

    
    def _execute_status_check(self, DB번호):
        """ 수행에 대한 체크 """
        RepCur = self.RepConn.cursor()
        
        CheckInfo = dict()
        #1) 수행시킬 SQL이 있는지 체크, 잔여개수로 리턴한다.
        sql = """SELECT COUNT(*) CNT
                   FROM 프로파일링_SQL
                   WHERE DB번호 = ?
                   AND 실행상태 = '대기'
                   AND ROWNUM = 1
                   """
        
        RepCur.execute(sql, DB번호)
        CheckInfo['잔여건수'], = RepCur.fetchone()
        
        CheckResult = False if CheckInfo['잔여건수'] == 0 else True
            
        return CheckResult, CheckInfo

    def execute(self, **kwargs):
        ### 몇개 실행할지 정함
        degree = kwargs['degree'] if 'degree' in kwargs.keys() else 1
        duplicate = kwargs['duplicate'] if 'duplicate' in kwargs.keys() else 'pass'
        
        self.sqltypeCall()

        ts, ProfConns, RepConns = [], [], []
        for ProcessNo in range(degree):
            
            ProfConn = self.ProfilingDBInfo['Connector'](self.ProfilingDBInfo['ConnectionString'])
            RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])

            t = Thread(target=self._execute, args=(ProcessNo, self.DB번호, RepConn, ProfConn, duplicate))

            t.start()
            ts.append(t)
            ProfConns.append(ProfConn)
            RepConns.append(RepConn)            
        
        for t in ts:
            t.join()

        for ProcessNo in range(degree):
            try:
                ProfConns[ProcessNo].close()
            except:
                pass
            try:
                RepConns[ProcessNo].close()
            except:
                pass



    def _execute(self, ProcessNo, DB번호, RepConn, ProfConn, duplicate):
        ## 실행순서 (orderby) 1) BigSize, 
        """ 1) 프로파일링 할 대상을 확인
            2) 실행상태 변경
            3) 데이터 처리 성공여부 확인
            4) 데이터 처리에 성공한 경우, 프로파일링할 SQL 생성
        """
        ### 데이터 중복 발생시 처리 방법 1) merge(삭제하고 insert), pass(오류로 처리하기)
        

        while True:
            CheckResult, _ = self._execute_status_check(DB번호)
             
            if not CheckResult:
                break 
            
            try:
                RepCur = RepConn.cursor()
                # 1) 프로파일링 할 대상을 지정
                sql = """SELECT 프로파일링SQL번호, DB번호, 스키마명, 테이블명, SQL유형번호, 수집컬럼목록
                            FROM 프로파일링_SQL
                            WHERE 실행상태 = '대기' 
                                AND DB번호 = ?
                            ORDER BY 실행순서
                            FETCH FIRST 1 ROWS ONLY"""

                RepCur.execute(sql, DB번호)
                프로파일링SQL번호, DB번호, 스키마명, 테이블명, SQL유형번호, 수집컬럼목록 = RepCur.fetchone()

                # 2) 실행상태 변경
                sql = """UPDATE 프로파일링_SQL
                                SET 실행상태 = '실행', 실행시작일시 = SYSDATE
                            WHERE 프로파일링SQL번호 = ?
                                AND 실행상태 = '대기' """

                #txnId = str(uuid.uuid1()).upper(), 처리트랜잭션아이디 = ?
                RepCur.execute(sql, 프로파일링SQL번호)

                # 3) 데이터 처리 성공여부 확인
                TxnSucess = True if RepCur.rowcount > 0 else False

                # 4) 데이터 처리에 성공하면, 프로파일링할 SQL을 만듬
                if not TxnSucess:
                    RepCur.execute('rollback')

                else:
                    # 5) 프로파일링 대상 DB에 실행할 SQL을 생성
                    print('프로세스번호:{ProcessNo} SQL번호:{프로파일링SQL번호} DB번호:{DB번호} 스키마명:{스키마명}, 테이블명:{테이블명}'.format(ProcessNo=ProcessNo, 프로파일링SQL번호=프로파일링SQL번호, DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명))
                    RepCur.execute('commit')
                    sql = """SELECT SQL
                                FROM DTWAREADM.프로파일링_SQL분할
                                WHERE 프로파일링sql번호 = ?
                                ORDER BY 순서"""

                    RepCur.execute(sql, 프로파일링SQL번호)
                    execsql = "".join([RepCur.fetchone()[0] for _ in range(RepCur.rowcount)])

                    # 5) 프로파일링 SQL 실행
                    ProfCur = ProfConn.cursor()
                    ProfCur.execute(execsql)
                    ProfilingData = ProfCur.fetchall()

                    # 6) 리포지터리DB에 데이터 적재
                    if duplicate == 'merge':
                        duplicateCleansingDataset = [[DB번호, 스키마명, 테이블명, ColumnName] for ColumnName in 수집컬럼목록.split(",")]
                        InsertTableName = self.InsertSqls[SQL유형번호]['적재테이블명']
                        Deletesql = """DELETE {테이블명}
                                        WHERE DB번호 =?
                                            AND 스키마명 = ?
                                            AND 테이블명 = ?
                                            AND 컬럼명 = ?""".format(테이블명=InsertTableName)
                        RepCur.executemany(Deletesql, duplicateCleansingDataset)
                        
                    
                    # ) 데이터 적재
                    insertsql = self.InsertSqls[SQL유형번호]['SQL']
                    RepCur.executemany(insertsql, ProfilingData) if len(ProfilingData) > 0 else None

                    sql = """UPDATE 프로파일링_SQL
                                SET 실행상태 = '종료', 실행종료일시 = SYSDATE             
                            WHERE 프로파일링SQL번호 = ?
                                AND 실행상태 = '실행'"""
                    RepCur.execute(sql, 프로파일링SQL번호)
                    RepCur.execute('commit')

                RepCur.close()
            except:
                ######################################################################################
                ######### 오류 발생시 접속 종료시키기 재접속
                try:
                    RepConn.close()
                except:
                    pass
                
                try:
                    ProfConn.close()
                except:
                    pass
                
                RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])
                ProfConn = self.ProfilingDBInfo['Connector'](self.ProfilingDBInfo['ConnectionString'])
                ######################################################################################

                _, val, _ = sys.exc_info()
                ErrorText = """ ### 오류 발생
    - 프로세스번호:{ProcessNo}
    - SQL번호:{프로파일링SQL번호}
    - 오류내용 : {오류내용}  """
                print(ErrorText.format(ProcessNo=ProcessNo, 프로파일링SQL번호=프로파일링SQL번호, 오류내용=val))
                sql = """UPDATE 프로파일링_SQL
                            SET 실행상태 = '오류', 실행종료일시 = SYSDATE, 실행오류내용 = ?
                          WHERE 프로파일링SQL번호 = ?
                             AND 실행상태 = '실행'"""
                RepCur = RepConn.cursor()
                RepCur.execute(sql, str(val), 프로파일링SQL번호)
                RepCur.execute('commit')
                RepCur.close()

    
    def sqltypeCall(self):
        """ 적재 SQL INSERT 구조를 생성하는 문구"""
        sql = """SELECT SQL유형번호, 적재테이블명, 적재컬럼목록
        FROM 프로파일링SQL유형 """
        RepCur = self.RepConn.cursor()
        RepCur.execute(sql)
        _result = RepCur.fetchall()

        result = dict()
        for sql유형번호, 적재테이블명, 적재컬럼목록 in _result:
            적재바인딩문구 = ", ".join(['?' for _ in 적재컬럼목록.replace(" ", '').split(',')])
            sql = """INSERT INTO {적재테이블명} ({적재컬럼목록})
            values ({적재바인딩문구})""".format(적재테이블명=적재테이블명, 적재컬럼목록=적재컬럼목록, 적재바인딩문구=적재바인딩문구)
            _row_dict = {'적재테이블명':적재테이블명, '적재컬럼목록':적재컬럼목록, '적재바인딩문구':적재바인딩문구, 'SQL':sql}
            result[int(sql유형번호)] = _row_dict
        
        self.InsertSqls = result

    
    def _execute_status_check(self, DB번호):
        """ 수행에 대한 체크 """
        RepCur = self.RepConn.cursor()
        
        CheckInfo = dict()
        #1) 수행시킬 SQL이 있는지 체크, 잔여개수로 리턴한다.
        sql = """SELECT COUNT(*) CNT
                   FROM 프로파일링SQL
                   WHERE DB번호 = ?
                   AND 실행상태 = '대기'
                   AND ROWNUM = 1
                   """
        
        RepCur.execute(sql, DB번호)
        CheckInfo['잔여건수'], = RepCur.fetchone()
        
        CheckResult = False if CheckInfo['잔여건수'] == 0 else True
            
        return CheckResult, CheckInfo

    def execute(self, **kwargs):
        ### 몇개 실행할지 정함
        degree = kwargs['degree'] if 'degree' in kwargs.keys() else 1
        duplicate = kwargs['duplicate'] if 'duplicate' in kwargs.keys() else 'pass'
        
        self.sqltypeCall()

        ts, ProfConns, RepConns = [], [], []
        for ProcessNo in range(degree):
            
            ProfConn = self.ProfilingDBInfo['Connector'](self.ProfilingDBInfo['ConnectionString'])
            RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])

            t = Thread(target=self._execute, args=(ProcessNo, self.DB번호, RepConn, ProfConn, duplicate))

            t.start()
            ts.append(t)
            ProfConns.append(ProfConn)
            RepConns.append(RepConn)            
        
        for t in ts:
            t.join()

        for ProcessNo in range(degree):
            try:
                ProfConns[ProcessNo].close()
            except:
                pass
            try:
                RepConns[ProcessNo].close()
            except:
                pass



    def _execute(self, ProcessNo, DB번호, RepConn, ProfConn, duplicate):
        ## 실행순서 (orderby) 1) BigSize, 
        """ 1) 프로파일링 할 대상을 확인
            2) 실행상태 변경
            3) 데이터 처리 성공여부 확인
            4) 데이터 처리에 성공한 경우, 프로파일링할 SQL 생성
        """
        ### 데이터 중복 발생시 처리 방법 1) merge(삭제하고 insert), pass(오류로 처리하기)
        

        while True:
            CheckResult, _ = self._execute_status_check(DB번호)
             
            if not CheckResult:
                break 
            
            try:
                RepCur = RepConn.cursor()
                # 1) 프로파일링 할 대상을 지정
                sql = """SELECT 프로파일링SQL번호, DB번호, 스키마명, 테이블명, SQL유형번호, 수집컬럼목록
                            FROM 프로파일링SQL
                            WHERE 실행상태 = '대기' 
                                AND DB번호 = ?
                            ORDER BY 실행순서
                            FETCH FIRST 1 ROWS ONLY"""

                RepCur.execute(sql, DB번호)
                프로파일링SQL번호, DB번호, 스키마명, 테이블명, SQL유형번호, 수집컬럼목록 = RepCur.fetchone()

                # 2) 실행상태 변경
                sql = """UPDATE 프로파일링SQL
                                SET 실행상태 = '실행', 실행시작일시 = SYSDATE
                            WHERE 프로파일링SQL번호 = ?
                                AND 실행상태 = '대기' """

                #txnId = str(uuid.uuid1()).upper(), 처리트랜잭션아이디 = ?
                RepCur.execute(sql, 프로파일링SQL번호)

                # 3) 데이터 처리 성공여부 확인
                TxnSucess = True if RepCur.rowcount > 0 else False

                # 4) 데이터 처리에 성공하면, 프로파일링할 SQL을 만듬
                if not TxnSucess:
                    RepCur.execute('rollback')

                else:
                    # 5) 프로파일링 대상 DB에 실행할 SQL을 생성
                    print('프로세스번호:{ProcessNo} SQL번호:{프로파일링SQL번호} DB번호:{DB번호} 스키마명:{스키마명}, 테이블명:{테이블명}'.format(ProcessNo=ProcessNo, 프로파일링SQL번호=프로파일링SQL번호, DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명))
                    RepCur.execute('commit')
                    sql = """SELECT SQL
                                FROM DTWAREADM.프로파일링SQL분할
                                WHERE 프로파일링sql번호 = ?
                                ORDER BY 순서"""

                    RepCur.execute(sql, 프로파일링SQL번호)
                    execsql = "".join([RepCur.fetchone()[0] for _ in range(RepCur.rowcount)])

                    # 5) 프로파일링 SQL 실행
                    ProfCur = ProfConn.cursor()
                    ProfCur.execute(execsql)
                    ProfilingData = ProfCur.fetchall()

                    # 6) 리포지터리DB에 데이터 적재
                    if duplicate == 'merge':
                        duplicateCleansingDataset = [[DB번호, 스키마명, 테이블명, ColumnName] for ColumnName in 수집컬럼목록.split(",")]
                        InsertTableName = self.InsertSqls[SQL유형번호]['적재테이블명']
                        Deletesql = """DELETE {테이블명}
                                        WHERE DB번호 =?
                                            AND 스키마명 = ?
                                            AND 테이블명 = ?
                                            AND 컬럼명 = ?""".format(테이블명=InsertTableName)
                        RepCur.executemany(Deletesql, duplicateCleansingDataset)
                        
                    
                    # ) 데이터 적재
                    insertsql = self.InsertSqls[SQL유형번호]['SQL']
                    RepCur.executemany(insertsql, ProfilingData) if len(ProfilingData) > 0 else None

                    sql = """UPDATE 프로파일링SQL
                                SET 실행상태 = '종료', 실행종료일시 = SYSDATE             
                            WHERE 프로파일링SQL번호 = ?
                                AND 실행상태 = '실행'"""
                    RepCur.execute(sql, 프로파일링SQL번호)
                    RepCur.execute('commit')

                RepCur.close()
            except:
                ######################################################################################
                ######### 오류 발생시 접속 종료시키기 재접속
                try:
                    RepConn.close()
                except:
                    pass
                
                try:
                    ProfConn.close()
                except:
                    pass
                
                RepConn = self.RepositoryDBInfo['Connector'](self.RepositoryDBInfo['ConnectionString'])
                ProfConn = self.ProfilingDBInfo['Connector'](self.ProfilingDBInfo['ConnectionString'])
                ######################################################################################

                _, val, _ = sys.exc_info()
                ErrorText = """ ### 오류 발생
    - 프로세스번호:{ProcessNo}
    - SQL번호:{프로파일링SQL번호}
    - 오류내용 : {오류내용}  """
                print(ErrorText.format(ProcessNo=ProcessNo, 프로파일링SQL번호=프로파일링SQL번호, 오류내용=val))
                sql = """UPDATE 프로파일링SQL
                            SET 실행상태 = '오류', 실행종료일시 = SYSDATE, 실행오류내용 = ?
                          WHERE 프로파일링SQL번호 = ?
                             AND 실행상태 = '실행'"""
                RepCur = RepConn.cursor()
                RepCur.execute(sql, str(val), 프로파일링SQL번호)
                RepCur.execute('commit')
                RepCur.close()
