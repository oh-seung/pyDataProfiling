import pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import uuid
import sys

class SQLCreateDictionarySelect:
    """ SQL 생성시 필요한 딕셔너리를 SELECT 함
        Repository가 file형식으로 변경될 때 해당클래스를 수정하여 사용
    """
    def _TableLisimport pandas as pd
import numpy as np
from tqdm.notebook import tqdm
import uuid
import sys

class SQLCreateDictionarySelect:
    """ SQL 생성시 필요한 딕셔너리를 SELECT 함
        Repository가 file형식으로 변경될 때 해당클래스를 수정하여 사용
    """
    def _TableListSelect(self, DB번호, schemaList):
        sql = """SELECT DB번호, 스키마명, 테이블명
                   FROM 프로파일링_테이블정보
                  WHERE DB번호 = :DB번호
                    AND 스키마명 IN ({schemaList}) """.format(schemaList=schemaList)
        self.TableList = pd.read_sql(sql, self.RepConn, params=[DB번호])   
    
    def _ColumnListSelect(self, DB번호, schemaList):
        includeColumnList = self.StringDatatype + self.NumericDatatype + self.DatetimeDatatype
        includeColumnList = "'" + "', '".join(includeColumnList) + "'"
        sql = """SELECT DB번호, 스키마명, 테이블명, 컬럼명, 데이터타입
                   FROM 프로파일링_컬럼정보
                  WHERE (DB번호, 스키마명, 테이블명) IN (SELECT DB번호, 스키마명, 테이블명
                                                           FROM 프로파일링_테이블정보
                                                          WHERE DB번호 = :DB번호
                                                            AND 스키마명 IN ({schemaList})  )
                    AND 데이터타입 IN ({includeColumnList})
                  ORDER BY DB번호, 스키마명, 테이블명, 컬럼순서
                    """.format(schemaList=schemaList, includeColumnList=includeColumnList)
        self.ColumnList = pd.read_sql(sql, self.RepConn, params=[DB번호]).astype({'DB번호':int}).set_index(['DB번호', '스키마명', '테이블명'])

        
        
    def DictionaryListSelect(self, DB번호, schemaList):
        schemaList = "'" + "', '".join(schemaList) + "'"
        self._TableListSelect(DB번호, schemaList)
        self._ColumnListSelect(DB번호, schemaList)
        
class SQLCreateRepository:
    """ SQL을 리포지터리에 저장하기 위한 클래스 """
    def ProfilingSQLTransform(self, sqls):
        """ 식별자값과 SQL값 분할"""
        _sqls = []
        sqlSplitLength = 1000 ### sql을 분할하고자 하는 값
        for sql in sqls:
            uuidValue = str(uuid.uuid1()).upper()
            sqlPart = [[uuidValue, i, sql[4][i*sqlSplitLength : (i+1)*sqlSplitLength]] for i in range(len(sql[4]) // sqlSplitLength + 1)]
            columns = ",".join(sql[3])
            sql = [uuidValue] + sql[0:3] + [columns] + [sqlPart]
            #sql.append(sqlPart)
            _sqls.append(sql)
        return _sqls
        
    
    def ProfilingSQLInsert(self, sqls, sqltype:int):
        InsertSql1 = """INSERT INTO 프로파일링_SQL (프로파일링SQL번호, DB번호, 스키마명, 테이블명, 수집컬럼목록, SQL유형번호, 실행상태)
        VALUES (?, ?, ?, ?, ?, ?, ?) """
        
        InsertSql2 = """INSERT INTO 프로파일링_SQL분할 (프로파일링SQL번호, 순서, SQL)
        VALUES (?, ?, ?) """        
        
        cur = self.RepConn.cursor()
        try:
            for sql in tqdm(sqls):
                cur.execute(InsertSql1, sql[0:5] + [sqltype, '대기'])
                cur.executemany(InsertSql2, sql[5])
            cur.execute('commit')
        except:
            cur.execute('rollback')
            _, val, _ = sys.exc_info()
            print(val)
            

class SQLCreate(SQLCreateDictionarySelect, SQLCreateRepository):
    """ 프로파일링 SQL을 생성하기 위한 클래스 """
    def __init__(self):
        self.TableList = pd.DataFrame(None, columns = ['DB번호', '스키마명', '테이블명'])
        self.ColumnList = pd.DataFrame(None, columns = ['DB번호', '스키마명', '테이블명', '컬럼명', '데이터타입'])
        
        self.StringDatatype = ['VARCHAR', 'VARCHAR2', 'CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', ]
        self.NumericDatatype = ['NUMBER', 'INT', 'DECIMAL', 'TINYINT', 'BIGINT', 'SMALLINT', 'FLOAT32', ]
        self.DatetimeDatatype = ['DATE', 'TIMESTAMP(6)', 'TIMESTAMP', 'TIMESTAMP(9)', 'TIMESTAMP(6) WITH TIME ZONE', ]
        self.ExceptionDatatype = ['RAW', 'CLOB', 'BLOB', 'LONG', 'ROWID', ]

    def RepositoryDBConnect(self, RepConn):
        """ 리포지터리 DB에 접속한다."""
        self.RepConn = RepConn
        
    def ColumnPartition(self, columncount, TableList, ColumnList):
        """ 프로파일링SQL을 생성할 때 한번에 수행할 수 있는 컬럼씩 그룹핑 한다.
            동시에 수행되는 컬럼 갯수가 적으면 테이블 반복 조회 횟수가 늘어나고,
            동시에 수행되는 컬럼 갯수가 많의면 한SQL의 집계횟수가 늘어 성능이 저하될 수 있다. """
        columnPartResult = []
        for DB번호, 스키마명, 테이블명 in TableList.values:
            ColumnData = ColumnList.loc[DB번호].loc[스키마명].loc[테이블명].values.reshape(-1, 2).tolist()

            ### 컬럼별로 그룹핑 만들기
            v, etc = divmod(len(ColumnData), columncount)
            v = v + 1 if etc > 0 else v
            TabColumnList = [ColumnData[i*columncount : (i+1)*columncount] for i in range(v)]
            columnPartResult.append([DB번호, 스키마명, 테이블명, TabColumnList])
        return columnPartResult
        
    def SampleProfingSQLCreate(self, **kwargs):
        """ 샘플 데이터 수집 SQL을 생성한다.
            - estimate : full 또는 sample로 지정
            - rowcount : estimate가 sample일 경우, 수집하고자 하는 최대 건수 지정
            - columncount : 한 SQL당 수집되는 컬럼 개수를 지정
        """
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0
        
        ### 한번에 수집하고자 하는 컬럼의 건수(기본이 10)
        columncount = kwargs['columncount'] if 'columncount' in kwargs.keys() else 10 
        
        condition = self.ConditionCreate(estimate, rowcount)
        
        ###
        columnPartResult = self.ColumnPartition(columncount, self.TableList, self.ColumnList)
        
        sqltype = 2
        self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)
        
    def sqltypeLoopSQLCreate(self, columnPartResult, sqltype, condition):
        sqls = []
        for DB번호, 스키마명, 테이블명, TabColumnList in columnPartResult:
            for ColumnInfo in TabColumnList:
                if sqltype == 1:
                    sql = self.ColumnProfingSQLCreateSingle(DB번호, 스키마명, 테이블명, ColumnInfo, condition)
                elif sqltype == 2:             
                    sql = self.SampleProfingSQLCreateSingle(DB번호, 스키마명, 테이블명, ColumnInfo, condition)                
                sqls.append([DB번호, 스키마명, 테이블명, np.array(ColumnInfo)[:, 0].tolist(), sql])
        
        ### sql을 테이블에 적재할 수 있도록 분할하고(※CLOB 사용 불가) ID를 생성
        sqls = self.ProfilingSQLTransform(sqls)
        ### DB에 적재함
        self.ProfilingSQLInsert(sqls, sqltype=sqltype)

    def SampleProfingSQLCreateSingle(self, DB번호, 스키마명, 테이블명, ColumnInfo, condition:str):
        """ 샘플정보를 수집하는 개별 SQL을 생성한다. """
        DecodeColumnName = "DECODE(B.NO, "
        StrDecodeColumnValue = "DECODE(B.NO, 99, Null, "
        NumDecodeColumnValue = "DECODE(B.NO, 99, TO_NUMBER(Null), "
        DateDecodeColumnValue = "DECODE(B.NO, 99, TO_DATE(Null, 'YYYYMMDD'), "
        ColumnCount = 0
        
        
        for 컬럼명, 데이터타입 in ColumnInfo:
            ### 컬럼의 개수 파악, copy_t의 rownum 개수를 파악
            ColumnCount += 1

            ### 컬럼명 생성
            DecodeColumnName = DecodeColumnName + str(ColumnCount) + ", '" + 컬럼명 + "', "

            ### 데이터타입별로 합칠 조건을 생성함
            if 데이터타입 in self.StringDatatype:
                StrDecodeColumnValue = StrDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '
            elif 데이터타입 in self.NumericDatatype:
                NumDecodeColumnValue = NumDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '
            elif 데이터타입 in self.DatetimeDatatype:
                DateDecodeColumnValue = DateDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '

        DecodeColumnName = DecodeColumnName[:-2] + ")"
        StrDecodeColumnValue = StrDecodeColumnValue[:-2] + ")"
        NumDecodeColumnValue = NumDecodeColumnValue[:-2] + ")"
        DateDecodeColumnValue = DateDecodeColumnValue[:-2] + ")"        
        
        ### 컬럼 리스트를 생성
        ColumnList = ", ".join(np.array(ColumnInfo)[:, 0].tolist())       
        
        SQLText1 = "SELECT " + ColumnList + ", COUNT(*) AS 발생건수 \n" \
                    + "                FROM " + 스키마명 + "." + 테이블명 + " \n" \
                    + "               {condition}".format(condition=condition) \
                    + "               GROUP BY " + ColumnList
        
        SQLText = """SELECT {DB번호} as DB번호
     , '{스키마명}' as 스키마명
     , '{테이블명}' as 테이블명
     , C.컬럼명
     , SUBSTRB(C.문자형데이터값, 1, 4000) 문자형데이터값
     , C.숫자형데이터값
     , C.날짜형데이터값
     , C.출현건수
     , R_NUM AS 출현순위
FROM (SELECT {DECODE컬럼명} as 컬럼명
           , {문자형_DECODE컬럼값} AS 문자형데이터값
           , {숫자형_DECODE컬럼값} AS 숫자형데이터값
           , {날짜형_DECODE컬럼값} AS 날짜형데이터값
           , SUM(발생건수) as 출현건수
           , ROW_NUMBER() OVER(PARTITION BY {DECODE컬럼명}
                               ORDER BY SUM(발생건수) DESC) AS R_NUM
        FROM ({SQLText}) A
           , (SELECT ROWNUM AS NO FROM DUAL CONNECT BY ROWNUM <= {NO}) B
       GROUP BY B.NO,
                {문자형_DECODE컬럼값},
                {숫자형_DECODE컬럼값},
                {날짜형_DECODE컬럼값}) C
WHERE C.R_NUM <= 10
""".format(DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명, SQLText=SQLText1, NO=str(ColumnCount), \
                       DECODE컬럼명=DecodeColumnName, 문자형_DECODE컬럼값=StrDecodeColumnValue, \
                       숫자형_DECODE컬럼값=NumDecodeColumnValue, 날짜형_DECODE컬럼값=DateDecodeColumnValue)
        return SQLText
    
    
    def ColumnProfingSQLCreate(self, **kwargs):
        """ 컬럼에 대한 기본적인 통계값을 수집 SQL을 생성
            - estimate : full 또는 sample로 지정
            - rowcount : estimate가 sample일 경우, 수집하고자 하는 최대 건수 지정
            - columncount : 한 SQL당 수집되는 컬럼 개수를 지정
        """
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0
        
        ### 한번에 수집하고자 하는 컬럼의 건수(기본이 10)
        columncount = kwargs['columncount'] if 'columncount' in kwargs.keys() else 10 
        
        if estimate == 'full':
            condition = 'WHERE 1=1'
        elif estimate == 'sample':
            condition = 'WHERE ROWNUM <= {rowcount} \n'.format(rowcount=rowcount)

        ###
        columnPartResult = self.ColumnPartition(columncount, self.TableList, self.ColumnList)

        sqltype = 1
        self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)

    def ConditionCreate(self, estimate, rowcount):
        if estimate == 'full':
            condition = 'WHERE 1=1'
        elif estimate == 'sample':
            condition = 'WHERE ROWNUM <= {rowcount} \n'.format(rowcount=rowcount)        
        
        return condition
    
    def ExecuteErrorSQLCreate(self, **kwargs):
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0

        ### 한번에 수집하고자 하는 컬럼의 건수, 재수집대상이라 1로 고정
        columncount = 1

        condition = self.ConditionCreate(estimate, rowcount)

        ### sqltype 별로
        ReCreateSqlNo = []
        for sqltype in [1, 2]:
            #### TableList, ColumnList 재편성
            sql = """SELECT 프로파일링SQL번호, DB번호, 스키마명, 테이블명, 수집컬럼목록
                       FROM 프로파일링_SQL
                      WHERE 실행상태 = '오류'
                        AND SQL유형번호 = ?
                        AND INSTR(수집컬럼목록, ',') > 0"""
            RepCur = self.RepConn.cursor()
            RepCur.execute(sql, sqltype)
            _Result = RepCur.fetchall()

            Result = []
            for 프로파일링SQL번호, DB번호, 스키마명, 테이블명, 컬럼목록 in _Result:
                ReCreateSqlNo.append(프로파일링SQL번호)
                for 컬럼명 in 컬럼목록.split(","):
                    Result.append([DB번호, 스키마명, 테이블명, 컬럼명,])

            ErrorColumnList = pd.DataFrame(Result, columns=['DB번호', '스키마명', '테이블명', '컬럼명']).astype({'DB번호':int})
            ErrorTableList = ErrorColumnList[['DB번호','스키마명','테이블명']].drop_duplicates()

            TableList = pd.merge(left=self.TableList, right=ErrorTableList)
            ColumnList = pd.merge(left=self.ColumnList.reset_index(), right=ErrorColumnList).set_index(['DB번호', '스키마명', '테이블명'])

            columnPartResult = self.ColumnPartition(columncount, TableList, ColumnList)

            self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)

        sql = """UPDATE 프로파일링_SQL SET 실행상태 = '오류재생성' WHERE 프로파일링SQL번호 = ? """
        
        _ = [RepCur.execute(sql, SqlNo) for SqlNo in ReCreateSqlNo]
        RepCur.execute('commit')


    def ColumnProfingSQLCreateSingle(self, DB번호, 스키마명, 테이블명, ColumnInfo, condition:str):
        """ 컬럼별 기본적인 통계 수집할 수 있는 개별 SQL을 생성한다. """        
        DecodeColumnList = ['컬럼명', 'NOTNULL건수', 'NULL건수', 'UNIQUE건수','최소길이', '최대길이', '최대BYTE길이', \
                            '문자열최대값', '문자열최소값', '정수값길이','소수점길이', '숫자형최대값', '숫자형최소값', \
                            '평균값', '날짜최대값', '날짜최소값']
        ColumnCount = 0 
        DecodeArray = ['DECODE(B.NO, ' for _ in DecodeColumnList]

        SQLText_SelectList = "SELECT COUNT(*) 총건수 \n"
        for 컬럼명, 데이터타입 in ColumnInfo:
            ColumnCount += 1
            ColumnIndex = '_'+str(ColumnCount).rjust(2, "0")    
            DecodeArray[0] += str(ColumnCount) + ", '" + 컬럼명 + "', "
            컬럼명 = '"'+컬럼명+'"'

            ### 전체 컬럼에 해당
            ColumnCondition = '             , COUNT('+ 컬럼명 +') AS NOTNULL건수' + ColumnIndex + '\n' #1
            ColumnCondition += '             , COUNT(*) - COUNT('+ 컬럼명 +') AS NULL건수' + ColumnIndex + '\n' #2
            ColumnCondition += '             , COUNT(DISTINCT '+ 컬럼명 +') AS UNIQUE건수' + ColumnIndex + '\n' #3
            for i in [1,2,3]:
                DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '    

            ### 문자형인 경우
            if 데이터타입 in self.StringDatatype:
                ColumnCondition += '             , MIN(LENGTH(TRIM('+ 컬럼명 +'))) AS 최소길이' + ColumnIndex + '\n' #4
                ColumnCondition += '             , MAX(LENGTH(TRIM('+ 컬럼명 +'))) AS 최대길이' + ColumnIndex + '\n' #5
                ColumnCondition += '             , MAX(LENGTHB(TRIM('+ 컬럼명 +'))) AS 최대BYTE길이' + ColumnIndex + '\n' #6
                ColumnCondition += '             , SUBSTRB(MAX(TRIM('+ 컬럼명 +')), 1, 4000) AS 문자열최대값' + ColumnIndex + '\n' #7
                ColumnCondition += '             , SUBSTRB(MIN(TRIM('+ 컬럼명 +')), 1, 4000) AS 문자열최소값' + ColumnIndex + '\n' #8
                for i in [4,5,6,7,8]: #4,5,6,7,8
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '    

            ### 숫자형 인경우
            if 데이터타입 in self.NumericDatatype:
                ColumnCondition += '             , MAX(LENGTH(TRUNC(TO_CHAR('+컬럼명+')))) AS 정수값길이' + ColumnIndex + '\n' #9
                ColumnCondition += '             , MAX(CASE WHEN INSTR('+컬럼명+', \'.\') > 0 THEN LENGTH(TO_CHAR('+컬럼명+' - TRUNC('+컬럼명+")))-1 ELSE 0 END) AS 소수점길이" + ColumnIndex + '\n' #10
                ColumnCondition += '             , MAX('+컬럼명+') AS 숫자형최대값' + ColumnIndex + '\n' #11
                ColumnCondition += '             , MIN('+컬럼명+') AS 숫자형최소값' + ColumnIndex + '\n' #12
                ColumnCondition += '             , AVG('+컬럼명+') AS 평균값' + ColumnIndex + '\n' #13
                for i in [9,10,11,12,13]:
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '                

            ### 날짜형인 경우
            if 데이터타입 in self.DatetimeDatatype:
                ColumnCondition += "             , LEAST(MAX("+ 컬럼명 +"), TO_DATE('9999/12/31', 'YYYY/MM/DD')) AS 날짜최대값" + ColumnIndex + '\n' #13
                ColumnCondition += "             , GREATEST(MIN("+ 컬럼명 +"), TO_DATE('0001/01/01', 'YYYY/MM/DD')) AS 날짜최소값" + ColumnIndex + '\n' #14
                for i in range(14, 15):
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '

            SQLText_SelectList += ColumnCondition

        DecodeFullText = ''
        for i, DecodeText in enumerate(DecodeArray):
            if i == 1:
                DecodeFullText += '     , 총건수 \n'
            DecodeFullText += "     , "+DecodeText + '99999, Null) AS {컬럼명}\n'.format(컬럼명=DecodeColumnList[i])
        DecodeFullText = DecodeFullText[:-1] 

        SQLText = """SELECT {DB번호} AS DB번호
     , '{스키마명}' AS 스키마명
     , '{테이블명}' AS 테이블명
{DecodeFullText}
  FROM ({SQLText_SelectList} 
          FROM {스키마명}.{테이블명}
         {condition}) A
     , (SELECT ROWNUM NO FROM DUAL CONNECT BY ROWNUM <= {ColumnCount}) B"""

        SQLText = SQLText.format(DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명,\
                                 DecodeFullText=DecodeFullText, SQLText_SelectList=SQLText_SelectList[:-1], \
                                 condition=condition, ColumnCount=ColumnCount)
        
        return SQLTexttSelect(self, DB번호, schemaList):
        sql = """SELECT DB번호, 스키마명, 테이블명
                   FROM 테이블정보
                  WHERE DB번호 = :DB번호
                    AND 스키마명 IN ({schemaList}) """.format(schemaList=schemaList)
        self.TableList = pd.read_sql(sql, self.RepConn, params=[DB번호])   
    
    def _ColumnListSelect(self, DB번호, schemaList):
        includeColumnList = self.StringDatatype + self.NumericDatatype + self.DatetimeDatatype
        includeColumnList = "'" + "', '".join(includeColumnList) + "'"
        sql = """SELECT DB번호, 스키마명, 테이블명, 컬럼명, 데이터타입
                   FROM 컬럼정보
                  WHERE (DB번호, 스키마명, 테이블명) IN (SELECT DB번호, 스키마명, 테이블명
                                                           FROM 테이블정보
                                                          WHERE DB번호 = :DB번호
                                                            AND 스키마명 IN ({schemaList})  )
                    AND 데이터타입 IN ({includeColumnList})
                  ORDER BY DB번호, 스키마명, 테이블명, 컬럼순서
                    """.format(schemaList=schemaList, includeColumnList=includeColumnList)
        self.ColumnList = pd.read_sql(sql, self.RepConn, params=[DB번호]).astype({'DB번호':int}).set_index(['DB번호', '스키마명', '테이블명'])

        
        
    def DictionaryListSelect(self, DB번호, schemaList):
        schemaList = "'" + "', '".join(schemaList) + "'"
        self._TableListSelect(DB번호, schemaList)
        self._ColumnListSelect(DB번호, schemaList)
        
class SQLCreateRepository:
    """ SQL을 리포지터리에 저장하기 위한 클래스 """
    def ProfilingSQLTransform(self, sqls):
        """ 식별자값과 SQL값 분할"""
        _sqls = []
        sqlSplitLength = 1000 ### sql을 분할하고자 하는 값
        for sql in sqls:
            uuidValue = str(uuid.uuid1()).upper()
            sqlPart = [[uuidValue, i, sql[4][i*sqlSplitLength : (i+1)*sqlSplitLength]] for i in range(len(sql[4]) // sqlSplitLength + 1)]
            columns = ",".join(sql[3])
            sql = [uuidValue] + sql[0:3] + [columns] + [sqlPart]
            #sql.append(sqlPart)
            _sqls.append(sql)
        return _sqls
        
    
    def ProfilingSQLInsert(self, sqls, sqltype:int):
        InsertSql1 = """INSERT INTO 프로파일링SQL (프로파일링SQL번호, DB번호, 스키마명, 테이블명, 수집컬럼목록, SQL유형번호, 실행상태)
        VALUES (?, ?, ?, ?, ?, ?, ?) """
        
        InsertSql2 = """INSERT INTO 프로파일링SQL분할 (프로파일링SQL번호, 순서, SQL)
        VALUES (?, ?, ?) """        
        
        cur = self.RepConn.cursor()
        try:
            for sql in tqdm(sqls):
                cur.execute(InsertSql1, sql[0:5] + [sqltype, '대기'])
                cur.executemany(InsertSql2, sql[5])
            cur.execute('commit')
        except:
            cur.execute('rollback')
            _, val, _ = sys.exc_info()
            print(val)
            

class SQLCreate(SQLCreateDictionarySelect, SQLCreateRepository):
    """ 프로파일링 SQL을 생성하기 위한 클래스 """
    def __init__(self):
        self.TableList = pd.DataFrame(None, columns = ['DB번호', '스키마명', '테이블명'])
        self.ColumnList = pd.DataFrame(None, columns = ['DB번호', '스키마명', '테이블명', '컬럼명', '데이터타입'])
        
        self.StringDatatype = ['VARCHAR', 'VARCHAR2', 'CHAR', 'NCHAR', 'NVARCHAR', 'NVARCHAR2', ]
        self.NumericDatatype = ['NUMBER', 'INT', 'DECIMAL', 'TINYINT', 'BIGINT', 'SMALLINT', 'FLOAT32', ]
        self.DatetimeDatatype = ['DATE', 'TIMESTAMP(6)', 'TIMESTAMP', 'TIMESTAMP(9)', 'TIMESTAMP(6) WITH TIME ZONE', ]
        self.ExceptionDatatype = ['RAW', 'CLOB', 'BLOB', 'LONG', 'ROWID', ]

    def RepositoryDBConnect(self, RepConn):
        """ 리포지터리 DB에 접속한다."""
        self.RepConn = RepConn
        
    def ColumnPartition(self, columncount, TableList, ColumnList):
        """ 프로파일링SQL을 생성할 때 한번에 수행할 수 있는 컬럼씩 그룹핑 한다.
            동시에 수행되는 컬럼 갯수가 적으면 테이블 반복 조회 횟수가 늘어나고,
            동시에 수행되는 컬럼 갯수가 많의면 한SQL의 집계횟수가 늘어 성능이 저하될 수 있다. """
        columnPartResult = []
        for DB번호, 스키마명, 테이블명 in TableList.values:
            ColumnData = ColumnList.loc[DB번호].loc[스키마명].loc[테이블명].values.reshape(-1, 2).tolist()

            ### 컬럼별로 그룹핑 만들기
            v, etc = divmod(len(ColumnData), columncount)
            v = v + 1 if etc > 0 else v
            TabColumnList = [ColumnData[i*columncount : (i+1)*columncount] for i in range(v)]
            columnPartResult.append([DB번호, 스키마명, 테이블명, TabColumnList])
        return columnPartResult
        
    def SampleProfingSQLCreate(self, **kwargs):
        """ 샘플 데이터 수집 SQL을 생성한다.
            - estimate : full 또는 sample로 지정
            - rowcount : estimate가 sample일 경우, 수집하고자 하는 최대 건수 지정
            - columncount : 한 SQL당 수집되는 컬럼 개수를 지정
        """
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0
        
        ### 한번에 수집하고자 하는 컬럼의 건수(기본이 10)
        columncount = kwargs['columncount'] if 'columncount' in kwargs.keys() else 10 
        
        condition = self.ConditionCreate(estimate, rowcount)
        
        ###
        columnPartResult = self.ColumnPartition(columncount, self.TableList, self.ColumnList)
        
        sqltype = 2
        self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)
        
    def sqltypeLoopSQLCreate(self, columnPartResult, sqltype, condition):
        sqls = []
        for DB번호, 스키마명, 테이블명, TabColumnList in columnPartResult:
            for ColumnInfo in TabColumnList:
                if sqltype == 1:
                    sql = self.ColumnProfingSQLCreateSingle(DB번호, 스키마명, 테이블명, ColumnInfo, condition)
                elif sqltype == 2:             
                    sql = self.SampleProfingSQLCreateSingle(DB번호, 스키마명, 테이블명, ColumnInfo, condition)                
                sqls.append([DB번호, 스키마명, 테이블명, np.array(ColumnInfo)[:, 0].tolist(), sql])
        
        ### sql을 테이블에 적재할 수 있도록 분할하고(※CLOB 사용 불가) ID를 생성
        sqls = self.ProfilingSQLTransform(sqls)
        ### DB에 적재함
        self.ProfilingSQLInsert(sqls, sqltype=sqltype)

    def SampleProfingSQLCreateSingle(self, DB번호, 스키마명, 테이블명, ColumnInfo, condition:str):
        """ 샘플정보를 수집하는 개별 SQL을 생성한다. """
        DecodeColumnName = "DECODE(B.NO, "
        StrDecodeColumnValue = "DECODE(B.NO, 99, Null, "
        NumDecodeColumnValue = "DECODE(B.NO, 99, TO_NUMBER(Null), "
        DateDecodeColumnValue = "DECODE(B.NO, 99, TO_DATE(Null, 'YYYYMMDD'), "
        ColumnCount = 0
        
        
        for 컬럼명, 데이터타입 in ColumnInfo:
            ### 컬럼의 개수 파악, copy_t의 rownum 개수를 파악
            ColumnCount += 1

            ### 컬럼명 생성
            DecodeColumnName = DecodeColumnName + str(ColumnCount) + ", '" + 컬럼명 + "', "

            ### 데이터타입별로 합칠 조건을 생성함
            if 데이터타입 in self.StringDatatype:
                StrDecodeColumnValue = StrDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '
            elif 데이터타입 in self.NumericDatatype:
                NumDecodeColumnValue = NumDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '
            elif 데이터타입 in self.DatetimeDatatype:
                DateDecodeColumnValue = DateDecodeColumnValue + str(ColumnCount) + ', A."' + 컬럼명 + '", '

        DecodeColumnName = DecodeColumnName[:-2] + ")"
        StrDecodeColumnValue = StrDecodeColumnValue[:-2] + ")"
        NumDecodeColumnValue = NumDecodeColumnValue[:-2] + ")"
        DateDecodeColumnValue = DateDecodeColumnValue[:-2] + ")"        
        
        ### 컬럼 리스트를 생성
        ColumnList = ", ".join(np.array(ColumnInfo)[:, 0].tolist())       
        
        SQLText1 = "SELECT " + ColumnList + ", COUNT(*) AS 발생건수 \n" \
                    + "                FROM " + 스키마명 + "." + 테이블명 + " \n" \
                    + "               {condition}".format(condition=condition) \
                    + "               GROUP BY " + ColumnList
        
        SQLText = """SELECT {DB번호} as DB번호
     , '{스키마명}' as 스키마명
     , '{테이블명}' as 테이블명
     , C.컬럼명
     , SUBSTRB(C.문자형데이터값, 1, 4000) 문자형데이터값
     , C.숫자형데이터값
     , C.날짜형데이터값
     , C.출현건수
     , R_NUM AS 출현순위
FROM (SELECT {DECODE컬럼명} as 컬럼명
           , {문자형_DECODE컬럼값} AS 문자형데이터값
           , {숫자형_DECODE컬럼값} AS 숫자형데이터값
           , {날짜형_DECODE컬럼값} AS 날짜형데이터값
           , SUM(발생건수) as 출현건수
           , ROW_NUMBER() OVER(PARTITION BY {DECODE컬럼명}
                               ORDER BY SUM(발생건수) DESC) AS R_NUM
        FROM ({SQLText}) A
           , (SELECT ROWNUM AS NO FROM DUAL CONNECT BY ROWNUM <= {NO}) B
       GROUP BY B.NO,
                {문자형_DECODE컬럼값},
                {숫자형_DECODE컬럼값},
                {날짜형_DECODE컬럼값}) C
WHERE C.R_NUM <= 10
""".format(DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명, SQLText=SQLText1, NO=str(ColumnCount), \
                       DECODE컬럼명=DecodeColumnName, 문자형_DECODE컬럼값=StrDecodeColumnValue, \
                       숫자형_DECODE컬럼값=NumDecodeColumnValue, 날짜형_DECODE컬럼값=DateDecodeColumnValue)
        return SQLText
    
    
    def ColumnProfingSQLCreate(self, **kwargs):
        """ 컬럼에 대한 기본적인 통계값을 수집 SQL을 생성
            - estimate : full 또는 sample로 지정
            - rowcount : estimate가 sample일 경우, 수집하고자 하는 최대 건수 지정
            - columncount : 한 SQL당 수집되는 컬럼 개수를 지정
        """
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0
        
        ### 한번에 수집하고자 하는 컬럼의 건수(기본이 10)
        columncount = kwargs['columncount'] if 'columncount' in kwargs.keys() else 10 
        
        if estimate == 'full':
            condition = 'WHERE 1=1'
        elif estimate == 'sample':
            condition = 'WHERE ROWNUM <= {rowcount} \n'.format(rowcount=rowcount)

        ###
        columnPartResult = self.ColumnPartition(columncount, self.TableList, self.ColumnList)

        sqltype = 1
        self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)

    def ConditionCreate(self, estimate, rowcount):
        if estimate == 'full':
            condition = 'WHERE 1=1'
        elif estimate == 'sample':
            condition = 'WHERE ROWNUM <= {rowcount} \n'.format(rowcount=rowcount)        
        
        return condition
    
    def ExecuteErrorSQLCreate(self, **kwargs):
        ### 데이터 수집의 범위 (기본값은 full (다른값은 sample))
        estimate = kwargs['estimate'].lower() if 'estimate' in kwargs.keys() else 'full'
        
        ### 수집건수 (0은 전체, 나머지 숫자는 해당 건수만큼만)
        rowcount = kwargs['rowcount'] if 'rowcount' in kwargs.keys() else 0

        ### 한번에 수집하고자 하는 컬럼의 건수, 재수집대상이라 1로 고정
        columncount = 1

        condition = self.ConditionCreate(estimate, rowcount)

        ### sqltype 별로
        ReCreateSqlNo = []
        for sqltype in [1, 2]:
            #### TableList, ColumnList 재편성
            sql = """SELECT 프로파일링SQL번호, DB번호, 스키마명, 테이블명, 수집컬럼목록
                       FROM 프로파일링sql
                      WHERE 실행상태 = '오류'
                        AND SQL유형번호 = ?
                        AND INSTR(수집컬럼목록, ',') > 0"""
            RepCur = self.RepConn.cursor()
            RepCur.execute(sql, sqltype)
            _Result = RepCur.fetchall()

            Result = []
            for 프로파일링SQL번호, DB번호, 스키마명, 테이블명, 컬럼목록 in _Result:
                ReCreateSqlNo.append(프로파일링SQL번호)
                for 컬럼명 in 컬럼목록.split(","):
                    Result.append([DB번호, 스키마명, 테이블명, 컬럼명,])

            ErrorColumnList = pd.DataFrame(Result, columns=['DB번호', '스키마명', '테이블명', '컬럼명']).astype({'DB번호':int})
            ErrorTableList = ErrorColumnList[['DB번호','스키마명','테이블명']].drop_duplicates()

            TableList = pd.merge(left=self.TableList, right=ErrorTableList)
            ColumnList = pd.merge(left=self.ColumnList.reset_index(), right=ErrorColumnList).set_index(['DB번호', '스키마명', '테이블명'])

            columnPartResult = self.ColumnPartition(columncount, TableList, ColumnList)

            self.sqltypeLoopSQLCreate(columnPartResult, sqltype, condition)

        sql = """UPDATE 프로파일링SQL SET 실행상태 = '오류재생성' WHERE 프로파일링SQL번호 = ? """
        
        _ = [RepCur.execute(sql, SqlNo) for SqlNo in ReCreateSqlNo]
        RepCur.execute('commit')


    def ColumnProfingSQLCreateSingle(self, DB번호, 스키마명, 테이블명, ColumnInfo, condition:str):
        """ 컬럼별 기본적인 통계 수집할 수 있는 개별 SQL을 생성한다. """        
        DecodeColumnList = ['컬럼명', 'NOTNULL건수', 'NULL건수', 'UNIQUE건수','최소길이', '최대길이', '최대BYTE길이', \
                            '문자열최대값', '문자열최소값', '정수값길이','소수점길이', '숫자형최대값', '숫자형최소값', \
                            '평균값', '날짜최대값', '날짜최소값']
        ColumnCount = 0 
        DecodeArray = ['DECODE(B.NO, ' for _ in DecodeColumnList]

        SQLText_SelectList = "SELECT COUNT(*) 총건수 \n"
        for 컬럼명, 데이터타입 in ColumnInfo:
            ColumnCount += 1
            ColumnIndex = '_'+str(ColumnCount).rjust(2, "0")    
            DecodeArray[0] += str(ColumnCount) + ", '" + 컬럼명 + "', "
            컬럼명 = '"'+컬럼명+'"'

            ### 전체 컬럼에 해당
            ColumnCondition = '             , COUNT('+ 컬럼명 +') AS NOTNULL건수' + ColumnIndex + '\n' #1
            ColumnCondition += '             , COUNT(*) - COUNT('+ 컬럼명 +') AS NULL건수' + ColumnIndex + '\n' #2
            ColumnCondition += '             , COUNT(DISTINCT '+ 컬럼명 +') AS UNIQUE건수' + ColumnIndex + '\n' #3
            for i in [1,2,3]:
                DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '    

            ### 문자형인 경우
            if 데이터타입 in self.StringDatatype:
                ColumnCondition += '             , MIN(LENGTH(TRIM('+ 컬럼명 +'))) AS 최소길이' + ColumnIndex + '\n' #4
                ColumnCondition += '             , MAX(LENGTH(TRIM('+ 컬럼명 +'))) AS 최대길이' + ColumnIndex + '\n' #5
                ColumnCondition += '             , MAX(LENGTHB(TRIM('+ 컬럼명 +'))) AS 최대BYTE길이' + ColumnIndex + '\n' #6
                ColumnCondition += '             , SUBSTRB(MAX(TRIM('+ 컬럼명 +')), 1, 4000) AS 문자열최대값' + ColumnIndex + '\n' #7
                ColumnCondition += '             , SUBSTRB(MIN(TRIM('+ 컬럼명 +')), 1, 4000) AS 문자열최소값' + ColumnIndex + '\n' #8
                for i in [4,5,6,7,8]: #4,5,6,7,8
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '    

            ### 숫자형 인경우
            if 데이터타입 in self.NumericDatatype:
                ColumnCondition += '             , MAX(LENGTH(TRUNC(TO_CHAR('+컬럼명+')))) AS 정수값길이' + ColumnIndex + '\n' #9
                ColumnCondition += '             , MAX(CASE WHEN INSTR('+컬럼명+', \'.\') > 0 THEN LENGTH(TO_CHAR('+컬럼명+' - TRUNC('+컬럼명+")))-1 ELSE 0 END) AS 소수점길이" + ColumnIndex + '\n' #10
                ColumnCondition += '             , MAX('+컬럼명+') AS 숫자형최대값' + ColumnIndex + '\n' #11
                ColumnCondition += '             , MIN('+컬럼명+') AS 숫자형최소값' + ColumnIndex + '\n' #12
                ColumnCondition += '             , AVG('+컬럼명+') AS 평균값' + ColumnIndex + '\n' #13
                for i in [9,10,11,12,13]:
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '                

            ### 날짜형인 경우
            if 데이터타입 in self.DatetimeDatatype:
                ColumnCondition += "             , LEAST(MAX("+ 컬럼명 +"), TO_DATE('9999/12/31', 'YYYY/MM/DD')) AS 날짜최대값" + ColumnIndex + '\n' #13
                ColumnCondition += "             , GREATEST(MIN("+ 컬럼명 +"), TO_DATE('0001/01/01', 'YYYY/MM/DD')) AS 날짜최소값" + ColumnIndex + '\n' #14
                for i in range(14, 15):
                    DecodeArray[i] += str(ColumnCount) + ', ' + DecodeColumnList[i] + ColumnIndex + ', '

            SQLText_SelectList += ColumnCondition

        DecodeFullText = ''
        for i, DecodeText in enumerate(DecodeArray):
            if i == 1:
                DecodeFullText += '     , 총건수 \n'
            DecodeFullText += "     , "+DecodeText + '99999, Null) AS {컬럼명}\n'.format(컬럼명=DecodeColumnList[i])
        DecodeFullText = DecodeFullText[:-1] 

        SQLText = """SELECT {DB번호} AS DB번호
     , '{스키마명}' AS 스키마명
     , '{테이블명}' AS 테이블명
{DecodeFullText}
  FROM ({SQLText_SelectList} 
          FROM {스키마명}.{테이블명}
         {condition}) A
     , (SELECT ROWNUM NO FROM DUAL CONNECT BY ROWNUM <= {ColumnCount}) B"""

        SQLText = SQLText.format(DB번호=DB번호, 스키마명=스키마명, 테이블명=테이블명,\
                                 DecodeFullText=DecodeFullText, SQLText_SelectList=SQLText_SelectList[:-1], \
                                 condition=condition, ColumnCount=ColumnCount)
        
        return SQLText
