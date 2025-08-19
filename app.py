from flask import Flask, render_template, request, redirect, url_for, flash, session, jsonify,Response,make_response
import pymysql
import mysql.connector
from mysql.connector import Error
import xlsxwriter
import bcrypt, hashlib, os, io
from datetime import datetime
import pandas as pd
from datetime import datetime, time
import openpyxl
from openpyxl.utils import column_index_from_string
import traceback
from collections import OrderedDict  # 추가
import zipfile
from werkzeug.utils import secure_filename
import urllib.parse
#from flask import make_response
#from io import StringIO
#import csv

app = Flask(__name__)
app.secret_key = 'supersecretkey'  # 세션 키 (적절한 값으로 설정)

# 데이터베이스 연결 설정
db_config = {
    'host': '10.38.150.249',
    'user': 'bantadmin',
    'password': 'bant$321',
    'db': 'bant',
    'charset': 'utf8mb4',
}

#def get_db_connection():
#    return pymysql.connect(**db_config)

def get_db_connection():
    try:
        connection = mysql.connector.connect(**db_config)
        return connection
    except Error as e:
        print(f"Database connection error: {e}")
        return None

#======================================================================
@app.route('/', methods=['GET', 'POST'])   #root로 login 페이지 지정 
def login():
    if request.method == 'POST':
        user_id = request.form['user_id']
        user_pswd = request.form['user_pswd']

        # 데이터베이스에서 사용자 정보를 확인하는 쿼리
        connection = get_db_connection()
        with connection.cursor(buffered=True) as cursor:
                sql = "SELECT * FROM cm_user WHERE USER_ID = %s"
                cursor.execute(sql, (user_id,))
                user = cursor.fetchone()

            # 디버깅: 쿼리 결과 확인
        print(user)

        # 사용자 입력 비밀번호와 DB에 저장된 암호화된 비밀번호를 비교(우선 ID만 확인)
        if user:
    
            if user_id == 'kkh0123456' or 'ekgus3907' or 'jhlim':
                ###flash('로그인 성공!', 'success')
                ###return redirect(url_for('main_page'))  # 로그인 후 이동할 페이지 
                return render_template('main.html')           
            else:

                    flash('아이디 또는 비밀번호가 잘못되었습니다.', 'danger')
        else:
            flash('아이디 또는 비밀번호가 잘못되었습니다.', 'danger')

    return render_template('login.html')

### 화면에서 입력한 검색어로 2개의 테이블을 조인하여 like 매칭 후 결과를 보여주는 app
@app.route("/suju_info", methods=["GET", "POST"])
def sujuinfo():
    if request.method == "POST":
        customer_name = request.form.get("customer_name")
        if customer_name:
            result = get_customer_address(customer_name)
            return render_template("sujuinfo.html", customer_name=customer_name, results=result)
        else:
            flash("고객명을 입력해주세요.")
            return redirect(url_for("sujuinfo"))
    return render_template("sujuinfo.html", customer_name=None, results=None)

def get_customer_address(customer_name):
    try:
        connection = get_db_connection()
        cursor = connection.cursor()

        query = """
            SELECT suju_no, a.empno, nm, hpno, d.cust_nm, e.cust_nm, suju_name 
            FROM sa_ssujuinfo a
            INNER JOIN bs_hrbss c ON a.empno = c.empno  
            INNER JOIN bs_cust d ON a.ch_cd = d.cust_cd 
            INNER JOIN bs_cust e ON a.cust_cd = e.cust_cd 
            WHERE suju_name LIKE %s OR d.cust_nm LIKE %s OR e.cust_nm LIKE %s
            UNION ALL
            SELECT suju_no, a.empno, nm, hpno, d.cust_nm, e.cust_nm, suju_name 
            FROM sa_msujuinfo a 
            INNER JOIN bs_hrbss c ON a.empno = c.empno  
            INNER JOIN bs_cust d ON a.ch_cd = d.cust_cd 
            INNER JOIN bs_cust e ON a.cust_cd = e.cust_cd 
            WHERE suju_name LIKE %s OR d.cust_nm LIKE %s OR e.cust_nm LIKE %s
        """

        like_pattern = f"%{customer_name}%"
        params = (like_pattern, like_pattern, like_pattern, like_pattern, like_pattern, like_pattern)

        cursor.execute(query, params)
        result = cursor.fetchall()

        cursor.close()
        connection.close()

        return result

    except pymysql.MySQLError as err:
        print(f"데이터베이스 연결에 문제가 있습니다: {err}")
        return None

### ======================================================수금내역조회
@app.route('/collect_list', methods=['GET', 'POST'])
def collect_list():
    today = datetime.now().strftime('%Y-%m-%d')
    results = []
    totals = {'공급가': 0, '부가세': 0, '합계금액': 0, '총계약금액': 0}
    
    if request.method == 'POST':
        search_conditions = {
            'bill_date_from': request.form.get('bill_date_from', ''),
            'bill_date_to': request.form.get('bill_date_to', today),
            'cust_name': request.form.get('cust_name', '')
        }
        
        try:
            # 날짜 형식 변환 (YYYY-MM-DD → YYYYMMDD)
            bill_date_from = search_conditions['bill_date_from'].replace("-", "")
            bill_date_to = search_conditions['bill_date_to'].replace("-", "")
            cust_nm = search_conditions['cust_name'].strip()

            connection = get_db_connection()
            if not connection:
                return render_template('collect_list.html', 
                                    error="DB 연결 실패",
                                    search_conditions=search_conditions)

            # 1. 거래처 코드 조회 (수금처명 매핑)
            cust_cd = None
            if cust_nm:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT CUST_CD FROM bs_cust 
                        WHERE (UPPER(CUST_NM) LIKE UPPER(%s) OR UPPER(CUST_ABRV_NM) LIKE UPPER(%s))
                        AND USE_YN = 'Y' LIMIT 1
                    """, (f"%{cust_nm}%", f"%{cust_nm}%"))
                    cust_result = cursor.fetchone()
                    cust_cd = cust_result['CUST_CD'] if cust_result else None

            # 2. 메인 쿼리 실행 (수정된 JOIN 및 컬럼 매핑)
            with connection.cursor(dictionary=True) as cursor:
                query = """
                    SELECT 
                        c.CUST_NM AS 수금처명,
                        sc.BILL_DATE AS 수금일자,
                        sc.SUPRICE_AMT AS 공급가,
                        sc.VAT AS 부가세,
                        sc.TOT_AMT AS 합계금액,
                        COALESCE(ss.SUJU_NO, ms.SUJU_NO) AS 수주번호,
                        COALESCE(ss.CHG_CHASU, ms.CHG_CHASU, '0') AS 변경차수,
                        COALESCE(ss.SUJU_NAME, ms.SUJU_NAME, '-') AS 수주건명,
                        COALESCE(DATE_FORMAT(ss.CONT_DATE, '%Y%m%d'), DATE_FORMAT(ms.SUJU_DATE, '%Y%m%d'), '-') AS 계약일자,
                        COALESCE(ss.CONT_AMT, ms.TOT_CONTAMT, 0) AS 총계약금액
                    FROM sa_collect sc
                    LEFT JOIN bs_cust c ON sc.CH_CD = c.CUST_CD
                    LEFT JOIN sa_ssujuinfo ss ON sc.SUJU_NO = ss.SUJU_NO 
                        AND sc.CHG_CHASU = ss.CHG_CHASU
                        AND sc.COMP_CD = ss.COMP_CD
                        AND sc.SITE_CD = ss.SITE_CD
                    LEFT JOIN sa_msujuinfo ms ON sc.SUJU_NO = ms.SUJU_NO
                        AND sc.CHG_CHASU = ms.CHG_CHASU
                        AND sc.COMP_CD = ms.COMP_CD
                        AND sc.SITE_CD = ms.SITE_CD
                    WHERE sc.BILL_DATE BETWEEN %s AND %s
                    """ + (" AND sc.CH_CD = %s" if cust_cd else "") + """
                    ORDER BY sc.BILL_DATE ASC
                """
                params = [bill_date_from, bill_date_to]
                if cust_cd:
                    params.append(cust_cd)
                
                cursor.execute(query, params)
                results = cursor.fetchall()

                # 합계 계산
                if results:
                    totals = {
                        '공급가': sum(row['공급가'] or 0 for row in results),
                        '부가세': sum(row['부가세'] or 0 for row in results),
                        '합계금액': sum(row['합계금액'] or 0 for row in results),
                        '총계약금액': sum(row['총계약금액'] or 0 for row in results)
                    }

        except Exception as e:
            print(f"에러 발생: {str(e)}")
            return render_template('collect_list.html', 
                                error=f"시스템 오류: {str(e)}",
                                search_conditions=search_conditions)
        finally:
            if connection and connection.is_connected():
                connection.close()

    return render_template(
        'collect_list.html',
        results=results,
        totals=totals,
        search_conditions=search_conditions if request.method == 'POST' 
                         else {'bill_date_to': today}
    )

### =============================================지급내역조회 DESC순 ASC순으로 변경
@app.route('/pay_list', methods=['GET', 'POST'])
def pay_list():
    today = datetime.now().strftime('%Y-%m-%d')
    results = []
    totals = {'지급금액': 0, '총계약금액': 0}
    
    if request.method == 'POST':
        search_conditions = {
            'pay_date_from': request.form.get('pay_date_from', ''),
            'pay_date_to': request.form.get('pay_date_to', today),
            'po_name': request.form.get('po_name', '')
        }
        
        try:
            # 날짜 형식 변환 (YYYY-MM-DD → YYYYMMDD)
            pay_date_from = search_conditions['pay_date_from'].replace("-", "")
            pay_date_to = search_conditions['pay_date_to'].replace("-", "")
            cust_nm = search_conditions['po_name'].strip()

            connection = get_db_connection()
            if not connection:
                return render_template('pay_list.html', 
                                    error="DB 연결 실패",
                                    search_conditions=search_conditions)

            # 1. 거래처 코드 조회 (수금처명 매핑)
            cust_cd = None
            if cust_nm:
                with connection.cursor(dictionary=True) as cursor:
                    cursor.execute("""
                        SELECT CUST_CD FROM bs_cust 
                        WHERE (UPPER(CUST_NM) LIKE UPPER(%s) OR UPPER(CUST_ABRV_NM) LIKE UPPER(%s))
                        AND USE_YN = 'Y' LIMIT 1
                    """, (f"%{cust_nm}%", f"%{cust_nm}%"))
                    po_result = cursor.fetchone()
                    cust_cd = po_result['CUST_CD'] if po_result else None

            # 2. 메인 쿼리 실행 (수정된 JOIN 및 컬럼 매핑)
            with connection.cursor(dictionary=True) as cursor:
                query = """
                    SELECT 
                        c.CUST_NM AS 지급처명,
                        sc.PAY_DATE AS 지급일자,
                        sc.PAY_AMT AS 지급금액,
                        COALESCE(ss.SUJU_NO, ms.SUJU_NO) AS 수주번호,
                        COALESCE(ss.CHG_CHASU, ms.CHG_CHASU, '0') AS 변경차수,
                        COALESCE(ss.SUJU_NAME, ms.SUJU_NAME, '-') AS 수주건명,
                        COALESCE(DATE_FORMAT(ss.CONT_DATE, '%Y%m%d'), DATE_FORMAT(ms.SUJU_DATE, '%Y%m%d'), '-') AS 계약일자,
                        COALESCE(ss.CONT_AMT, ms.TOT_CONTAMT, 0) AS 총계약금액
                    FROM sa_pay sc
                    LEFT JOIN bs_cust c ON sc.PO_CD = c.CUST_CD
                    LEFT JOIN sa_ssujuinfo ss ON sc.SUJU_NO = ss.SUJU_NO 
                        AND sc.CHG_CHASU = ss.CHG_CHASU
                        AND sc.COMP_CD = ss.COMP_CD
                        AND sc.SITE_CD = ss.SITE_CD
                    LEFT JOIN sa_msujuinfo ms ON sc.SUJU_NO = ms.SUJU_NO
                        AND sc.CHG_CHASU = ms.CHG_CHASU
                        AND sc.COMP_CD = ms.COMP_CD
                        AND sc.SITE_CD = ms.SITE_CD
                    WHERE sc.PAY_DATE BETWEEN %s AND %s
                    """ + (" AND sc.PO_CD = %s" if cust_cd else "") + """
                    ORDER BY sc.PAY_DATE ASC 
                """
                params = [pay_date_from, pay_date_to]
                if cust_cd:
                    params.append(cust_cd)
                
                cursor.execute(query, params)
                results = cursor.fetchall()

                # 합계 계산
                if results:
                    totals = {
                        '지급금액': sum(row['지급금액'] or 0 for row in results),
                        '총계약금액': sum(row['총계약금액'] or 0 for row in results)
                    }

        except Exception as e:
            print(f"에러 발생: {str(e)}")
            return render_template('pay_list.html', 
                                error=f"시스템 오류: {str(e)}",
                                search_conditions=search_conditions)
        finally:
            if connection and connection.is_connected():
                connection.close()

    return render_template(
        'pay_list.html',
        results=results,
        totals=totals,
        search_conditions=search_conditions if request.method == 'POST' 
                         else {'pay_date_to': today}
    )
### ========================================엑셀파일 거래처 코드 업데이트
@app.route('/ex_chcdupdate', methods=['GET', 'POST'])
def ex_chcdupdate():
    if request.method == 'POST':
        excel_path = request.form.get('excel_path')
        ref_column_index = int(request.form.get('ref_column_index'))
        target_column_index = int(request.form.get('target_column_index'))

        df = pd.read_excel(excel_path)
        connection = get_db_connection()
        cursor = connection.cursor()

        for index, row in df.iterrows():
            search_value = row.iloc[ref_column_index-1]  # 참조 대상 컬럼 값

            # 해당 값과 LIKE 조건으로 매칭하는 SQL 쿼리 작성
            # query = "SELECT cust_cd FROM bs_cust WHERE cust_nm LIKE %s"  # 거래처코드
            query = "SELECT empno FROM bs_hrbss WHERE nm LIKE %s"  # 이름으로 사번 매칭

            #cursor.execute(query, f'%{search_value}%')
            cursor.execute(query, (f'%{search_value}%',))  # 튜플 형태로 수정
            results = cursor.fetchall()

            # 단 한 건만 매칭되는 경우만 업데이트 진행
            if len(results) == 1:
                cust_cd = results[0][0]  # 첫 번째 컬럼이 cust_cd
                df.at[index, df.columns[target_column_index-1]] = cust_cd  # 변환 대상 컬럼에 값 업데이트

        df.to_excel(excel_path, index=False)

        return "작업이 완료되었습니다."

    return render_template('ex_chcdupdate.html')

### ========================================급여대장 한줄로 변환하기
@app.route('/ex_paylistconv', methods=['GET', 'POST'])
def paylist_converter():
    if request.method == 'GET':
        return render_template('ex_paylistconv.html'), 200, {'Content-Type': 'text/html'}

    elif request.method == 'POST':
        try:
            if 'file' not in request.files:
                return jsonify({"success": False, "message": "파일이 업로드되지 않았습니다."}), 400

            uploaded_file = request.files['file']
            if uploaded_file.filename == '':
                return jsonify({"success": False, "message": "파일명이 없습니다."}), 400

            df = pd.read_excel(uploaded_file, header=None)

            # 첫 3줄을 기준으로 컬럼명 추출
            headers = df.iloc[0].tolist() + df.iloc[1].tolist() + df.iloc[2].tolist()
            headers = [str(h) if pd.notna(h) else f"컬럼{i}" for i, h in enumerate(headers)]

            data_list = []
            for i in range(3, len(df), 3):
                row1 = df.iloc[i]
                row2 = df.iloc[i + 1] if i + 1 < len(df) else pd.Series([None] * df.shape[1])
                row3 = df.iloc[i + 2] if i + 2 < len(df) else pd.Series([None] * df.shape[1])
                full_row = list(row1) + list(row2) + list(row3)
                row_dict = {headers[j]: (0 if pd.isna(val) else val) for j, val in enumerate(full_row)}
                data_list.append(row_dict)

            return jsonify({"success": True, "data": data_list}), 200

        except Exception as e:
            return jsonify({"success": False, "message": str(e)}), 500

### ========================================급여대장 한줄로 변환 후 필요항목 select
@app.route('/get_adjust_columns', methods=['GET'])
def get_adjust_columns():
    try:
        comp_cd = request.args.get('comp_cd', 'BANT')  # 기본값 'BANT'
        connection = get_db_connection()
        cursor = connection.cursor()
      
        query = f"""
            SELECT cd_nm
            FROM cm_code
            WHERE comp_cd = %s AND cl_cd IN ('MA014', 'MA007','MA008')
            ORDER BY 
                CASE cl_cd 
                    WHEN 'MA014' THEN 1
                    WHEN 'MA007' THEN 2
                    WHEN 'MA008' THEN 3
                    ELSE 4
                END,
                sort_ord
        """

        cursor.execute(query, (comp_cd,))
        results = cursor.fetchall()
        if not results:
            return jsonify({"success": False, "message": "No columns found."}), 404

        columns = [row[0] for row in results]
        return jsonify({"success": True, "columns": columns}), 200

    except Exception as e:
        return jsonify({"success": False, "message": str(e)}), 500

######################## 롯데카드 컨버전 start  ##########################
def get_classification(row):
    try:
        time_val = row['승인시간']
        addr = str(row.get('가맹점주소', ''))
        industry = str(row.get('가맹점업종', ''))
        gamangjumnm = str(row.get('가맹점명', ''))
        saupjano = str(row.get('사업자번호', ''))

        # 금액을 숫자로 처리
        amount_raw = row.get('승인금액(원화)', '0')
        if isinstance(amount_raw, str):
            amount = int(amount_raw.replace(",", "").strip())
        else:
            amount = int(amount_raw)

        # 업종 조건 우선 처리
        if '주유소' in industry:
            return '주유비'
        if any(word in industry for word in ['주차장', '부동산중계(임대)']):
            return '주차비'
        if any(word in industry for word in ['커피', '편의점']):
            return '간식/음료'
        if '골프' in industry:
            return '업무추진비'
        if '보험' in industry:
            return '보험료'
        if '국세' in industry:
            return '세금과공과'
        if '우체국' in industry:
            return '등기'
        if any(word in industry for word in ['기타교통','수입차']):
            return '교통비(출장)'
        if any(word in industry for word in ['기타(용역)','기타대행','전자상거래']):
            return '지급수수료'
        if any(word in industry for word in ['우체국']):
            return '등기'
        if '나이스결제대행' in gamangjumnm:
            return '업무추진비'
        if '운행서비스' in gamangjumnm:
            return '대리운전비'
        if 'Agoda_NICE' in gamangjumnm:
            return '숙박비(출장)'
        if '공사(주차료)' in gamangjumnm:
            return '주차비'
        if any(word in gamangjumnm for word in ['법원행정처', '쿠팡','한국정보통신','KCP결제','갤럭시아_ARS','다우데이타','나이스결제대행']):
            return '지급수수료'

        # 시간 조건
        hour = pd.to_datetime(time_val).time()
        is_night = hour >= time(16, 0) or hour < time(6, 0)
        is_day = not is_night
        has_songpa = '송파구' in addr
        low_amount = amount < 50000

        if is_night and has_songpa:
            return '내근석식'
        elif is_day and has_songpa:
            return '내근중식'
        elif low_amount and is_night and not has_songpa:
            return '외근석식'
        elif low_amount and is_day and not has_songpa:
            return '외근중식'
        elif not low_amount and is_night and not has_songpa:
            return '외근석식(접)'
        elif not low_amount and is_day and not has_songpa:
            return '외근중식(접)'
        else:
            return ''
    except Exception as e:
        return f"오류: {e}"


@app.route("/card_lotte_conv", methods=["GET", "POST"])
def card_lotte_conv():
    global df_global
    data = None
    columns = None
    if request.method == "POST":
        file = request.files["excel_file"]
        if file:
            df = pd.read_excel(file, skiprows=1)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df_global = df.copy()
            data = df.values.tolist()
            columns = df.columns.tolist()
    return render_template("card_lotte_conv.html", data=data, columns=columns)

# ✅ 자동 분류 라우트
@app.route("/classify", methods=["POST"])
def classify():
    global df_global
    if df_global is not None:
        df = df_global.copy()
        df['사원번호'] = df.apply(get_classification, axis=1)
        df_global = df.copy()  # ✅ 이 줄이 반드시 필요합니다 사용구분
        data = df.values.tolist()
        columns = df.columns.tolist()
        return render_template("card_lotte_conv.html", data=data, columns=columns)
    return render_template("card_lotte_conv.html", data=None, columns=None)

# 롯데카드 데이터를 erp 업로드 파일 형식으로 변환하고 엑셀 다운로드
@app.route("/lotte_reorder", methods=["POST"])
def lotte_reorder():
    global df_global
    if df_global is not None:
        df = df_global.copy()

        # 매칭 항목 정의
        columns_map = {
            "No": None,  # 순번
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일자",
            "사용구분": "사원번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액(원화)",
            "적요": "적요",
            "비고": "가맹점업종",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())

        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df.columns:
                l_reordered[col] = df[source_col]
            else:
                l_reordered[col] = ""

        df_global_l_reordered = l_reordered.copy()  # 하단 그리드용
        return render_template("card_lotte_conv.html", data=df.values.tolist(), columns=df.columns.tolist(),
                               l_reordered_data=l_reordered.values.tolist(), l_reordered_columns=l_reordered.columns.tolist())
    return render_template("card_lotte_conv.html")

@app.route("/lotte_download", methods=["POST"])
def lotte_download():
    global df_global
    if df_global is not None:
        # 동일 로직으로 재정렬
        columns_map = {
            "No": None,
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일자",
            "사용구분": "사원번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액(원화)",
            "적요": "적요",
            "비고": "가맹점업종",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())
        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df_global) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df_global.columns:
                l_reordered[col] = df_global[source_col]
            else:
                l_reordered[col] = ""

        from flask import send_file
        output_path = "lotte_card_result.xlsx"
        l_reordered.to_excel(output_path, index=False)
        return send_file(output_path, as_attachment=True)
    return "No data to download", 400
######################## 롯데카드 컨버전 end  ##########################

#################### 롯데카드 카드번호별 엑셀파일 다운로드 start ###############
@app.route("/lotte_download_by_card", methods=["POST"])
def lotte_download_by_card():
    global df_global
    if df_global is not None:
        # 동일한 재정렬 로직 수행
        columns_map = {
            "No": None,
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일자",
            "사용구분": "사원번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액(원화)",
            "적요": "적요",
            "비고": "가맹점업종",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())
        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df_global) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df_global.columns:
                l_reordered[col] = df_global[source_col]
            else:
                l_reordered[col] = ""

        # 카드번호별로 데이터 분리
        card_groups = l_reordered.groupby('카드번호')
        
        # 메모리 내에서 ZIP 파일 생성
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for card_num, group in card_groups:
                # 안전한 파일명 생성 (한글 처리)
                safe_card_num = secure_filename(str(card_num))
                filename = f"롯데카드_{safe_card_num}.xlsx"
                
                # 엑셀 파일을 메모리에 저장
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    group.to_excel(writer, index=False)
                excel_buffer.seek(0)
                
                # ZIP에 추가
                zip_file.writestr(filename, excel_buffer.getvalue())
        
        zip_buffer.seek(0)
        
        # ZIP 파일 다운로드 응답 생성 (한글 파일명 인코딩 처리)
        response = make_response(zip_buffer.getvalue())
        response.headers['Content-Type'] = 'application/zip'
        safe_zip_filename = urllib.parse.quote("롯데카드_분할다운로드.zip")
        response.headers['Content-Disposition'] = f'attachment; filename*=UTF-8\'\'{safe_zip_filename}'
        return response
    
    return "No data to download", 400

#################### 롯데카드 카드번호별 엑셀파일 다운로드 end   ###############

######################## kb국민카드 컨버전 start  ##########################
def kb_get_classification(row):
    try:
        time_val = row['승인시간']
        addr = str(row.get('가맹점주소', ''))
        industry = str(row.get('업종명', ''))
        gamangjumnm = str(row.get('가맹점명', ''))

        # 금액을 숫자로 처리
        amount_raw = row.get('승인금액', '0')
        if isinstance(amount_raw, str):
            amount = int(amount_raw.replace(",", "").strip())
        else:
            amount = int(amount_raw)

        # 업종 조건 우선 처리
        if '주유소' in industry:
            return '주유비'
        if '주차장' in industry:
            return '주차비'
        if any(word in industry for word in ['커피', '편의점']):
            return '간식/음료'
        if '골프' in industry:
            return '업무추진비'
        if '철도' in industry:
            return '교통비(출장)'
        if '우체국' in industry:
            return '등기'
        if any(word in gamangjumnm for word in ['통신판매','이니시스']):
            return '지급수수료'
        if any(word in gamangjumnm for word in ['주차장']):
            return '주차비'

        # 시간 조건
        hour = pd.to_datetime(time_val).time()
        is_night = hour >= time(16, 0) or hour < time(6, 0)
        is_day = not is_night
        has_songpa = '송파구' in addr
        low_amount = amount < 50000

        if is_night and has_songpa:
            return '내근석식'
        elif is_day and has_songpa:
            return '내근중식'
        elif low_amount and is_night and not has_songpa:
            return '외근석식'
        elif low_amount and is_day and not has_songpa:
            return '외근중식'
        elif not low_amount and is_night and not has_songpa:
            return '외근석식(접)'
        elif not low_amount and is_day and not has_songpa:
            return '외근중식(접)'
        else:
            return ''
    except Exception as e:
        return f"오류: {e}"


@app.route("/card_kb_conv", methods=["GET", "POST"])
def card_kb_conv():
    global df_global
    data = None
    columns = None
    if request.method == "POST":
        file = request.files["excel_file"]
        if file:
            df = pd.read_excel(file, skiprows=0)
            df = df.loc[:, ~df.columns.str.contains('^Unnamed')]
            df_global = df.copy()
            data = df.values.tolist()
            columns = df.columns.tolist()
    return render_template("card_kb_conv.html", data=data, columns=columns)

# ✅ 자동 분류 라우트
@app.route("/kb_classify", methods=["POST"])
def kb_classify():
    global df_global
    if df_global is not None:
        df = df_global.copy()
        df['부서번호'] = df.apply(kb_get_classification, axis=1)
        df_global = df.copy()  # ✅ 이 줄이 반드시 필요합니다 사용구분
        data = df.values.tolist()
        columns = df.columns.tolist()
        return render_template("card_kb_conv.html", data=data, columns=columns)
    return render_template("card_kb_conv.html", data=None, columns=None)

# 국민카드 데이터를 erp 업로드 파일 형식으로 변환하고 엑셀 다운로드
@app.route("/kb_reorder", methods=["POST"])
def kb_reorder():
    global df_global
    if df_global is not None:
        df = df_global.copy()

        # 매칭 항목 정의
        columns_map = {
            "No": None,  # 순번
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일",
            "사용구분": "부서번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액",
            "적요": "적요",
            "비고": "업종명",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())

        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df.columns:
                l_reordered[col] = df[source_col]
            else:
                l_reordered[col] = ""

        df_global_l_reordered = l_reordered.copy()  # 하단 그리드용
        return render_template("card_kb_conv.html", data=df.values.tolist(), columns=df.columns.tolist(),
                               l_reordered_data=l_reordered.values.tolist(), l_reordered_columns=l_reordered.columns.tolist())
    return render_template("card_kb_conv.html")

@app.route("/kb_download", methods=["POST"])
def kb_download():
    global df_global
    if df_global is not None:
        # 동일 로직으로 재정렬
        columns_map = {
            "No": None,
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일",
            "사용구분": "부서번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액",
            "적요": "적요",
            "비고": "업종명",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())
        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df_global) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df_global.columns:
                l_reordered[col] = df_global[source_col]
            else:
                l_reordered[col] = ""

        from flask import send_file
        output_path = "kb_card_result.xlsx"
        l_reordered.to_excel(output_path, index=False)
        return send_file(output_path, as_attachment=True)
    return "No data to download", 400
######################## kb국민카드 컨버전 end  ###############################

#################### kb국민카드 카드번호별 엑셀파일 다운로드 start #############
@app.route("/kb_download_by_card", methods=["POST"])
def kb_download_by_card():
    global df_global
    if df_global is not None:
        # 동일한 재정렬 로직 수행
        columns_map = {
            "No": None,
            "카드번호": "카드번호",
            "카드명": "카드명",
            "카드구분": "카드구분",
            "사용자": "사용자",
            "부서명": "부서명",
            "사용일자": "승인일",
            "사용구분": "부서번호",
            "계정과목": "계정과목",
            "사용금액": "승인금액",
            "적요": "적요",
            "비고": "업종명",
            "영수증유무": "영수증유무",
            "가맹점명": "가맹점명",
            "가맹점소재지": "가맹점주소",
            "등록방법": "등록방법",
        }

        l_reordered = pd.DataFrame(columns=columns_map.keys())
        for col, source_col in columns_map.items():
            if col == "No":
                l_reordered[col] = range(1, len(df_global) + 1)
            elif col == "영수증유무":
                l_reordered[col] = "Y"   
            elif source_col in df_global.columns:
                l_reordered[col] = df_global[source_col]
            else:
                l_reordered[col] = ""

        # 카드번호별로 데이터 분리
        card_groups = l_reordered.groupby('카드번호')
        
        # 메모리 내에서 ZIP 파일 생성
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zip_file:
            for card_num, group in card_groups:
                # 안전한 파일명 생성 (한글 처리)
                safe_card_num = secure_filename(str(card_num))
                filename = f"KB카드_{safe_card_num}.xlsx"
                
                # 엑셀 파일을 메모리에 저장
                excel_buffer = io.BytesIO()
                with pd.ExcelWriter(excel_buffer, engine='openpyxl') as writer:
                    group.to_excel(writer, index=False)
                excel_buffer.seek(0)
                
                # ZIP에 추가
                zip_file.writestr(filename, excel_buffer.getvalue())
        
        zip_buffer.seek(0)
        
        # ZIP 파일 다운로드 응답 생성 (한글 파일명 인코딩 처리)
        response = make_response(zip_buffer.getvalue())
        response.headers['Content-Type'] = 'application/zip'
        safe_zip_filename = urllib.parse.quote("KB카드_분할다운로드.zip")
        response.headers['Content-Disposition'] = f'attachment; filename*=UTF-8\'\'{safe_zip_filename}'
        return response
    
    return "No data to download", 400

#################### kb국민카드 카드번호별 엑셀파일 다운로드 end #############

############미청구내역 조회 start ################################################
@app.route('/non_charge_list', methods=['GET', 'POST'])
def non_charge_list():
    today = datetime.now().strftime('%Y-%m-%d')
    results = []

    search_conditions = {
        'from_date': '',
        'to_date': today,
        'suju_type': 'all',
        'unbilled_only': False
    }

    if request.method == 'POST':
        get_value = request.form.get
    else:
        get_value = request.args.get

    search_conditions['from_date'] = get_value('from_date', '')
    search_conditions['to_date'] = get_value('to_date', today)
    search_conditions['suju_type'] = get_value('suju_type', 'all')
    search_conditions['unbilled_only'] = get_value('unbilled_only') in ['1', 'true', 'True']

    try:
        from_date = search_conditions['from_date'].replace('-', '')
        to_date = search_conditions['to_date'].replace('-', '')
        suju_type = search_conditions['suju_type']
        unbilled_only = search_conditions['unbilled_only']

        connection = get_db_connection()
        if not connection:
            return render_template('non_charge_list.html',
                                   error="DB 연결 실패",
                                   search_conditions=search_conditions)

        with connection.cursor(dictionary=True) as cursor:
            query = """
                SELECT
                    sch.SUJU_NO AS 수주번호,
                    sch.CHG_CHASU AS 차수,
                    sch.BILL_DATE AS 청구예정일,
                    sch.SUPRICE_AMT AS 계획공급가,
                    sch.VAT AS 계획부가세,
                    sch.TOT_AMT AS 계획합계,
                    chg.BILL_DATE AS 실제청구일,
                    chg.SUPRICE_AMT AS 실제공급가,
                    chg.VAT AS 실제부가세,
                    chg.TOT_AMT AS 실제합계,
                    (sch.TOT_AMT - IFNULL(chg.TOT_AMT, 0)) AS 미청구금액,
                    CASE
                        WHEN LEFT(sch.SUJU_NO, 1) = 'S' THEN su.SUJU_NAME
                        WHEN LEFT(sch.SUJU_NO, 1) = 'M' THEN ma.SUJU_NAME
                        ELSE '-'
                    END AS 수주건명,
                    CASE
                        WHEN LEFT(sch.SUJU_NO, 1) = 'S' THEN cust_s.CUST_NM
                        WHEN LEFT(sch.SUJU_NO, 1) = 'M' THEN cust_m.CUST_NM
                        ELSE '-'
                    END AS 판매처
                FROM sa_billsch sch 

                INNER JOIN (
                    SELECT SUJU_NO, MAX(CHG_CHASU) AS MAX_CHASU
                    FROM sa_billsch
                    WHERE COMP_CD = 'BANT' AND SITE_CD = '1000'
                    GROUP BY SUJU_NO
                ) latest
                ON sch.SUJU_NO = latest.SUJU_NO AND sch.CHG_CHASU = latest.MAX_CHASU

                LEFT JOIN sa_charge chg
                    ON sch.COMP_CD = chg.COMP_CD AND sch.SITE_CD = chg.SITE_CD
                    AND sch.SUJU_NO = chg.SUJU_NO AND sch.CHG_CHASU = chg.CHG_CHASU
                    AND sch.BILL_SEQ = chg.BILL_SEQ
                LEFT JOIN sa_ssujuinfo su
                    ON sch.COMP_CD = su.COMP_CD AND sch.SITE_CD = su.SITE_CD
                    AND sch.SUJU_NO = su.SUJU_NO AND sch.CHG_CHASU = su.CHG_CHASU
                LEFT JOIN sa_msujuinfo ma
                    ON sch.COMP_CD = ma.COMP_CD AND sch.SITE_CD = ma.SITE_CD
                    AND sch.SUJU_NO = ma.SUJU_NO AND sch.CHG_CHASU = ma.CHG_CHASU
                LEFT JOIN bs_cust cust_s
                    ON su.CH_CD = cust_s.CUST_CD AND su.COMP_CD = cust_s.COMP_CD AND su.SITE_CD = cust_s.SITE_CD
                LEFT JOIN bs_cust cust_m
                    ON ma.CH_CD = cust_m.CUST_CD AND ma.COMP_CD = cust_m.COMP_CD AND ma.SITE_CD = cust_m.SITE_CD
                WHERE sch.COMP_CD = 'BANT' AND sch.SITE_CD = '1000'  
            """

            params = []

            if from_date:
                query += " AND sch.BILL_DATE >= %s"
                params.append(from_date)
            if to_date:
                query += " AND sch.BILL_DATE <= %s"
                params.append(to_date)
            if suju_type in ['S', 'M']:
                query += " AND LEFT(sch.SUJU_NO, 1) = %s"
                params.append(suju_type)
            if unbilled_only:
                query += " AND (sch.TOT_AMT - IFNULL(chg.TOT_AMT, 0)) <> 0"

            query += " ORDER BY sch.BILL_DATE ASC"

            cursor.execute(query, params)
            results = cursor.fetchall()

    except Exception as e:
        print(f"[non_charge_list] 오류: {e}")
        return render_template('non_charge_list.html',
                               error=f"시스템 오류: {e}",
                               search_conditions=search_conditions)
    finally:
        if connection:
            connection.close()

    return render_template('non_charge_list.html',
                           results=results,
                           search_conditions=search_conditions)


############미청구내역 조회 end ################################################

############매입내역 조회 START ################################################

@app.route('/buy_list', methods=['GET', 'POST'])
def buy_list():
    today = datetime.now().strftime('%Y-%m-%d')
    results = []
    totals = {'공급가': 0, '부가세': 0, '합계금액': 0}
    dept_list = []

    # DB 연결
    connection = get_db_connection()
    if not connection:
        return render_template('buy_list.html',
                               error="DB 연결 실패",
                               search_conditions={})

    try:
        cursor = connection.cursor(dictionary=True)

        # 부서 드롭다운: comp_cd='BANT' AND dstb_type='A01'
        sql_dept = """
            SELECT dept_cd, dept_nm
              FROM bs_dept
             WHERE comp_cd = 'BANT'
               AND dstb_type = 'A01'
             ORDER BY dept_cd
        """
        cursor.execute(sql_dept)
        dept_list = cursor.fetchall()

        # 화면에 '전체' 옵션 추가
        dept_list = [{'dept_cd': '전체', 'dept_nm': '전체'}] + dept_list

        if request.method == 'POST':
            search_conditions = {
                'date_from': request.form.get('date_from', ''),
                'date_to': request.form.get('date_to', today),
                'suju_type': request.form.get('suju_type', '전체'),
                'dept_cd': request.form.get('dept_cd', '전체')
            }

            # 날짜 변환 (YYYY-MM-DD → YYYYMMDD)  ※ sa_buy.tax_date가 CHAR(8)이므로 변환 필요
            date_from_ymd = search_conditions['date_from'].replace("-", "") if search_conditions['date_from'] else "19000101"
            date_to_ymd = search_conditions['date_to'].replace("-", "") if search_conditions['date_to'] else today.replace("-", "")

            # 매입내역 조회 (요청 조건 반영)
            # - 수주번호: suju_no + '-' + chg_chasu(2자리)
            # - 수주담당: sa_(s/ms)sujuinfo → cm_user.empno → user_nm
            # - 수주부서: 위 empno → cm_user.dept_cd → bs_dept.dept_nm
            # - 수주구분: '일반'→ suju_no 첫글자 'S', 'MA'→ 'M', '전체'→ 제한없음
            # - 부서필터: 선택부서 or ('BANT','A01' 전체)
            sql = """
                SELECT 
                       CONCAT(b.suju_no, '-', LPAD(b.chg_chasu,2,'0')) AS 수주번호,
                       u.user_nm AS 수주담당,
                       d.dept_nm AS 수주부서,
                       b.po_no AS 발주번호,
                       b.po_name AS 발주명,
                       b.po_cd AS 구매처코드,
                       c.cust_nm AS 구매처명,
                       b.tax_date AS 매입일자,        -- CHAR(8), 템플릿에서 yyyy-mm-dd로 포맷
                       b.suprice_amt AS 공급가,
                       b.vat AS 부가세,
                       b.tot_amt AS 합계금액
                  FROM sa_buy b
                  JOIN bs_cust c
                    ON b.comp_cd = c.comp_cd
                   AND b.site_cd = c.site_cd
                   AND b.po_cd  = c.cust_cd
                  JOIN (
                        SELECT s.comp_cd, s.site_cd, s.suju_no, s.chg_chasu, s.empno
                          FROM sa_ssujuinfo s
                        UNION ALL
                        SELECT m.comp_cd, m.site_cd, m.suju_no, m.chg_chasu, m.empno
                          FROM sa_msujuinfo m
                  ) si
                    ON b.comp_cd   = si.comp_cd
                   AND b.site_cd   = si.site_cd
                   AND b.suju_no   = si.suju_no
                   AND b.chg_chasu = si.chg_chasu
                  JOIN cm_user u
                    ON u.comp_cd = b.comp_cd
                   AND u.empno   = si.empno
                  JOIN bs_dept d
                    ON d.comp_cd = b.comp_cd
                   AND d.site_cd = b.site_cd
                   AND d.dept_cd = u.dept_cd
                 WHERE b.tax_date BETWEEN %(date_from)s AND %(date_to)s
            """

            params = {'date_from': date_from_ymd, 'date_to': date_to_ymd}

            # 수주구분 필터
            suju_type = search_conditions['suju_type']
            if suju_type == '일반':
                sql += " AND LEFT(b.suju_no,1) = 'S' "
            elif suju_type == 'MA':
                sql += " AND LEFT(b.suju_no,1) = 'M' "

            # 부서 필터
            if search_conditions['dept_cd'] != '전체':
                sql += " AND d.dept_cd = %(dept_cd)s AND d.dstb_type = 'A01' AND d.comp_cd = 'BANT' "
                params['dept_cd'] = search_conditions['dept_cd']
            else:
                sql += " AND d.comp_cd = 'BANT' AND d.dstb_type = 'A01' "

            sql += " ORDER BY b.tax_date ASC, b.suju_no, b.chg_chasu "

            cursor.execute(sql, params)
            results = cursor.fetchall()

            if results:
                totals = {
                    '공급가': sum((row.get('공급가') or 0) for row in results),
                    '부가세': sum((row.get('부가세') or 0) for row in results),
                    '합계금액': sum((row.get('합계금액') or 0) for row in results),
                }

        else:
            # 최초 진입 시 기본값
            search_conditions = {'date_to': today, 'suju_type': '전체', 'dept_cd': '전체'}

        return render_template(
            'buy_list.html',
            results=results,
            totals=totals,
            search_conditions=search_conditions,
            dept_list=dept_list
        )

    except Exception as e:
        print(f"[buy_list] Error: {e}")
        return render_template('buy_list.html',
                               error=f"시스템 오류: {str(e)}",
                               search_conditions={'date_to': today},
                               dept_list=dept_list)
    finally:
        try:
            if cursor: cursor.close()
        except: pass
        try:
            if connection and connection.is_connected():
                connection.close()
        except: pass



############매입내역 조회 END ################################################



# 애플리케이션 실행  (서버에서 실행시 0.0.0.0)
if __name__ == '__main__':
    app.run(debug=True)
    #app.run(host='0.0.0.0', port=5002)   