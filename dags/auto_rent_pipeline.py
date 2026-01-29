from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
import os
from google.cloud import bigquery, storage



SERVICE_KEY = os.getenv("API_SERVICE_KEY", "your-api-service-key")
PROJECT_ID = os.getenv("GCP_PROJECT_ID", "your-gcp-project-id")
KEY_FILE_NAME = os.getenv("KEY_FILE_NAME", "key.json")
BUCKET_NAME = os.getenv("GCS_BUCKET_NAME", "your-gcs-bucket-name")

# GCP 설정
DATASET_ID = "real_estate_data"
TABLE_ID = "raw_rent_transactions"



KEY_PATH = f"/opt/airflow/keys/{KEY_FILE_NAME}"

# 서울시 구 코드
SEOUL_DISTRICTS = {
    '11110': '종로구', '11140': '중구', '11170': '용산구', '11200': '성동구', 
    '11215': '광진구', '11230': '동대문구', '11260': '중랑구', '11290': '성북구', 
    '11305': '강북구', '11320': '도봉구', '11350': '노원구', '11380': '은평구', 
    '11410': '서대문구', '11440': '마포구', '11470': '양천구', '11500': '강서구', 
    '11530': '구로구', '11545': '금천구', '11560': '영등포구', '11590': '동작구', 
    '11620': '관악구', '11650': '서초구', '11680': '강남구', '11710': '송파구', 
    '11740': '강동구'
}



def get_text(item, tag):
    node = item.find(tag)
    return node.text.strip() if node is not None and node.text else "0"

def fetch_data_from_api(**context):
    """
    Airflow 실행 날짜(Logical Date)의 '지난 달' 데이터를 수집합니다.
    예: 2월 1일에 실행 -> 1월 데이터를 수집
    """
    # Airflow가 넘겨주는 실행 기준 날짜
    logical_date = context['logical_date']
    # 지난 달 계산 (예: 2025-02-01 -> 202501)
    target_date = logical_date - relativedelta(months=1)
    target_ym = target_date.strftime("%Y%m")
    
    print(f"수집 대상 월: {target_ym}")
    
    # 저장할 임시 경로
    save_path = f"/tmp/rent_data_{target_ym}.csv"
    all_data = []

    # 수집 대상: 오피스텔 & 연립다세대
    targets = [
        {"type": "officetel", "url": "http://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent", "name_tag": "offiNm", "area_tag": "excluUseAr"},
        {"type": "townhouse", "url": "http://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent", "name_tag": "mhouseNm", "area_tag": "excluUseAr"}
    ]

    for target in targets:
        print(f"{target['type']} 수집 시작...")
        for code, district_name in SEOUL_DISTRICTS.items():
            params = {
                'serviceKey': SERVICE_KEY,
                'LAWD_CD': code,
                'DEAL_YMD': target_ym,
                'numOfRows': '9999',
                'pageNo': '1'
            }
            try:
                response = requests.get(target['url'], params=params, timeout=10)
                if response.status_code == 200:
                    root = ET.fromstring(response.content)
                    items = root.findall('.//item')
                    
                    for item in items:
                        row = {
                            'year_month': target_ym,
                            'district_code': code,
                            'district_name': district_name,
                            'dong': get_text(item, 'umdNm'),
                            'jibun': get_text(item, 'jibun'),
                            'name': get_text(item, target['name_tag']),
                            'floor': get_text(item, 'floor'),
                            'area': get_text(item, target['area_tag']),
                            'deposit': get_text(item, 'deposit').replace(',', ''),
                            'monthly_rent': get_text(item, 'monthlyRent').replace(',', ''),
                            'build_year': get_text(item, 'buildYear'),
                            'deal_day': get_text(item, 'dealDay'),
                            'type': target['type']
                        }
                        all_data.append(row)
                time.sleep(0.1) # 트래픽 조절
            except Exception as e:
                print(f"Error ({district_name}): {e}")

    # CSV 저장
    if all_data:
        pd.DataFrame(all_data).to_csv(save_path, index=False, encoding='utf-8-sig')
        print(f"수집 완료: {len(all_data)}건 -> {save_path}")
        return save_path
    else:
        print("수집된 데이터가 없습니다.")
        return None

def upload_to_gcs(**context):
    """수집된 CSV를 GCS에 업로드"""
    # 이전 Task(fetch_data)에서 반환한 파일 경로를 받아옴
    file_path = context['task_instance'].xcom_pull(task_ids='fetch_data_task')
    
    if not file_path or not os.path.exists(file_path):
        print("업로드할 파일이 없습니다.")
        return

    # 파일명에서 년월 추출 (rent_data_202401.csv -> 202401)
    ym = file_path.split('_')[-1].split('.')[0]
    destination_blob_name = f"raw/monthly/{ym}/rent_data.csv"

    client = storage.Client.from_service_account_json(KEY_PATH)
    bucket = client.bucket(BUCKET_NAME)
    blob = bucket.blob(destination_blob_name)
    blob.upload_from_filename(file_path)
    
    print(f"GCS 업로드 완료: gs://{BUCKET_NAME}/{destination_blob_name}")
    return f"gs://{BUCKET_NAME}/{destination_blob_name}"

def load_to_bq(**context):
    """GCS 파일을 BigQuery에 적재 (Append 모드)"""
    gcs_uri = context['task_instance'].xcom_pull(task_ids='upload_to_gcs_task')
    
    if not gcs_uri:
        print("적재할 GCS 경로가 없습니다.")
        return

    client = bigquery.Client.from_service_account_json(KEY_PATH)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    schema = [
        bigquery.SchemaField("year_month", "STRING"),
        bigquery.SchemaField("district_code", "STRING"),
        bigquery.SchemaField("district_name", "STRING"),
        bigquery.SchemaField("dong", "STRING"),
        bigquery.SchemaField("jibun", "STRING"),
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("floor", "INTEGER"),
        bigquery.SchemaField("area", "FLOAT"),
        bigquery.SchemaField("deposit", "INTEGER"),
        bigquery.SchemaField("monthly_rent", "INTEGER"),
        bigquery.SchemaField("build_year", "INTEGER"),
        bigquery.SchemaField("deal_day", "STRING"),
        bigquery.SchemaField("type", "STRING"),
    ]

    job_config = bigquery.LoadJobConfig(
        schema=schema,  # 자동 감지(autodetect) 대신 이 스키마를 사용
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        write_disposition="WRITE_APPEND", 
    )

    try:
        load_job = client.load_table_from_uri(gcs_uri, table_ref, job_config=job_config)
        load_job.result()
        print(f"BigQuery 적재 완료: {gcs_uri}")
    except Exception as e:
        print(f"적재 실패 상세 에러: {e}")
        raise e

# DAG 정의
default_args = {
    'owner': 'yejin',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_data(**context):

    # 1. BigQuery 클라이언트 연결
    client = bigquery.Client.from_service_account_json(KEY_PATH)

    # 2. 테이블 경로 설정
    # 원본 테이블
    source_table = f"{PROJECT_ID}.{DATASET_ID}.raw_rent_transactions"
    # 새로 만들 가공 테이블
    target_table = f"{PROJECT_ID}.{DATASET_ID}.rent_market_view"

    # 3. SQL 쿼리
    query = f"""
    CREATE OR REPLACE TABLE `{target_table}` AS
    SELECT
        district_name,
        dong,
        name AS building_name,
        build_year,
        floor,
        
        -- 날짜 분해
        PARSE_DATE('%Y%m', year_month) AS deal_date,
        EXTRACT(YEAR FROM PARSE_DATE('%Y%m', year_month)) AS deal_year,
        EXTRACT(MONTH FROM PARSE_DATE('%Y%m', year_month)) AS deal_month,

        -- 면적 계산
        area AS area_m2,
        ROUND(area / 3.3058, 1) AS area_pyung, -- 평수

        -- 금액 변환
        deposit,
        monthly_rent,

        -- 보기 좋은 문자열
        CASE 
            WHEN deposit >= 10000 THEN 
                CONCAT(CAST(FLOOR(deposit / 10000) AS STRING), '억 ', 
                       IF(MOD(deposit, 10000) > 0, CONCAT(CAST(MOD(deposit, 10000) AS STRING), '만원'), ''))
            ELSE CONCAT(CAST(deposit AS STRING), '만원')
        END AS deposit_korean,

        -- 전세 평단가
        ROUND(deposit / (area / 3.3058), 0) AS deposit_per_pyung,
        
        -- 월세 평단가
        CASE
            WHEN monthly_rent > 0 THEN ROUND(monthly_rent / (area / 3.3058), 0)
            ELSE 0
        END AS rent_per_pyung,

        IF(monthly_rent = 0, 'Jeonse', 'Monthly') AS rent_type

    FROM `{source_table}`
    ORDER BY deal_date DESC
    """

    try:
        query_job = client.query(query)  # API 요청
        query_job.result()  # 결과가 나올 때까지 기다림
        print(f"테이블 생성됨: {target_table}")
    except Exception as e:
        print(f"에러 발생: {e}")
        raise e

with DAG(
    dag_id='auto_rent_pipeline',
    default_args=default_args,
    description='서울시 전월세 실거래가 월간 자동 수집',
    schedule_interval='@monthly', # 매월 1일 0시에 실행
    start_date=datetime(2026, 1, 1), # 시작일 (과거 데이터 채우기 가능)
    catchup=False, # True로 하면 과거 안 돌린 기간을 한꺼번에 다 돌려버림 주의
    tags=['real_estate', 'etl']
) as dag:

    # 1. 수집 Task
    t1 = PythonOperator(
        task_id='fetch_data_task',
        python_callable=fetch_data_from_api,
        provide_context=True
    )

    # 2. GCS 업로드 Task
    t2 = PythonOperator(
        task_id='upload_to_gcs_task',
        python_callable=upload_to_gcs,
        provide_context=True
    )

    # 3. BigQuery 적재 Task
    t3 = PythonOperator(
        task_id='load_to_bq_task',
        python_callable=load_to_bq,
        provide_context=True
    )

    # 실행 순서
    t1 >> t2 >> t3