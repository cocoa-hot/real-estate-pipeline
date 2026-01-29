from google.cloud import bigquery
import os


KEY_PATH = "keys/service-account-key.json" 
PROJECT_ID = "your-gcp-project-id"
BUCKET_NAME = "your-gcs-bucket-name"
DATASET_ID = "real_estate_data"  # 데이터셋 이름
TABLE_ID = "raw_rent_transactions"  # 새로 만들 테이블 이름


def load_to_bq():
    # 1. 클라이언트 연결
    client = bigquery.Client.from_service_account_json(KEY_PATH)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    # 2. 테이블 스키마 정의 (CSV 파일 헤더와 순서/타입이 일치해야 함)
    # CSV 헤더: year_month, district_code, district_name, dong, jibun, name, floor, area, deposit, monthly_rent, build_year, deal_day, type
    schema = [
        bigquery.SchemaField("year_month", "STRING"),      # 계약년월 (예: 202401)
        bigquery.SchemaField("district_code", "STRING"),   # 구코드
        bigquery.SchemaField("district_name", "STRING"),   # 구이름
        bigquery.SchemaField("dong", "STRING"),            # 법정동
        bigquery.SchemaField("jibun", "STRING"),           # 지번
        bigquery.SchemaField("name", "STRING"),            # 건물명 (단지명/연립명)
        bigquery.SchemaField("floor", "INTEGER"),          # 층
        bigquery.SchemaField("area", "FLOAT"),             # 전용면적
        bigquery.SchemaField("deposit", "INTEGER"),        # 보증금 (만원)
        bigquery.SchemaField("monthly_rent", "INTEGER"),   # 월세 (만원)
        bigquery.SchemaField("build_year", "INTEGER"),     # 건축년도
        bigquery.SchemaField("deal_day", "STRING"),        # 계약일
        bigquery.SchemaField("type", "STRING"),            # 구분 (officetel / townhouse)
    ]

    # 3. 적재 설정
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,                # CSV 헤더(첫 줄) 건너뛰기
        write_disposition="WRITE_TRUNCATE", # 실행할 때마다 기존 데이터 지우고 새로 쓰기 (중복 방지용)
    )

    # 4. 적재할 파일 목록 (GCS 경로)
    # 오피스텔과 연립다세대 파일을 한 번에 리스트로 묶어서 로드
    uris = [
        f"gs://{BUCKET_NAME}/raw/officetel/officetel_data.csv",
        f"gs://{BUCKET_NAME}/raw/townhouse/townhouse_data.csv"
    ]

    print(f"BigQuery 적재 시작... 대상 테이블: {table_ref}")
    
    try:
        # 적재 작업 시작
        load_job = client.load_table_from_uri(
            uris, table_ref, job_config=job_config
        )
        
        load_job.result()  # 작업 완료 대기

        # 결과 확인
        destination_table = client.get_table(table_ref)
        print(f"적재 완료: 총 {destination_table.num_rows}행이 저장되었습니다.")
        
    except Exception as e:
        print(f"적재 실패: {e}")

if __name__ == "__main__":
    load_to_bq()