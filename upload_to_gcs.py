from google.cloud import storage
import os

# 1. 설정 정보
KEY_PATH = "keys/service-account-key.json" 
BUCKET_NAME = "your-gcs-bucket-name"

def upload_blob(bucket_name, source_file_name, destination_blob_name):
    """파일을 GCS 버킷에 업로드합니다."""
    try:
        # 클라이언트 생성
        storage_client = storage.Client.from_service_account_json(KEY_PATH)
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)

        # 업로드 수행
        blob.upload_from_filename(source_file_name)

        print(f"업로드 성공: {source_file_name} -> gs://{bucket_name}/{destination_blob_name}")
        
    except Exception as e:
        print(f"업로드 실패 ({source_file_name}): {e}")

if __name__ == "__main__":
    # 1. 오피스텔 데이터 업로드
    if os.path.exists("officetel_data.csv"):
        upload_blob(BUCKET_NAME, "officetel_data.csv", "raw/officetel/officetel_data.csv")
    else:
        print("officetel_data.csv 파일이 없습니다.")

    # 2. 연립다세대 데이터 업로드
    if os.path.exists("townhouse_data.csv"):
        upload_blob(BUCKET_NAME, "townhouse_data.csv", "raw/townhouse/townhouse_data.csv")
    else:
        print("townhouse_data.csv 파일이 없습니다.")