import requests
import pandas as pd
import xml.etree.ElementTree as ET
import time
import os


SERVICE_KEY = os.getenv("API_SERVICE_KEY")

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
    """XML 노드에서 텍스트 추출 (없으면 '0' 반환)"""
    node = item.find(tag)
    return node.text.strip() if node is not None and node.text else "0"

def collect_data(target_type, start_year, end_year):

    if target_type == "officetel":
        url = "http://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"
        file_name = "officetel_data.csv"
        tags = {
            'name': 'offiNm',       # 단지명
            'area': 'excluUseAr'    # 전용면적
        }
        print(f"[오피스텔] 데이터 수집 시작... (저장파일명: {file_name})")
    else:
        url = "http://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"
        file_name = "townhouse_data.csv"
        tags = {
            'name': 'mhouseNm',     # 연립다세대명
            'area': 'excluUseAr'    # 전용면적
        }
        print(f"[연립다세대] 데이터 수집 시작... (저장파일명: {file_name})")

    all_data = []
    total_requests = 0
    
    # 기간 설정
    months = []
    for year in range(start_year, end_year + 1):
        for month in range(1, 13):
            months.append(f"{year}{str(month).zfill(2)}")
            
    # 반복문 시작
    for ymd in months:
        for code, district_name in SEOUL_DISTRICTS.items():
            
            params = {
                'serviceKey': SERVICE_KEY,
                'LAWD_CD': code,
                'DEAL_YMD': ymd,
                'numOfRows': '9999',
                'pageNo': '1'
            }
            
            try:
                response = requests.get(url, params=params, timeout=10)
                total_requests += 1
                
                if response.status_code == 200:
                    root = ET.fromstring(response.content)
                    res_code = root.find('.//resultCode')
                    
                    # 성공 코드 체크 ('00' 또는 '000')
                    if res_code is not None and res_code.text in ['00', '000']:
                        items = root.findall('.//item')
                        if items:
                            for item in items:
                                row = {
                                    'year_month': ymd,
                                    'district_code': code,
                                    'district_name': district_name,
                                    'dong': get_text(item, 'umdNm'),       # 법정동
                                    'jibun': get_text(item, 'jibun'),      # 지번
                                    'name': get_text(item, tags['name']),  # 건물명 (분기처리됨)
                                    'floor': get_text(item, 'floor'),      # 층
                                    'area': get_text(item, tags['area']),  # 전용면적
                                    'deposit': get_text(item, 'deposit').replace(',', ''),       # 보증금 (쉼표제거)
                                    'monthly_rent': get_text(item, 'monthlyRent').replace(',', ''), # 월세 (쉼표제거)
                                    'build_year': get_text(item, 'buildYear'), # 건축년도
                                    'deal_day': get_text(item, 'dealDay'),     # 계약일
                                    'type': target_type
                                }
                                all_data.append(row)
                                
                            print(f"{ymd} {district_name}: {len(items)}건 수집")
                        else:
                            print(f"pass - {ymd} {district_name}: 거래 없음")
                    else:
                        res_msg = root.find('.//resultMsg')
                        print(f"API 오류: {res_code.text} - {res_msg.text if res_msg else ''}")
                else:
                    print(f"HTTP 오류: {response.status_code}")
                
                time.sleep(0.3) # 트래픽 조절
                
            except Exception as e:
                print(f"에러 발생 ({ymd} {district_name}): {e}")
                time.sleep(1)

            # 100회 요청마다 중간 저장
            if total_requests % 100 == 0:
                if all_data:
                    pd.DataFrame(all_data).to_csv(file_name, index=False, encoding='utf-8-sig')
                    print(f"중간 저장 완료 ({len(all_data)}건)")

    # 최종 저장
    if all_data:
        final_df = pd.DataFrame(all_data)
        final_df.to_csv(file_name, index=False, encoding='utf-8-sig')
        print(f"\n최종 완료! {file_name} 저장됨. (총 {len(final_df)}건)")
        # 데이터 샘플 출력 (확인용)
        print(final_df[['name', 'deposit', 'monthly_rent']].head())
    else:
        print("\n데이터가 수집되지 않았습니다. API 키나 기간을 확인하세요.")

if __name__ == "__main__":
    # 1. 오피스텔 수집
    #collect_data("officetel", 2022, 2025)
    
    # 2. 연립다세대 수집
    collect_data("townhouse", 2022, 2025)