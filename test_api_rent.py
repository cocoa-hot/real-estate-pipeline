import requests
import xml.etree.ElementTree as ET

SERVICE_KEY = "YOUR_API_SERVICE_KEY_HERE"

def get_text(item, tag):
    """XML 태그에서 텍스트를 안전하게 추출하는 함수"""
    node = item.find(tag)
    return node.text.strip() if node is not None and node.text else "0"

def test_rent_api(target_type="officetel"):
    # 1. 목표에 따라 URL 변경
    if target_type == "officetel":
        # 오피스텔 전월세 URL
        url = "https://apis.data.go.kr/1613000/RTMSDataSvcOffiRent/getRTMSDataSvcOffiRent"
        type_name = "오피스텔"
    else:
        # 연립다세대 전월세 URL
        url = "http://apis.data.go.kr/1613000/RTMSDataSvcRHRent/getRTMSDataSvcRHRent"
        type_name = "연립다세대"

    params = {
        'serviceKey': SERVICE_KEY,
        'LAWD_CD': '11110',
        'DEAL_YMD': '202401'
    }

    print(f"\n[{type_name} 데이터 요청 시작...]")
    
    try:
        response = requests.get(url, params=params, timeout=30)
        
        if response.status_code == 200:
            root = ET.fromstring(response.content)
            res_code = root.find('.//resultCode')
            
            if res_code is not None and res_code.text == '00':
                items = root.findall('.//item')
                if items:
                    print(f"수집 성공! (총 {len(items)}건)")
                    for i, item in enumerate(items[:3]):
                        # 전월세 데이터 파싱
                        # 연립다세대는 '연립다세대', 오피스텔은 '단지' 또는 '오피스텔' 태그를 씀
                        name = get_text(item, '연립다세대') if target_type == 'townhouse' else get_text(item, '단지')
                        deposit = get_text(item, '보증금액')
                        rent = get_text(item, '월세금액')
                        floor = get_text(item, '층')
                        
                        print(f"[{i+1}] {name} ({floor}층) | 보증금: {deposit}만원 / 월세: {rent}만원")
                else:
                    print("데이터 없음 (거래 내역 없음)")
            else:
                res_msg = root.find('.//resultMsg')
                print(f"API 에러: {res_msg.text if res_msg is not None else '코드 확인 필요'}")
        else:
            print(f"HTTP 에러: {response.status_code}")

    except Exception as e:
        print(f"시스템 에러: {e}")

if __name__ == "__main__":
    test_rent_api("officetel") # 오피스텔 테스트
    test_rent_api("townhouse") # 연립다세대 테스트