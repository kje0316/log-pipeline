import streamlit as st
import pandas as pd


# 페이지 설정을 한 번만 해줍니다.
st.set_page_config(
    page_title="직방 데이터 분석 대시보드",
    layout="wide"
)

st.title("🏡 직방 데이터 분석 대시보드: 환영합니다!")
st.write("왼쪽 사이드바에서 원하는 분석 페이지를 선택해주세요.")

st.markdown("""
### 프로젝트 개요
이 대시보드는 직방 로그 데이터를 Spark Batch 파이프라인으로 처리한 결과를 기반으로
지역별 가격 트렌드와 인기 매물 순위를 분석합니다.

- **데이터 소스:** CSV 파일 (Spark Batch 결과)
- **분석 주제:** - 지역별 인기 매물 Top 10
    - 시간대별/유형별 평균 가격 트렌드
""")