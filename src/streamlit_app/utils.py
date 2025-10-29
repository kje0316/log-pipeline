import os
import sys
import pandas as pd
import streamlit as st
from sqlalchemy import create_engine

# 프로젝트 루트를 sys.path에 추가
project_root = os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
sys.path.append(project_root)

from src.config.config import Config

# PostgreSQL 설정을 Config 클래스에서 가져오기
POSTGRES_HOST = Config.POSTGRES_HOST
POSTGRES_PORT = Config.POSTGRES_PORT
POSTGRES_DB = Config.POSTGRES_DB
POSTGRES_USER = Config.POSTGRES_USER
POSTGRES_PASSWORD = Config.POSTGRES_PASSWORD


@st.cache_data
def load_table_from_postgres(table_name: str, columns_to_int: list = None) -> pd.DataFrame:
    """
    PostgreSQL 데이터베이스에서 지정된 테이블을 로드하고, 필요 시 데이터 타입 정리를 수행합니다.
    Streamlit의 캐싱을 적용하여 앱 성능을 최적화합니다.

    Args:
        table_name (str): 로드할 PostgreSQL 테이블 이름.
        columns_to_int (list): 정수형으로 변환할 컬럼 이름 리스트.
        
    Returns:
        pd.DataFrame: 로드된 DataFrame 또는 빈 DataFrame (오류 시).
    """

    try:
        # SQLAlchemy 엔진 생성
        engine = create_engine(
            f"postgresql://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{POSTGRES_HOST}:{POSTGRES_PORT}/{POSTGRES_DB}"
        )

        # 테이블 로드
        df = pd.read_sql_table(table_name, engine)

        # 데이터 타입 정리 (정수형 변환)
        if columns_to_int:
            for col_name in columns_to_int:
                if col_name in df.columns:
                    df[col_name] = pd.to_numeric(df[col_name], errors="coerce").fillna(0).astype(int)

        # 날짜형 컬럼 처리 (예: date_key)
        if "date_key" in df.columns:
            df["date_key"] = pd.to_datetime(df["date_key"], errors="coerce")

        return df

    except Exception as e:
        st.error(f"❌ 데이터 로드 중 오류 발생: {e}")
        return pd.DataFrame()
