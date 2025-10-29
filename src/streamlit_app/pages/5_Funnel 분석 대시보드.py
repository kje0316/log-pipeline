import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import load_table_from_postgres

# 페이지 설정
st.set_page_config(
    page_title="Funnel 분석 대시보드",
    page_icon="🔍",
    layout="wide"
)

st.title("🔍 전환 Funnel 분석 대시보드")
st.markdown("**4단계 Funnel: 검색 → 상세 조회 → 관심 표시 → 구매**")
st.markdown("---")

# 데이터 로드 (4단계 Funnel)
df = load_table_from_postgres(
    table_name="funnel_analysis",
    columns_to_int=["total_sessions", "search_count", "detail_count", "interest_count", "purchase_count"]
)

if df.empty:
    st.warning("⚠️ Funnel 분석 데이터가 없습니다. 배치 파이프라인을 실행해주세요.")
    st.stop()

# 날짜 컬럼 확인 및 변환
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

# 전체 기간 집계
total_data = df[[
    'search_count', 'detail_count', 'interest_count', 'purchase_count'
]].sum()

# Funnel 시각화
st.subheader("🔄 전환 Funnel 흐름")

# Funnel 데이터 준비 (4단계)
funnel_stages = ['1. 검색', '2. 상세 조회', '3. 관심 표시', '4. 구매']
funnel_values = [
    total_data['search_count'],
    total_data['detail_count'],
    total_data['interest_count'],
    total_data['purchase_count']
]

# Funnel 차트
fig_funnel = go.Figure(go.Funnel(
    y=funnel_stages,
    x=funnel_values,
    textposition="inside",
    textinfo="value+percent previous+percent initial",
    marker=dict(color=['#1f77b4', '#ff7f0e', '#2ca02c', '#d62728']),
    connector=dict(line=dict(color='gray', width=2))
))

fig_funnel.update_layout(
    title=f'전환 Funnel (총 기간: {len(df)}일)',
    height=500
)

st.plotly_chart(fig_funnel, use_container_width=True)

# 전환율 Metrics
st.markdown("---")
st.subheader("📊 주요 전환율 지표")

col1, col2, col3, col4 = st.columns(4)

# 각 단계별 전환율 계산
search_to_detail = (total_data['detail_count'] / total_data['search_count'] * 100) if total_data['search_count'] > 0 else 0
detail_to_interest = (total_data['interest_count'] / total_data['detail_count'] * 100) if total_data['detail_count'] > 0 else 0
interest_to_purchase = (total_data['purchase_count'] / total_data['interest_count'] * 100) if total_data['interest_count'] > 0 else 0
overall = (total_data['purchase_count'] / total_data['search_count'] * 100) if total_data['search_count'] > 0 else 0

with col1:
    st.metric("검색 → 상세", f"{search_to_detail:.1f}%")

with col2:
    st.metric("상세 → 관심", f"{detail_to_interest:.1f}%")

with col3:
    st.metric("관심 → 구매", f"{interest_to_purchase:.1f}%")

with col4:
    st.metric("전체 전환율", f"{overall:.2f}%", help="검색에서 구매까지의 전체 전환율")

# 단계별 볼륨
st.markdown("---")
st.subheader("📊 단계별 사용자 수")

col1, col2, col3, col4 = st.columns(4)

with col1:
    st.metric("검색", f"{int(total_data['search_count']):,}")

with col2:
    st.metric("상세 조회", f"{int(total_data['detail_count']):,}")

with col3:
    st.metric("관심 표시", f"{int(total_data['interest_count']):,}")

with col4:
    st.metric("구매", f"{int(total_data['purchase_count']):,}")

# 일별 데이터가 여러 개인 경우에만 트렌드 표시
if len(df) > 1:
    st.markdown("---")
    st.subheader("📈 일별 전환율 트렌드")

    fig_trend = go.Figure()

    fig_trend.add_trace(go.Scatter(
        x=df['date'],
        y=df['search_to_detail_rate'],
        mode='lines+markers',
        name='검색 → 상세',
        line=dict(color='#1f77b4', width=2)
    ))

    fig_trend.add_trace(go.Scatter(
        x=df['date'],
        y=df['detail_to_interest_rate'],
        mode='lines+markers',
        name='상세 → 관심',
        line=dict(color='#ff7f0e', width=2)
    ))

    fig_trend.add_trace(go.Scatter(
        x=df['date'],
        y=df['interest_to_purchase_rate'],
        mode='lines+markers',
        name='관심 → 구매',
        line=dict(color='#2ca02c', width=2)
    ))

    fig_trend.update_layout(
        title='일별 단계별 전환율',
        xaxis_title='날짜',
        yaxis_title='전환율 (%)',
        hovermode='x unified',
        legend=dict(orientation="h", yanchor="bottom", y=1.02, xanchor="right", x=1)
    )

    st.plotly_chart(fig_trend, use_container_width=True)

    # 전체 전환율 트렌드
    st.subheader("🎯 전체 전환율 트렌드 (검색 → 구매)")

    fig_overall = go.Figure()

    fig_overall.add_trace(go.Scatter(
        x=df['date'],
        y=df['overall_conversion_rate'],
        mode='lines+markers',
        name='전체 전환율',
        line=dict(color='#d62728', width=3),
        marker=dict(size=8),
        fill='tozeroy',
        fillcolor='rgba(214, 39, 40, 0.2)'
    ))

    fig_overall.add_hline(
        y=df['overall_conversion_rate'].mean(),
        line_dash="dash",
        line_color="gray",
        annotation_text=f"평균: {df['overall_conversion_rate'].mean():.2f}%",
        annotation_position="right"
    )

    fig_overall.update_layout(
        title='일별 전체 전환율',
        xaxis_title='날짜',
        yaxis_title='전환율 (%)',
        hovermode='x unified'
    )

    st.plotly_chart(fig_overall, use_container_width=True)

# 전환율 비교 (바 차트)
st.markdown("---")
st.subheader("📊 단계별 전환율 비교")

conversion_rates = {
    '검색 → 상세': search_to_detail,
    '상세 → 관심': detail_to_interest,
    '관심 → 구매': interest_to_purchase
}

fig_bar = go.Figure(data=[
    go.Bar(
        x=list(conversion_rates.keys()),
        y=list(conversion_rates.values()),
        text=[f"{v:.1f}%" for v in conversion_rates.values()],
        textposition='auto',
        marker_color=['#1f77b4', '#ff7f0e', '#2ca02c']
    )
])

fig_bar.update_layout(
    title='각 단계별 전환율',
    yaxis_title='전환율 (%)',
    xaxis_title='Funnel 단계',
    showlegend=False,
    height=400
)

st.plotly_chart(fig_bar, use_container_width=True)

# 데이터 테이블
st.markdown("---")
st.subheader("📋 Funnel 데이터 상세")

display_df = df[[
    'date', 'total_sessions', 'search_count', 'detail_count',
    'interest_count', 'purchase_count', 'overall_conversion_rate'
]].copy()

display_df.columns = [
    '날짜', '총 세션', '검색', '상세',
    '관심', '구매', '전체 전환율(%)'
]

st.dataframe(
    display_df.style.format({
        '총 세션': '{:,.0f}',
        '검색': '{:,.0f}',
        '상세': '{:,.0f}',
        '관심': '{:,.0f}',
        '구매': '{:,.0f}',
        '전체 전환율(%)': '{:.2f}'
    }),
    use_container_width=True
)

# 인사이트
st.markdown("---")
st.subheader("💡 주요 인사이트")

col1, col2, col3 = st.columns(3)

with col1:
    st.info(f"""
    **Funnel 요약**

    - 총 세션: {int(df['total_sessions'].sum()):,}개
    - 검색 시작: {int(total_data['search_count']):,}명
    - 최종 구매: {int(total_data['purchase_count']):,}명
    """)

with col2:
    # 가장 큰 이탈이 발생하는 단계
    dropout_rates = {
        '검색→상세': 100 - search_to_detail,
        '상세→관심': 100 - detail_to_interest,
        '관심→구매': 100 - interest_to_purchase
    }
    max_dropout_stage = max(dropout_rates, key=dropout_rates.get)
    max_dropout_rate = dropout_rates[max_dropout_stage]

    st.warning(f"""
    **최대 이탈 구간**

    {max_dropout_stage}
    - 이탈률: {max_dropout_rate:.1f}%
    - 개선 필요!
    """)

with col3:
    # 잠재 매출 계산
    current_rate = overall
    target_rate = current_rate * 1.5  # 50% 개선 목표
    potential_purchases = (total_data['search_count'] * target_rate / 100) - total_data['purchase_count']

    st.success(f"""
    **개선 잠재력**

    현재 전환율: {current_rate:.2f}%
    50% 개선 시: {target_rate:.2f}%
    추가 예상 구매: +{int(potential_purchases):,}건
    """)

# 개선 제안
st.markdown("---")
st.subheader("🚀 전환율 개선 제안")

col1, col2 = st.columns(2)

with col1:
    st.markdown(f"""
    #### 단계별 개선 포인트

    **1. 검색 → 상세 ({search_to_detail:.1f}%)**
    - 검색 결과 품질 개선
    - 관련성 높은 매물 추천

    **2. 상세 → 관심 ({detail_to_interest:.1f}%)**
    - 상세 페이지 UX 개선
    - 매물 정보 충실화

    **3. 관심 → 구매 ({interest_to_purchase:.1f}%)**
    - 구매 프로세스 간소화
    - 인센티브/프로모션 제공
    """)

with col2:
    st.markdown("""
    #### A/B 테스트 제안

    - 검색 필터 개선 테스트
    - 상세 페이지 레이아웃 변경
    - 위시리스트 기능 개선
    - 원클릭 구매 기능 추가

    #### 추천 분석

    - 이탈 사용자 행동 분석
    - 고전환 사용자 패턴 분석
    - 경쟁사 Funnel 벤치마킹
    """)
