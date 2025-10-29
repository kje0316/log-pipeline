import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import load_table_from_postgres

# 페이지 설정
st.set_page_config(
    page_title="세션 분석 대시보드",
    page_icon="📊",
    layout="wide"
)

st.title("📊 세션 분석 대시보드")
st.markdown("---")

# 데이터 로드
df = load_table_from_postgres(
    table_name="session_analysis",
    columns_to_int=["total_sessions", "unique_users"]
)

if df.empty:
    st.warning("⚠️ 세션 분석 데이터가 없습니다. 배치 파이프라인을 실행해주세요.")
    st.stop()

# 날짜 컬럼 확인 및 변환
if 'date' in df.columns:
    df['date'] = pd.to_datetime(df['date'])

# Metrics 표시
col1, col2, col3, col4 = st.columns(4)

with col1:
    total_sessions = df['total_sessions'].sum()
    st.metric("총 세션 수", f"{total_sessions:,}")

with col2:
    avg_events = df['avg_events_per_session'].mean()
    st.metric("평균 세션당 이벤트", f"{avg_events:.2f}")

with col3:
    avg_duration = df['avg_session_duration_sec'].mean()
    st.metric("평균 세션 시간", f"{avg_duration:.1f}초")

with col4:
    avg_bounce_rate = df['bounce_rate_pct'].mean()
    st.metric("평균 이탈률", f"{avg_bounce_rate:.1f}%")

st.markdown("---")

# 날짜별 세션 지표 트렌드
st.subheader("📈 날짜별 세션 지표 트렌드")

# 데이터가 1일치만 있을 때는 bar 차트, 여러 날일 때는 line 차트
if len(df) == 1:
    # 세션 수 (bar 차트)
    fig1 = px.bar(
        df,
        x='date',
        y='total_sessions',
        title='일별 총 세션 수',
        labels={'date': '날짜', 'total_sessions': '세션 수'},
        text='total_sessions'
    )
    fig1.update_traces(marker_color='#1f77b4', texttemplate='%{text:,}', textposition='outside')
    fig1.update_layout(hovermode='x unified', showlegend=False)
    st.plotly_chart(fig1, use_container_width=True)

    # 세션 품질 지표
    col1, col2 = st.columns(2)

    with col1:
        fig2 = px.bar(
            df,
            x='date',
            y='avg_events_per_session',
            title='일별 평균 세션당 이벤트 수',
            labels={'date': '날짜', 'avg_events_per_session': '평균 이벤트 수'},
            text='avg_events_per_session'
        )
        fig2.update_traces(marker_color='#2ca02c', texttemplate='%{text:.2f}', textposition='outside')
        fig2.update_layout(hovermode='x unified', showlegend=False)
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        fig3 = px.bar(
            df,
            x='date',
            y='avg_session_duration_sec',
            title='일별 평균 세션 시간 (초)',
            labels={'date': '날짜', 'avg_session_duration_sec': '평균 세션 시간 (초)'},
            text='avg_session_duration_sec'
        )
        fig3.update_traces(marker_color='#ff7f0e', texttemplate='%{text:.1f}', textposition='outside')
        fig3.update_layout(hovermode='x unified', showlegend=False)
        st.plotly_chart(fig3, use_container_width=True)
else:
    # 세션 수 트렌드 (line 차트)
    fig1 = px.line(
        df,
        x='date',
        y='total_sessions',
        title='일별 총 세션 수',
        labels={'date': '날짜', 'total_sessions': '세션 수'}
    )
    fig1.update_traces(line_color='#1f77b4', line_width=2)
    fig1.update_layout(hovermode='x unified')
    st.plotly_chart(fig1, use_container_width=True)

    # 세션 품질 지표
    col1, col2 = st.columns(2)

    with col1:
        fig2 = px.line(
            df,
            x='date',
            y='avg_events_per_session',
            title='일별 평균 세션당 이벤트 수',
            labels={'date': '날짜', 'avg_events_per_session': '평균 이벤트 수'}
        )
        fig2.update_traces(line_color='#2ca02c', line_width=2)
        fig2.update_layout(hovermode='x unified')
        st.plotly_chart(fig2, use_container_width=True)

    with col2:
        fig3 = px.line(
            df,
            x='date',
            y='avg_session_duration_sec',
            title='일별 평균 세션 시간 (초)',
            labels={'date': '날짜', 'avg_session_duration_sec': '평균 세션 시간 (초)'}
        )
        fig3.update_traces(line_color='#ff7f0e', line_width=2)
        fig3.update_layout(hovermode='x unified')
        st.plotly_chart(fig3, use_container_width=True)

# 이탈률 분석
st.markdown("---")
st.subheader("🚪 이탈률 분석")

if len(df) == 1:
    # 단일 날짜 데이터는 bar 차트
    fig4 = px.bar(
        df,
        x='date',
        y='bounce_rate_pct',
        title='일별 이탈률 (단일 이벤트 세션 비율)',
        labels={'date': '날짜', 'bounce_rate_pct': '이탈률 (%)'},
        text='bounce_rate_pct'
    )
    fig4.update_traces(marker_color='#d62728', texttemplate='%{text:.1f}%', textposition='outside')
    fig4.update_layout(hovermode='x unified', showlegend=False, yaxis=dict(range=[0, 100]))
else:
    # 여러 날짜 데이터는 line 차트
    fig4 = go.Figure()

    fig4.add_trace(go.Scatter(
        x=df['date'],
        y=df['bounce_rate_pct'],
        mode='lines+markers',
        name='이탈률',
        line=dict(color='#d62728', width=2),
        marker=dict(size=6)
    ))

    fig4.add_hline(
        y=df['bounce_rate_pct'].mean(),
        line_dash="dash",
        line_color="gray",
        annotation_text=f"평균: {df['bounce_rate_pct'].mean():.1f}%",
        annotation_position="right"
    )

    fig4.update_layout(
        title='일별 이탈률 (단일 이벤트 세션 비율)',
        xaxis_title='날짜',
        yaxis_title='이탈률 (%)',
        hovermode='x unified',
        yaxis=dict(range=[0, 100])
    )

st.plotly_chart(fig4, use_container_width=True)

# 사용자 참여도 분석
st.markdown("---")
st.subheader("👥 사용자 참여도")

col1, col2 = st.columns(2)

# 사용자당 평균 세션 수 계산
df['sessions_per_user'] = df['total_sessions'] / df['unique_users']

if len(df) == 1:
    # 단일 날짜 데이터는 bar 차트
    with col1:
        fig5 = px.bar(
            df,
            x='date',
            y='unique_users',
            title='일별 활성 사용자 수',
            labels={'date': '날짜', 'unique_users': '활성 사용자'},
            text='unique_users'
        )
        fig5.update_traces(marker_color='#9467bd', texttemplate='%{text:,}', textposition='outside')
        fig5.update_layout(hovermode='x unified', showlegend=False)
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        fig6 = px.bar(
            df,
            x='date',
            y='sessions_per_user',
            title='사용자당 평균 세션 수',
            labels={'date': '날짜', 'sessions_per_user': '평균 세션 수'},
            text='sessions_per_user'
        )
        fig6.update_traces(marker_color='#8c564b', texttemplate='%{text:.2f}', textposition='outside')
        fig6.update_layout(hovermode='x unified', showlegend=False)
        st.plotly_chart(fig6, use_container_width=True)
else:
    # 여러 날짜 데이터는 line 차트
    with col1:
        fig5 = px.line(
            df,
            x='date',
            y='unique_users',
            title='일별 활성 사용자 수',
            labels={'date': '날짜', 'unique_users': '활성 사용자'}
        )
        fig5.update_traces(line_color='#9467bd', line_width=2)
        fig5.update_layout(hovermode='x unified')
        st.plotly_chart(fig5, use_container_width=True)

    with col2:
        fig6 = px.line(
            df,
            x='date',
            y='sessions_per_user',
            title='사용자당 평균 세션 수',
            labels={'date': '날짜', 'sessions_per_user': '평균 세션 수'}
        )
        fig6.update_traces(line_color='#8c564b', line_width=2)
        fig6.update_layout(hovermode='x unified')
        st.plotly_chart(fig6, use_container_width=True)

# 세션 품질 매트릭스
st.markdown("---")
st.subheader("📊 세션 품질 종합 분석")

# 세션 품질 점수 계산 (예시)
df['quality_score'] = (
    (df['avg_events_per_session'] / df['avg_events_per_session'].max()) * 40 +
    (df['avg_session_duration_sec'] / df['avg_session_duration_sec'].max()) * 30 +
    ((100 - df['bounce_rate_pct']) / 100) * 30
)

fig7 = px.bar(
    df,
    x='date',
    y='quality_score',
    title='일별 세션 품질 점수 (100점 만점)',
    labels={'date': '날짜', 'quality_score': '품질 점수'},
    color='quality_score',
    color_continuous_scale='RdYlGn'
)
fig7.update_layout(hovermode='x unified')
st.plotly_chart(fig7, use_container_width=True)

# 데이터 테이블
st.markdown("---")
st.subheader("📋 세션 데이터 상세")

display_df = df[[
    'date', 'total_sessions', 'avg_events_per_session',
    'avg_session_duration_sec', 'bounce_rate_pct', 'unique_users'
]].copy()

display_df.columns = [
    '날짜', '총 세션', '평균 이벤트/세션',
    '평균 세션 시간(초)', '이탈률(%)', '활성 사용자'
]

st.dataframe(
    display_df.style.format({
        '총 세션': '{:,.0f}',
        '평균 이벤트/세션': '{:.2f}',
        '평균 세션 시간(초)': '{:.1f}',
        '이탈률(%)': '{:.1f}',
        '활성 사용자': '{:,.0f}'
    }),
    use_container_width=True
)

# 인사이트
st.markdown("---")
st.subheader("💡 주요 인사이트")

col1, col2, col3 = st.columns(3)

with col1:
    best_day = df.loc[df['avg_events_per_session'].idxmax()]
    st.info(f"""
    **가장 활발한 날**

    {best_day['date'].strftime('%Y-%m-%d')}
    - 평균 이벤트: {best_day['avg_events_per_session']:.2f}개
    """)

with col2:
    longest_day = df.loc[df['avg_session_duration_sec'].idxmax()]
    st.success(f"""
    **가장 긴 세션 시간**

    {longest_day['date'].strftime('%Y-%m-%d')}
    - 평균 시간: {longest_day['avg_session_duration_sec']:.1f}초
    """)

with col3:
    lowest_bounce = df.loc[df['bounce_rate_pct'].idxmin()]
    st.success(f"""
    **가장 낮은 이탈률**

    {lowest_bounce['date'].strftime('%Y-%m-%d')}
    - 이탈률: {lowest_bounce['bounce_rate_pct']:.1f}%
    """)
