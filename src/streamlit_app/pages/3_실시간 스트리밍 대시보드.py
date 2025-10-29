import streamlit as st
import pandas as pd
import plotly.express as px
import plotly.graph_objects as go
from utils import load_table_from_postgres
from datetime import datetime

st.set_page_config(page_title="실시간 스트리밍 모니터링", layout="wide")

st.title("🚀 실시간 스트리밍 파이프라인 모니터링")
st.markdown("5분 단위 집계 데이터를 실시간으로 확인할 수 있습니다.")
st.markdown("---")

# 데이터 로드
df_streaming = load_table_from_postgres(
    table_name="streaming_stats",
    columns_to_int=["total_events", "active_users"]
)

if not df_streaming.empty:
    # 시간 컬럼 변환
    df_streaming['window_start'] = pd.to_datetime(df_streaming['window_start'])
    df_streaming['window_end'] = pd.to_datetime(df_streaming['window_end'])

    # 최신 순으로 정렬
    df_streaming = df_streaming.sort_values('window_start', ascending=False).reset_index(drop=True)

    # ========================================
    # 1. 핵심 지표 (KPI)
    # ========================================
    st.header("📊 실시간 핵심 지표")

    col1, col2, col3, col4 = st.columns(4)

    # 최신 윈도우 데이터
    latest_window = df_streaming.iloc[0]
    total_windows = len(df_streaming)
    total_events_all = df_streaming['total_events'].sum()
    avg_events_per_window = df_streaming['total_events'].mean()

    with col1:
        st.metric(
            label="최신 윈도우 이벤트 수",
            value=f"{latest_window['total_events']:,}",
            delta=f"{latest_window['window_start'].strftime('%H:%M')} ~ {latest_window['window_end'].strftime('%H:%M')}"
        )

    with col2:
        st.metric(
            label="최신 윈도우 활성 사용자",
            value=f"{latest_window['active_users']:,}",
            delta="5분간 활동"
        )

    with col3:
        st.metric(
            label="총 윈도우 수",
            value=f"{total_windows}개",
            delta="5분 단위"
        )

    with col4:
        st.metric(
            label="윈도우당 평균 이벤트",
            value=f"{avg_events_per_window:,.0f}",
            delta=f"총 {total_events_all:,}건"
        )

    st.markdown("---")

    # ========================================
    # 2. 시간대별 이벤트 추이 (타임시리즈 그래프)
    # ========================================
    st.header("📈 5분 단위 이벤트 발생 추이")

    # 최근 데이터만 표시 (최대 50개 윈도우)
    display_limit = min(50, len(df_streaming))
    df_display = df_streaming.head(display_limit).sort_values('window_start')

    # 라인 차트: 이벤트 수 추이
    fig_events = px.line(
        df_display,
        x='window_start',
        y='total_events',
        markers=True,
        title=f'최근 {display_limit}개 윈도우 이벤트 발생량',
        labels={'window_start': '시간 (5분 단위)', 'total_events': '이벤트 수'}
    )
    fig_events.update_traces(line_color='#FF6B6B', marker=dict(size=8))
    fig_events.update_layout(hovermode='x unified')

    st.plotly_chart(fig_events, use_container_width=True)

    # ========================================
    # 3. 활성 사용자 추이
    # ========================================
    st.header("👥 5분 단위 활성 사용자 추이")

    # 라인 차트: 활성 사용자 수 추이
    fig_users = px.line(
        df_display,
        x='window_start',
        y='active_users',
        markers=True,
        title=f'최근 {display_limit}개 윈도우 활성 사용자 수',
        labels={'window_start': '시간 (5분 단위)', 'active_users': '활성 사용자 수'}
    )
    fig_users.update_traces(line_color='#4ECDC4', marker=dict(size=8))
    fig_users.update_layout(hovermode='x unified')

    st.plotly_chart(fig_users, use_container_width=True)

    st.markdown("---")

    # ========================================
    # 4. 이벤트 vs 사용자 비교 (복합 차트)
    # ========================================
    st.header("📊 이벤트 수 vs 활성 사용자 비교")

    # 이중 축 차트
    fig_comparison = go.Figure()

    fig_comparison.add_trace(go.Scatter(
        x=df_display['window_start'],
        y=df_display['total_events'],
        name='이벤트 수',
        mode='lines+markers',
        line=dict(color='#FF6B6B', width=2),
        yaxis='y'
    ))

    fig_comparison.add_trace(go.Scatter(
        x=df_display['window_start'],
        y=df_display['active_users'],
        name='활성 사용자',
        mode='lines+markers',
        line=dict(color='#4ECDC4', width=2),
        yaxis='y2'
    ))

    fig_comparison.update_layout(
        title='이벤트 수와 활성 사용자 추이 비교',
        xaxis=dict(title='시간 (5분 단위)'),
        yaxis=dict(title='이벤트 수', side='left', showgrid=False),
        yaxis2=dict(title='활성 사용자 수', side='right', overlaying='y', showgrid=False),
        hovermode='x unified',
        legend=dict(x=0.01, y=0.99, bordercolor="Black", borderwidth=1)
    )

    st.plotly_chart(fig_comparison, use_container_width=True)

    st.markdown("---")

    # ========================================
    # 5. 사용자당 평균 이벤트 수 분석
    # ========================================
    st.header("🎯 사용자당 평균 이벤트 수")

    df_display['events_per_user'] = df_display['total_events'] / df_display['active_users']

    fig_avg = px.bar(
        df_display,
        x='window_start',
        y='events_per_user',
        title='윈도우별 사용자당 평균 이벤트 수',
        labels={'window_start': '시간 (5분 단위)', 'events_per_user': '이벤트/사용자'},
        color='events_per_user',
        color_continuous_scale='Viridis'
    )
    fig_avg.update_layout(showlegend=False)

    st.plotly_chart(fig_avg, use_container_width=True)

    st.markdown("---")

    # ========================================
    # 6. 상세 데이터 테이블
    # ========================================
    if st.checkbox('📋 상세 데이터 테이블 보기', key='streaming_detail_table'):
        st.subheader("스트리밍 통계 상세 데이터")

        df_table = df_streaming.copy()
        df_table['window_start'] = df_table['window_start'].dt.strftime('%Y-%m-%d %H:%M:%S')
        df_table['window_end'] = df_table['window_end'].dt.strftime('%Y-%m-%d %H:%M:%S')

        st.dataframe(
            df_table.rename(columns={
                'window_start': '윈도우 시작',
                'window_end': '윈도우 종료',
                'total_events': '총 이벤트',
                'active_users': '활성 사용자'
            })[['윈도우 시작', '윈도우 종료', '총 이벤트', '활성 사용자']],
            use_container_width=True,
            hide_index=True
        )

    # ========================================
    # 7. 데이터 통계 요약
    # ========================================
    with st.expander("📊 통계 요약 보기"):
        col1, col2 = st.columns(2)

        with col1:
            st.markdown("### 이벤트 통계")
            st.write(f"- 최대: {df_streaming['total_events'].max():,}건")
            st.write(f"- 최소: {df_streaming['total_events'].min():,}건")
            st.write(f"- 평균: {df_streaming['total_events'].mean():,.2f}건")
            st.write(f"- 표준편차: {df_streaming['total_events'].std():,.2f}건")

        with col2:
            st.markdown("### 활성 사용자 통계")
            st.write(f"- 최대: {df_streaming['active_users'].max():,}명")
            st.write(f"- 최소: {df_streaming['active_users'].min():,}명")
            st.write(f"- 평균: {df_streaming['active_users'].mean():,.2f}명")
            st.write(f"- 표준편차: {df_streaming['active_users'].std():,.2f}명")

else:
    st.error("스트리밍 데이터를 로드하지 못했습니다. 스트리밍 파이프라인이 실행 중인지 확인해주세요.")
    st.info("스트리밍 파이프라인을 실행하려면: `.venv/bin/python src/streaming/streaming_job.py`")
