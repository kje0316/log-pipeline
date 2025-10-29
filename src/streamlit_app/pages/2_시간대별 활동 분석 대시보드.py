import streamlit as st
import pandas as pd
import plotly.express as px
from utils import load_table_from_postgres


st.set_page_config(page_title="시간대별 검색량 분석", layout="wide")
# st.title("시간대별 검색량 대시보드 📊")
# st.markdown("---")


import altair as alt



# ------------------------------------------
# 1. 시간대별 사용자 활동
# ------------------------------------------
df1 = load_table_from_postgres(table_name="daily_activity_peak", columns_to_int=["hourly_activity_count", "unique_users", "unique_local1", "unique_local2"])
# --- Streamlit 분석 코드 시작 ---
# st.title("⏰ 시간대별 사용자 활동 분석")
# st.markdown("---")

df_activity = df1.copy()
if not df_activity.empty:
    st.header("⏰ 시간대별 사용자 활동 요약")
    
    # 2. 핵심 지표 (KPI) 표시
    col1, col2 = st.columns(2)
    
    max_hour = df_activity.loc[df_activity['hourly_activity_count'].idxmax()]
    
    with col1:
        st.metric(
            label="총 활동량 (24시간 합계)",
            value=f"{df_activity['hourly_activity_count'].sum():,}"
        )
        
    with col2:
        st.metric(
            label="최대 활동 시간",
            value=f"{max_hour['search_hour']}시",
            delta=f"활동 수: {max_hour['hourly_activity_count']:,}"
        )

    # st.markdown("---")

    # 3. 시간대별 활동 막대 그래프 시각화
    # st.subheader("시간대별 활동량 (Hourly Activity Count)")
    
    # Plotly Bar Chart 생성
    fig = px.bar(
        df_activity,
        x='search_hour',
        y='hourly_activity_count',
        text='hourly_activity_count', # 막대 위에 값 표시
        color='hourly_activity_count',
        color_continuous_scale=px.colors.sequential.Plasma, # 색상 스케일 적용
        title="24시간 사용자 활동 추이"
    )

    # 차트 레이아웃 설정
    fig.update_traces(texttemplate='%{text}', textposition='outside')
    fig.update_layout(
        xaxis_title="시간 (Hour)", 
        yaxis_title="총 활동 수",
        xaxis={'tickmode': 'array', 'tickvals': df_activity['search_hour'].tolist()} # X축 0~23시 모두 표시
    )

    st.plotly_chart(fig, use_container_width=True)
    
    if st.checkbox('상세 테이블 보기', key='detail_table_4'):

        # 4. 데이터프레임 표시 (선택 사항)
        # st.subheader("Raw Data Table")
        st.dataframe(df_activity.rename(columns={
            'search_hour': '시간',
            'hourly_activity_count': '총 활동 수'
        }), use_container_width=True, hide_index=True)

else:
    st.warning("데이터를 로드하지 못했거나 데이터프레임이 비어 있습니다.")


st.markdown("---")






# ------------------------------------------
# 2. 시간대별 거래 유형 선호도 변화
# ------------------------------------------

# st.header('2. 광역 지역별 선호하는 매물 유형')
# # --- 데이터 불러오기 ---
# df2 = load_table_from_postgres(table_name="hourly_deal", columns_to_int=["searches_by_deal_type", "unique_users"])
# # st.dataframe(df2)

# df_shift = df2.sort_values(['search_hour', 'deal_type']).reset_index(drop=True)

# # 4. 거래 유형 이름 변경 (시각화 친화적으로)
# df_shift['deal_type_kr'] = df_shift['deal_type'].replace({
#     'rent_monthly': '월세',
#     'rent': '전세', # 'rent'를 전세(보증금)로 해석
#     'rent_deposit_only': '반전세/특정 전세', # 보증금만 있는 경우
#     'sales': '매매'
# })

# # --- Streamlit 분석 코드 시작 ---
# st.title("⏱️ 시간대별 거래 유형 선호도 변화 분석")
# st.markdown("---")

# # 1. 데이터 로드
# if not df_shift.empty:
#     st.sidebar.header("⚙️ 분석 필터")

#     # 2. 사이드바 필터: 거래 유형 선택 (비교 대상)
#     all_deal_types = df_shift['deal_type_kr'].unique()
#     selected_deals = st.sidebar.multiselect(
#         "비교할 거래 유형 선택",
#         options=all_deal_types,
#         default=all_deal_types # 기본값으로 모두 선택
#     )
    
#     # 3. 데이터 필터링
#     df_filtered = df_shift[
#         df_shift['deal_type_kr'].isin(selected_deals)
#     ].copy()
    
#     st.header("📈 24시간 동안의 거래 유형별 활동량 추이")
    
#     if not df_filtered.empty:
#         # 4. 라인 차트 시각화
#         st.subheader("시간대별 거래 유형 검색량 비교")
        
#         # Plotly Line Chart 생성
#         fig = px.line(
#             df_filtered,
#             x='search_hour',
#             y='searches_by_deal_type',
#             color='deal_type_kr', # 거래 유형별로 선을 분리
#             markers=True,
#             title='시간대별 사용자 거래 유형 검색 활동 변화'
#         )

#         # 차트 레이아웃 설정
#         fig.update_layout(
#             xaxis_title="시간 (Hour)", 
#             yaxis_title="총 검색 수",
#             # X축을 0부터 23까지 표시
#             xaxis={'tickmode': 'linear', 'dtick': 1, 'range': [0, 23]}, 
#             legend_title="거래 유형"
#         )
#         fig.update_traces(mode='lines+markers')

#         st.plotly_chart(fig, use_container_width=True)
        
#         st.markdown("---")
        
#         # 5. 상세 데이터 테이블
#         st.subheader("상세 데이터")
#         df_display = df_filtered.rename(columns={
#             'search_hour': '시간',
#             'deal_type_kr': '거래 유형',
#             'searches_by_deal_type': '총 검색 수'
#         })
#         st.dataframe(df_display[['시간', '거래 유형', '총 검색 수']], use_container_width=True, hide_index=True)

# else:
#     st.warning("데이터를 로드하지 못했거나 필터 조건에 맞는 데이터가 없습니다.")






# st.markdown('---')
# if st.checkbox('상세 데이터 테이블 보기', key='detail_table_2'):
#     st.subheader('원본 상세 데이터 2')
#     st.dataframe(df2)



# st.markdown("---")



# ------------------------------------------
# 3. 시간대별 사용자 몰입도 변화 
# ------------------------------------------

# --- 데이터 불러오기 ---
df3 = load_table_from_postgres(table_name="hourly_engagement", columns_to_int=["search_hour", "total_sessions", "avg_events_per_session"])
# st.dataframe(df3)

df_engagement = df3.sort_values('search_hour').reset_index(drop=True)


# --- Streamlit 분석 코드 시작 ---
st.header("📉 시간대별 사용자 몰입도 변화")
# st.markdown("---")
st.markdown("세션당 평균 이벤트 수는 앱이나 웹사이트에서 사용자가 얼마나 깊이 있게 콘텐츠를 탐색하고 상호작용했는지를 나타내는 핵심 지표입니다. ")
st.text("\n\n\n\n\n")

if not df_engagement.empty:
    # st.header("세션당 평균 이벤트 수 변화 추이")

    # 2. 핵심 지표 (KPI) 표시
    col1, col2, col3 = st.columns(3)
    
    # 몰입도 지표: 세션당 평균 이벤트 수
    max_engagement = df_engagement.loc[df_engagement['avg_events_per_session'].idxmax()]
    min_engagement = df_engagement.loc[df_engagement['avg_events_per_session'].idxmin()]

    with col1:
        st.metric(
            label="총 세션 수",
            value=f"{df_engagement['total_sessions'].sum():,}"
        )
        
    with col2:
        st.metric(
            label="최고 몰입 시간 (Highest Engagement)",
            value=f"{max_engagement['search_hour']}시",
            delta=f"평균 이벤트: {max_engagement['avg_events_per_session']:.2f}"
        )

    with col3:
        st.metric(
            label="최저 몰입 시간 (Lowest Engagement)",
            value=f"{min_engagement['search_hour']}시",
            delta=f"평균 이벤트: {min_engagement['avg_events_per_session']:.2f}",
            delta_color="inverse" # 낮을수록 부정적임을 표시
        )
        
    # st.markdown("---")

    # 3. 몰입도 변화 추이 그래프 시각화 (라인 차트)
    # st.subheader("세션당 평균 이벤트 수 (몰입도) 변화")
    
    # Plotly Line Chart: 몰입도 지표
    fig_engagement = px.line(
        df_engagement,
        x='search_hour',
        y='avg_events_per_session',
        markers=True,
        title='• 세션당 평균 이벤트 수 (몰입도) 추이'
    )
    fig_engagement.update_layout(
        xaxis_title="시간 (Hour)", 
        yaxis_title="평균 이벤트 수 / 세션",
        xaxis={'tickmode': 'linear', 'dtick': 1} # X축을 0부터 23까지 1시간 간격으로 표시
    )
    st.plotly_chart(fig_engagement, use_container_width=True)
    
    st.markdown("---")

    # 4. 세션 수와 몰입도를 함께 표시 (이중 축 또는 Bubble/Scatter)
    # st.subheader("세션 수와 몰입도의 관계")

    fig_bubble = px.scatter(
        df_engagement,
        x='search_hour',
        y='avg_events_per_session',
        size='total_sessions', # 세션 수에 따라 버블 크기 변경
        color='avg_events_per_session',
        hover_data=['total_sessions'],
        color_continuous_scale=px.colors.sequential.Plasma,
        title='• 시간대별 세션 크기와 몰입도 비교'
    )
    fig_bubble.update_layout(
        xaxis_title="시간 (Hour)", 
        yaxis_title="평균 이벤트 수 / 세션 (몰입도)",
        xaxis={'tickmode': 'linear', 'dtick': 1}
    )
    st.plotly_chart(fig_bubble, use_container_width=True)


    # 5. 상세 데이터 테이블
    if st.checkbox('상세 테이블 보기', key='detail_table_3'):
        st.subheader("상세 데이터")
        df_display = df_engagement.rename(columns={
            'search_hour': '시간',
            'total_sessions': '총 세션 수',
            'avg_events_per_session': '세션당 평균 이벤트 수'
        })
        st.dataframe(df_display, use_container_width=True, hide_index=True)

else:
    st.warning("데이터를 로드하지 못했거나 데이터프레임이 비어 있습니다.")









