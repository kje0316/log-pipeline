import streamlit as st
import pandas as pd
import plotly.express as px
from utils import load_table_from_postgres
import altair as alt

# ------------------------------------------
# Session State 초기화 및 통합 사이드바 필터
# ------------------------------------------

# df_pref와 df_activity가 로드되었다고 가정하고 필터 옵션을 생성해야 하므로,
# 실제 데이터 로드를 먼저 수행해야 합니다.
# 여기서는 편의상 모든 데이터프레임이 로드되는 시점 이후에 필터링을 통합합니다.

# 데이터프레임 로드 (필터 옵션 생성을 위해 필요)
df1 = load_table_from_postgres(table_name="regional_search", columns_to_int=["total_searches"])
df_pref = load_table_from_postgres(table_name="room_preference", columns_to_int=["type_searches", "total_regional_searches", "preference_rate", ])
df3 = load_table_from_postgres(table_name="hourly_activity", columns_to_int=["search_hour", "hourly_activity_count"])

# 모든 데이터프레임의 local1과 local2를 결합하여 통합 필터 옵션을 만듭니다.
# 실제 운영 환경에서는 메타데이터 테이블에서 통합 옵션을 가져오는 것이 효율적입니다.
all_locals = pd.concat([df1[['local1', 'local2']], df_pref[['local1', 'local2']], df3[['local1', 'local2']]]).drop_duplicates()
all_local1_options = sorted(all_locals[all_locals['local1'] != 'D']['local1'].dropna().unique().tolist())

# Session State 초기화
if 'selected_local1' not in st.session_state:
    st.session_state.selected_local1 = all_local1_options[0] if all_local1_options else ''
if 'selected_local2' not in st.session_state:
    st.session_state.selected_local2 = ''


with st.sidebar:
    st.header("🗺️ 통합 지역 선택 필터")
    
    # 1. local1 (광역 지역) 선택
    selected_local1_sidebar = st.selectbox(
        '**1단계: 광역 지역 (local1)을 선택하세요.**',
        options=all_local1_options,
        key='local1_select_unified'
    )
    st.session_state.selected_local1 = selected_local1_sidebar # Session State 업데이트

    # local1에 종속된 local2 옵션 필터링
    # 현재 선택된 local1에 해당하는 모든 local2 옵션을 통합 데이터에서 가져옴
    local2_options = sorted(all_locals[all_locals['local1'] == st.session_state.selected_local1]['local2'].dropna().unique().tolist())
    
    # 2. local2 (기초 지역) 선택
    selected_local2_sidebar = st.selectbox(
        '**2단계: 기초 지역 (local2)을 선택하세요.**',
        options=local2_options,
        key='local2_select_unified'
    )
    st.session_state.selected_local2 = selected_local2_sidebar # Session State 업데이트
    
    st.markdown("---")
    st.success(f"현재 분석 지역: **{st.session_state.selected_local1}** > **{st.session_state.selected_local2}**")


# st.title("지역별 검색량 분석 대시보드 📊")
# st.markdown("---")


# ------------------------------------------
# 1. 총 검색량 (local1 선택만 사용)
# ------------------------------------------

st.header('🔍 지역별 총 검색량')

if not df1.empty:
    
    # 전처리
    df_temp = df1[df1['local1'] != 'D']
    df_processed = df_temp.fillna({'local1': '', 'local2': ''})
    
    # 1. local1 (광역 지역) 선택 (Session State의 값을 반영)
    selected_local1 = st.session_state.selected_local1
    st.write(f"**현재 선택된 광역 지역:** **{selected_local1}**")

    # 2. local1 필터 적용
    df_filtered = df_processed[df_processed['local1'] == selected_local1]
    
    # 3. 주요 통계 및 최저/최고값 계산 및 표시
    if not df_filtered.empty:
        total_sum = df_filtered['total_searches'].sum()
        max_searches = df_filtered['total_searches'].max()
        min_searches = df_filtered['total_searches'].min()
        
        # 최고/최저 지역 찾기
        max_local2 = df_filtered[df_filtered['total_searches'] == max_searches]['local2'].iloc[0]
        min_local2 = df_filtered[df_filtered['total_searches'] == min_searches]['local2'].iloc[0]

        st.info(f"선택된 광역 지역: **{selected_local1 if selected_local1 != '' else '데이터 없음'}**")
        
        col_total, col_max, col_min = st.columns(3)
        
        with col_total:
            st.metric(label=f"✅ {selected_local1} 총 검색량 합계", value=f"{total_sum:,} 건")
            
        with col_max:
            st.metric(
                label=f" 최고 검색 지역 (local2)",
                value=f"{max_searches:,} 건",
                delta=f"{max_local2}" 
            )

        with col_min:
            st.metric(
                label=f"⬇️ 최저 검색 지역 (local2)", 
                value=f"{min_searches:,} 건",
                delta=f"{min_local2}" 
            )
        
        # 4. local2별 검색량 시각화        
        chart = alt.Chart(df_filtered).mark_bar().encode(
            x=alt.X('local2', title='기초 지역 (local2)', sort='-y'),
            y=alt.Y('total_searches', title='총 검색량'),
            tooltip=['local2', alt.Tooltip('total_searches', format=',')],
            color=alt.Color('total_searches', scale=alt.Scale(range='ramp', scheme='viridis'), legend=None)
        ).properties(
            title=f'{selected_local1} 지역 내 기초 지역별 검색량 비교'
        ).interactive()

        st.altair_chart(chart, use_container_width=True)
        
        
        # 5. 데이터 테이블 표시
        if st.checkbox(f'**{selected_local1}** 상세 데이터 테이블 보기', key='detail_table_1'):
            st.write('\n\n\n\n\n')
            st.subheader("상세 데이터 테이블 1")
            df_display = df_filtered.rename(columns={
                'local1': '광역 지역',
                'local2': '기초 지역',
                'total_searches': '총 검색량'
            })
            st.dataframe(
                df_display[['광역 지역', '기초 지역', '총 검색량']].style.format({'총 검색량': '{:,.0f}'}), 
                use_container_width=True, 
                hide_index=True
            )

    else:
        st.warning(f"선택하신 광역 지역 ({selected_local1})에 해당하는 기초 지역 데이터가 없습니다.")

else:
    st.error("데이터 로드에 실패했습니다.")


st.markdown("---")








# ------------------------------------------
# 2. 선호하는 매물 유형 (local1, local2 모두 사용)
# ------------------------------------------

# --- 데이터 불러오기 ---
# df_pref는 이미 로드됨

# 3. 데이터 로드 후 즉시 필터링 (핵심 수정 부분)
if not df_pref.empty:
    # 'local2' 컬럼에서 'D' 지역을 제외
    df_pref_processed = df_pref[df_pref['local2'] != 'D'].copy()
    
    # 'room_type' 컬럼에서 'NULL' 값을 제외
    df_pref_processed = df_pref_processed[df_pref_processed['room_type'] != 'NULL'].copy()
    
    # 'local1'이 'D'인 행을 제외 (if necessary)
    df_pref_processed = df_pref_processed[df_pref_processed['local1'] != 'D'].copy()
    
    df_pref_processed.reset_index(drop=True, inplace=True)


# 1. 데이터 로드 확인
if not df_pref_processed.empty:
    
    # Session State에서 선택된 지역 값 가져오기
    selected_local1 = st.session_state.selected_local1
    selected_local2 = st.session_state.selected_local2

    st.header(f"매물 유형별 선호도")
    st.write(f"**현재 분석 지역:** **{selected_local1}** > **{selected_local2}**")

    # 3. 데이터 필터링 (선택된 local1과 local2 모두 적용)
    df_filtered = df_pref_processed[
        (df_pref_processed['local1'] == selected_local1) & 
        (df_pref_processed['local2'] == selected_local2)
    ].copy()
    
    
    # 4. 시각화 및 분석
    if not df_filtered.empty:
        col1, col2 = st.columns([0.4, 0.6])
        
        # 4.1. 파이 차트 (선호도 비율 시각화)
        with col1:
            fig_pie = px.pie(
                df_filtered,
                names='room_type',
                values='preference_rate',
                title=f'{selected_local2} 총 검색 중 방 유형 비율',
                hole=0.4 
            )
            fig_pie.update_traces(
                textposition='inside', 
                textinfo='percent+label',
                hovertemplate="<b>%{label}</b><br>선호도 비율: %{value:.1f}%<extra></extra>"
            )
            st.plotly_chart(fig_pie, use_container_width=True)

        # 4.2. 막대 그래프 (총 검색 수 시각화)
        with col2:
            fig_bar = px.bar(
                df_filtered.sort_values('type_searches', ascending=False),
                x='room_type',
                y='type_searches',
                text=df_filtered['type_searches'].apply(lambda x: f'{x:,.0f}'),
                color='preference_rate', # 선호도 비율로 색상 구분
                color_continuous_scale=px.colors.sequential.Agsunset,
                title='유형별 실제 검색량 비교'
            )
            fig_bar.update_traces(textposition='outside')
            fig_bar.update_layout(xaxis_title="방 유형", yaxis_title="검색 수")
            st.plotly_chart(fig_bar, use_container_width=True)

        # 4.3. 데이터프레임 표시
        if st.checkbox(f'**{selected_local2}** 상세 데이터 테이블 보기', key='detail_table_2'):            
            st.write('\n\n\n\n\n')
            st.write('\n\n\n\n\n')
            st.subheader(" 상세 데이터 테이블 2")            
            df_display = df_filtered.rename(columns={
                'local2': '기초 지역',
                'room_type': '방 유형',
                'type_searches': '유형별 검색 수',
                'total_regional_searches': '지역 총 검색 수',
                'preference_rate': '선호도 비율 (%)'
            })
            columns_to_show = ["방 유형", "유형별 검색 수", "지역 총 검색 수", "선호도 비율 (%)"]

            st.dataframe(
                df_display[columns_to_show].style.format({
                    "유형별 검색 수": "{:,.0f}", 
                    "지역 총 검색 수": "{:,.0f}", 
                    "선호도 비율 (%)": "{:.1f}"
                }), 
                use_container_width=True, 
                hide_index=True
            )

    else:
        st.warning(f"선택하신 지역 ({selected_local1} {selected_local2})에 대한 데이터가 없습니다.")

else:
    st.error("데이터 로드에 실패했거나, 유효한 데이터가 존재하지 않습니다.")

st.markdown('---')









# ------------------------------------------
# 3. 시간대별 활동량 (local1, local2 모두 사용)
# ------------------------------------------

df_activity = df3.copy()
df_activity = df_activity.sort_values('search_hour').reset_index(drop=True)
if not df_activity.empty:
    
    # Session State에서 선택된 지역 값 가져오기
    selected_local1 = st.session_state.selected_local1
    selected_local2 = st.session_state.selected_local2

    
    # 3. 데이터 필터링 (local1과 local2 모두 필터링)
    df_filtered = df_activity[
        (df_activity['local1'] == selected_local1) &
        (df_activity['local2'] == selected_local2)
    ].copy()
    
    st.header(f"📈 지역별 사용자 24시간 활동 추이")
    st.write(f"**현재 분석 지역:** **{selected_local1}** > **{selected_local2}**")

    
    if not df_filtered.empty:
        # 4. 핵심 지표 (KPI) 표시
        col1, col2 = st.columns(2)
        max_hour_data = df_filtered.loc[df_filtered['hourly_activity_count'].idxmax()]
        
        with col1:
            st.metric(
                label="총 활동량 (24시간)",
                value=f"{df_filtered['hourly_activity_count'].sum():,}"
            )
            
        with col2:
            st.metric(
                label="최대 활동 시간",
                value=f"{max_hour_data['search_hour']}시",
                delta=f"활동 수: {max_hour_data['hourly_activity_count']:,}"
            )
        
        # 5. 시간대별 활동 추이 그래프 시각화 (Line Chart)
        fig = px.line(
            df_filtered,
            x='search_hour',
            y='hourly_activity_count',
            markers=True,
            title=f'{selected_local2} 시간대별 사용자 활동 변화'
        )

        # X축을 0부터 23까지 1시간 간격으로 표시
        fig.update_layout(
            xaxis_title="시간 (Hour)", 
            yaxis_title="총 활동 수",
            xaxis={'tickmode': 'linear', 'dtick': 1} 
        )
        fig.update_traces(mode='lines+markers')

        st.plotly_chart(fig, use_container_width=True)
        
        
        # 6. 상세 데이터 테이블
        if st.checkbox(f'**{selected_local2}** 상세 데이터 테이블 보기', key='detail_table_3'):
            df_display = df_filtered.rename(columns={
                'search_hour': '시간',
                'hourly_activity_count': '총 활동 수'
            })
            st.dataframe(df_display[['시간', '총 활동 수']], use_container_width=True, hide_index=True)

    else:
        st.warning(f"선택하신 지역 ({selected_local1} {selected_local2})에 대한 활동 데이터가 없습니다.")

else:
    st.error("데이터를 로드하지 못했습니다.")