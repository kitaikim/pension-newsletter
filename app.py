import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
from datetime import date
from newsletter import (
    get_latest_endpoint,
    fetch_portfolio_data,
    parse_items,
    fetch_dart_nps_trades,
)

st.set_page_config(
    page_title="국민연금 포트폴리오 대시보드",
    page_icon="🏦",
    layout="wide",
)

# ── 헤더 ──────────────────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#0d1b4b,#1a237e,#283593);
            padding:32px 40px; border-radius:16px; margin-bottom:24px;">
  <div style="font-size:11px; color:#90caf9; letter-spacing:3px; margin-bottom:8px;">
    NATIONAL PENSION SERVICE
  </div>
  <h1 style="color:white; margin:0; font-size:28px; font-weight:800;">
    국민연금 포트폴리오 대시보드
  </h1>
  <p style="color:#90caf9; margin:6px 0 0; font-size:14px;">
    공공데이터포털 · DART 전자공시 실시간 연동
  </p>
</div>
""", unsafe_allow_html=True)

# ── 데이터 로드 ────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_portfolio():
    url, period_label = get_latest_endpoint()
    data = fetch_portfolio_data(url)
    items, value_col = parse_items(data)
    return items, period_label

@st.cache_data(ttl=3600)
def load_trades():
    trades, _ = fetch_dart_nps_trades(days=30, sent_rcept_nos=None)
    return trades

col_refresh, col_date = st.columns([1, 9])
with col_refresh:
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
with col_date:
    st.caption(f"마지막 조회: {date.today().strftime('%Y.%m.%d')}")

with st.spinner("데이터 불러오는 중..."):
    items, period_label = load_portfolio()
    trades = load_trades()

# ── 포트폴리오 현황 ────────────────────────────────────
st.markdown("---")
st.subheader(f"📊 자산 분류별 현황 · {period_label} 기준")

# 합계 제외
chart_items = [i for i in items if "합계" not in i["name"] and i["name"] != "합 계"]
total = sum(i["value"] for i in chart_items)
total_jo = total / 1000

# 상단 지표
m1, m2, m3 = st.columns(3)
m1.metric("총 운용자산", f"{total_jo:,.1f}조원")
m2.metric("기준 시점", period_label)
m3.metric("자산 분류", f"{len(chart_items)}개")

# 파이차트 + 테이블
left, right = st.columns([1, 1])

with left:
    df_chart = pd.DataFrame(chart_items)
    fig = px.pie(
        df_chart, values="value", names="name",
        color_discrete_sequence=px.colors.sequential.Blues_r,
        hole=0.45,
    )
    fig.update_traces(textposition="outside", textinfo="percent+label")
    fig.update_layout(
        showlegend=False,
        margin=dict(t=20, b=20, l=20, r=20),
        height=380,
    )
    st.plotly_chart(fig, use_container_width=True)

with right:
    df_table = pd.DataFrame([
        {
            "구분": i["name"],
            "금액(십억원)": f"{i['value']:,.0f}",
            "비중": f"{i['value']/total*100:.1f}%" if total > 0 else "-",
        }
        for i in chart_items
    ])
    st.dataframe(df_table, use_container_width=True, hide_index=True, height=380)

# ── DART 매수/매도 내역 ────────────────────────────────
st.markdown("---")
st.subheader("📋 최근 30일 국민연금 매수/매도 내역")
st.caption("출처: DART 주식등의대량보유상황보고서 · 주가: 공시일 종가 기준")

if not trades:
    st.info("최근 30일 내 공시 내역이 없습니다.")
else:
    # 필터
    col_filter1, col_filter2, _ = st.columns([1, 1, 4])
    with col_filter1:
        dir_filter = st.selectbox("구분", ["전체", "매수", "매도"])
    with col_filter2:
        sort_by = st.selectbox("정렬", ["날짜순", "금액순"])

    filtered = trades
    if dir_filter != "전체":
        filtered = [t for t in filtered if t["direction"] == dir_filter]
    if sort_by == "금액순":
        filtered = sorted(filtered, key=lambda x: x.get("total_amount") or 0, reverse=True)

    for t in filtered:
        is_buy = t["direction"] == "매수"
        badge_color = "#1b5e20" if is_buy else "#b71c1c"
        badge_bg = "#e8f5e9" if is_buy else "#ffebee"
        badge_text = "▲ 매수" if is_buy else "▼ 매도"

        prev = t.get("prev_ratio")
        curr = t.get("curr_ratio")
        ratio_text = f"{round(prev,2):.2f}% → {round(curr,2):.2f}%" if prev is not None and curr is not None else f"{round(curr,2):.2f}%" if curr else "-"

        qty_text = f"{abs(int(t['qty_change'])):,}주" if t.get("qty_change") else "-"
        price_text = f"{t['price']:,}원" if t.get("price") else "-"
        amount_text = f"{int(t['total_amount']/1e8):,}억원" if t.get("total_amount") else "-"

        with st.container():
            c1, c2, c3, c4, c5, c6 = st.columns([1.2, 2, 1, 1.5, 1.5, 1.5])
            c1.markdown(f"<span style='font-size:13px;color:#555;'>{t['date']}</span>", unsafe_allow_html=True)
            c2.markdown(f"**[{t['corp_name']}]({t['url']})**")
            c3.markdown(f"<span style='background:{badge_bg};color:{badge_color};font-size:12px;font-weight:700;padding:3px 10px;border-radius:12px;'>{badge_text}</span>", unsafe_allow_html=True)
            c4.markdown(f"<span style='font-size:13px;'>{ratio_text}</span>", unsafe_allow_html=True)
            c5.markdown(f"<span style='font-size:13px;'>{qty_text}</span>", unsafe_allow_html=True)
            c6.markdown(f"<span style='font-size:13px;font-weight:600;'>{amount_text}</span>", unsafe_allow_html=True)
            st.divider()

# ── 푸터 ──────────────────────────────────────────────
st.caption("데이터 출처: 국민연금공단 · 공공데이터포털 (data.go.kr) · DART 전자공시 (dart.fss.or.kr)")
