import streamlit as st
import plotly.express as px
import pandas as pd
from datetime import date
from newsletter import (
    get_latest_endpoint,
    fetch_portfolio_data,
    parse_items,
    fetch_dart_nps_trades,
    load_trades_cache,
)

st.set_page_config(
    page_title="국민연금 포트폴리오 대시보드",
    page_icon="🏦",
    layout="wide",
)

# ── 반응형 CSS ──────────────────────────────────────────
st.markdown("""
<style>
@media (max-width: 768px) {
  /* 파이차트·테이블 컬럼 세로로 쌓기 */
  [data-testid="column"] { min-width: 100% !important; }
  /* 메트릭 폰트 조정 */
  [data-testid="metric-container"] { padding: 8px 4px !important; }
  [data-testid="stMetricValue"] { font-size: 20px !important; }
}
</style>
""", unsafe_allow_html=True)

# ── 헤더 ──────────────────────────────────────────────
st.markdown("""
<div style="background:linear-gradient(135deg,#0d1b4b,#1a237e,#283593);
            padding:28px 24px; border-radius:16px; margin-bottom:20px;">
  <div style="font-size:11px; color:#90caf9; letter-spacing:3px; margin-bottom:6px;">
    NATIONAL PENSION SERVICE
  </div>
  <h1 style="color:white; margin:0; font-size:24px; font-weight:800;">
    국민연금 포트폴리오 대시보드
  </h1>
  <p style="color:#90caf9; margin:6px 0 0; font-size:13px;">
    공공데이터포털 · DART 전자공시 실시간 연동
  </p>
</div>
""", unsafe_allow_html=True)

# ── 새로고침 ────────────────────────────────────────────
col_refresh, col_date = st.columns([1, 9])
with col_refresh:
    if st.button("🔄 새로고침"):
        st.cache_data.clear()
        st.rerun()
with col_date:
    st.caption(f"마지막 조회: {date.today().strftime('%Y.%m.%d')}")


# ── 데이터 로드 ────────────────────────────────────────
@st.cache_data(ttl=3600)
def load_portfolio():
    url, period_label = get_latest_endpoint()
    data = fetch_portfolio_data(url)
    items, value_col = parse_items(data)
    return items, period_label


@st.cache_data(ttl=3600)
def load_trades():
    # GitHub Actions가 저장한 캐시 파일 우선 사용 (빠름)
    trades, updated = load_trades_cache()
    if trades:
        return trades, updated
    # 캐시 없으면 API 직접 호출 (30일만)
    trades, _ = fetch_dart_nps_trades(days=30, sent_rcept_nos=None, fetch_prices=True)
    return trades, None


# ── 포트폴리오 현황 ────────────────────────────────────
st.markdown("---")

with st.spinner("포트폴리오 현황 불러오는 중..."):
    items, period_label = load_portfolio()

st.subheader(f"📊 자산 분류별 현황 · {period_label} 기준")

chart_items = [i for i in items if "합계" not in i["name"] and i["name"] != "합 계"]
total = sum(i["value"] for i in chart_items)
total_jo = total / 1000

m1, m2, m3 = st.columns(3)
m1.metric("총 운용자산", f"{total_jo:,.1f}조원")
m2.metric("기준 시점", period_label)
m3.metric("자산 분류", f"{len(chart_items)}개")

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
        height=360,
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
    st.dataframe(df_table, use_container_width=True, hide_index=True, height=360)


# ── DART 매수/매도 내역 ────────────────────────────────
st.markdown("---")
st.subheader("📋 최근 90일 국민연금 매수/매도 내역")
st.caption("출처: DART 주식등의대량보유상황보고서")

with st.spinner("공시 내역 불러오는 중..."):
    trades, cache_updated = load_trades()

if cache_updated:
    st.caption(f"캐시 기준: {cache_updated}")

if not trades:
    st.info("최근 90일 내 공시 내역이 없습니다.")
else:
    tab_cards, tab_foreign = st.tabs(["📋 공시 카드", "🌏 외국인 일별 매매"])

    # ── Tab 1: 공시 카드 ────────────────────────────────
    with tab_cards:
        col_filter1, col_filter2, _ = st.columns([1, 1, 4])
        with col_filter1:
            dir_filter = st.selectbox("구분", ["전체", "매수", "매도"])
        with col_filter2:
            sort_by = st.selectbox("정렬", ["날짜순", "비율변동순"])

        filtered = trades
        if dir_filter != "전체":
            filtered = [t for t in filtered if t["direction"] == dir_filter]
        if sort_by == "비율변동순":
            def _ratio_delta(t):
                p, c = t.get("prev_ratio"), t.get("curr_ratio")
                if p is not None and c is not None:
                    return abs(c - p)
                return 0
            filtered = sorted(filtered, key=_ratio_delta, reverse=True)

        cards_html = ""
        for t in filtered:
            is_buy = t["direction"] == "매수"
            badge_color = "#1b5e20" if is_buy else "#b71c1c"
            badge_bg = "#e8f5e9" if is_buy else "#ffebee"
            border_color = "#a5d6a7" if is_buy else "#ef9a9a"
            badge_text = "▲ 매수" if is_buy else "▼ 매도"

            prev = t.get("prev_ratio")
            curr = t.get("curr_ratio")
            if prev is not None and curr is not None:
                ratio_text = f"{round(prev,2):.2f}% → <b>{round(curr,2):.2f}%</b>"
            elif curr is not None:
                ratio_text = f"<b>{round(curr,2):.2f}%</b>"
            else:
                ratio_text = "-"

            qty_text = f"{abs(int(t['qty_change'])):,}주" if t.get("qty_change") else "-"
            amount_text = f"{int(t['total_amount']/1e8):,}억원" if t.get("total_amount") else "-"
            price_text = f"{t['price']:,}원" if t.get("price") else "-"
            dart_url = t.get("url", "#")

            fn = t.get("foreign_net")
            if fn is not None:
                fn_abs = abs(int(fn)) // 100  # 백만원 → 억원
                fn_sign = "▲ 순매수" if fn > 0 else "▼ 순매도"
                fn_color = "#1b5e20" if fn > 0 else "#b71c1c"
                fn_row = f"<span style='grid-column:1/-1; color:#888;'>🌏 외국인 5일합 <b style='color:{fn_color};'>{fn_sign} {fn_abs:,}억원</b></span>"
            else:
                fn_row = ""


            cards_html += f"""
<div style="border:1px solid {border_color}; border-left:4px solid {badge_color};
            border-radius:10px; padding:14px 16px; margin-bottom:10px; background:#fff;">
  <div style="display:flex; justify-content:space-between; align-items:center; margin-bottom:8px;">
    <a href="{dart_url}" target="_blank"
       style="font-size:16px; font-weight:700; color:#1a237e; text-decoration:none;">{t['corp_name']}</a>
    <span style="background:{badge_bg}; color:{badge_color}; font-size:12px; font-weight:700;
                 padding:3px 12px; border-radius:12px; white-space:nowrap;">{badge_text}</span>
  </div>
  <div style="display:grid; grid-template-columns:1fr 1fr; gap:6px 12px; font-size:13px; color:#555; margin-top:4px;">
    <span>📅 {t['date']}</span>
    <span>📊 {ratio_text}</span>
    <span>📦 {qty_text}</span>
    <span style="font-weight:600; color:#1a237e;">💰 {amount_text}</span>
    <span style="grid-column:1/-1; color:#888;">🏷️ 주당가격 <b style="color:#333;">{price_text}</b></span>
    {fn_row}
  </div>
</div>"""

        st.markdown(cards_html, unsafe_allow_html=True)

    # ── Tab 2: 외국인 일별 매매 매트릭스 ───────────────
    with tab_foreign:
        st.caption("출처: KIS Open API · 최근 5거래일 일별 외국인 순매수 (단위: 억원)")
        rows_data = [t for t in trades if t.get("foreign_daily")]
        if not rows_data:
            st.info("외국인 일별 데이터가 아직 수집되지 않았습니다. 다음 cron 실행 후 표시됩니다.")
        else:
            # 모든 거래일 수집 → 정렬
            all_dates = sorted({d["date"] for t in rows_data for d in t["foreign_daily"]})

            rows = []
            for t in rows_data:
                daily_map = {d["date"]: d["net"] for d in t["foreign_daily"]}
                row = {"종목": t["corp_name"], "구분": t["direction"]}
                for date in all_dates:
                    net = daily_map.get(date)
                    row[f"{date[:2]}.{date[2:]}"] = (net // 100) if net is not None else None
                row["합계(억)"] = (t.get("foreign_net") or 0) // 100
                rows.append(row)

            df = pd.DataFrame(rows)
            # 절대값 큰 순 정렬
            df = df.iloc[df["합계(억)"].abs().argsort()[::-1]].reset_index(drop=True)

            # 색상 스타일링
            num_cols = [c for c in df.columns if c not in ("종목", "구분")]

            def _color(v):
                if pd.isna(v):
                    return "color: #ccc;"
                if v > 0:
                    return "color: #1b5e20; font-weight: 700;"
                if v < 0:
                    return "color: #b71c1c; font-weight: 700;"
                return "color: #888;"

            def _fmt(v):
                if pd.isna(v):
                    return "-"
                v = int(v)
                if v > 0:
                    return f"▲{v:,}"
                if v < 0:
                    return f"▼{abs(v):,}"
                return "0"

            styled = (
                df.style
                .map(_color, subset=num_cols)
                .format({c: _fmt for c in num_cols})
            )
            st.dataframe(styled, use_container_width=True, hide_index=True, height=600)

# ── 푸터 ──────────────────────────────────────────────
st.markdown("---")
st.caption("데이터 출처: 국민연금공단 · 공공데이터포털 (data.go.kr) · DART 전자공시 (dart.fss.or.kr)")
