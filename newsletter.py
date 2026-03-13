#!/usr/bin/env python3
"""국민연금 포트폴리오 뉴스레터 자동 발송 스크립트"""

import os
import sys
import json
import requests
from datetime import date, timedelta
from dotenv import load_dotenv
from pykrx import stock as krx_stock

load_dotenv()

def _get_secret(key):
    """로컬은 .env, Streamlit Cloud는 st.secrets에서 읽기"""
    try:
        import streamlit as st
        return st.secrets.get(key) or os.getenv(key)
    except Exception:
        return os.getenv(key)

API_KEY = _get_secret("NPS_API_KEY")
SENDGRID_API_KEY = _get_secret("SENDGRID_API_KEY")
SENDER_EMAIL = _get_secret("SENDER_EMAIL")
RECIPIENT_EMAIL = _get_secret("RECIPIENT_EMAIL")

API_BASE = "https://api.odcloud.kr/api"
DART_API_BASE = "https://opendart.fss.or.kr/api"
STATE_FILE = os.path.join(os.path.dirname(__file__), "last_sent.json")


def load_sent_rcept_nos():
    """이미 발송된 rcept_no 집합 로드"""
    if not os.path.exists(STATE_FILE):
        return set()
    with open(STATE_FILE) as f:
        data = json.load(f)
    return set(data.get("rcept_nos", []))


def save_sent_rcept_nos(rcept_nos: set):
    """발송된 rcept_no 집합 저장 (당해 연도만 유지)"""
    current_year = str(date.today().year)
    filtered = [r for r in rcept_nos if r.startswith(current_year)]
    with open(STATE_FILE, "w") as f:
        json.dump({"rcept_nos": filtered, "updated": date.today().isoformat()}, f, indent=2)

# 매월 갱신되는 마스터 데이터셋 UDDI (현황(말잔_십억원) 컬럼이 항상 최신값)
MASTER_UDDI = "21294cf5-01db-4d34-bcbe-92460145f5d0"


def get_latest_endpoint():
    """마스터 데이터셋에서 최신 기간 레이블 파싱"""
    url = f"{API_BASE}/15106894/v1/uddi:{MASTER_UDDI}"
    resp = requests.get(url, params={"serviceKey": API_KEY, "perPage": 1, "returnType": "JSON"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()
    items = data.get("data", [])
    if not items:
        raise RuntimeError("포트폴리오 데이터를 불러오지 못했습니다.")

    # 컬럼명에서 "YYYY년 MM월" 패턴 추출 (예: "2025년 11월(십억 원)")
    import re
    period_label = None
    for col in items[0].keys():
        m = re.search(r"(\d{4}년\s*\d{1,2}월)", col)
        if m:
            period_label = m.group(1).replace(" ", "")
            break
    if not period_label:
        period_label = "최신"

    print(f"  최신 데이터: {period_label}")
    return url, period_label


def fetch_portfolio_data(url):
    resp = requests.get(url, params={"serviceKey": API_KEY, "page": 1, "perPage": 100, "returnType": "JSON"}, timeout=15)
    resp.raise_for_status()
    return resp.json()


def parse_items(data):
    items = data.get("data", [])
    if not items:
        return [], None
    # 현황(말잔_십억원) 컬럼 우선 사용, 없으면 첫 번째 비-구분 컬럼
    value_col = "현황(말잔_십억원)" if "현황(말잔_십억원)" in items[0] else next(
        (k for k in items[0].keys() if k != "구분"), None
    )
    result = []
    for item in items:
        name = item.get("구분", "").strip()
        raw = item.get(value_col, 0)
        try:
            value = float(str(raw).replace(",", ""))
        except (ValueError, TypeError):
            value = 0.0
        if name:
            result.append({"name": name, "value": value})
    return result, value_col


def fetch_dart_nps_trades(days=30, sent_rcept_nos=None, fetch_prices=True):
    """최근 N일간 국민연금의 지분공시 전체 스캔 → 미발송 매수/매도 내역 반환"""
    dart_key = _get_secret("DART_API_KEY")
    end_de = date.today()
    # bgn_de는 당해 연도 1월 1일 이상으로 제한 (DART API 제약)
    year_start = date(end_de.year, 1, 1)
    bgn_de = max(end_de - timedelta(days=days), year_start)

    base_params = {
        "crtfc_key": dart_key,
        "pblntf_ty": "D",
        "bgn_de": bgn_de.strftime("%Y%m%d"),
        "end_de": end_de.strftime("%Y%m%d"),
        "sort": "date",
        "sort_mth": "desc",
        "page_count": 100,
    }

    nps_items = []
    page = 1
    while True:
        try:
            resp = requests.get(
                f"{DART_API_BASE}/list.json",
                params={**base_params, "page_no": page},
                timeout=15,
            )
            resp.raise_for_status()
            data = resp.json()
        except Exception as e:
            print(f"  [경고] DART 목록 조회 실패 (p{page}): {e}")
            break

        if data.get("status") not in ("000",):
            break

        items = data.get("list", [])
        if not items:
            break

        nps_items.extend(i for i in items if "국민연금" in i.get("flr_nm", ""))

        total_page = data.get("total_page", 1)
        if page >= total_page:
            break
        page += 1

    print(f"  국민연금 공시 {len(nps_items)}건 발견 (총 {page}페이지 스캔)")

    # 이미 발송된 공시 제외
    if sent_rcept_nos:
        before = len(nps_items)
        nps_items = [i for i in nps_items if i["rcept_no"] not in sent_rcept_nos]
        print(f"  이미 발송 제외: {before - len(nps_items)}건 → 신규 {len(nps_items)}건")

    # 중복 제거: 같은 종목 공시가 여러 건일 때 가장 최신 1건만
    seen = {}
    for item in nps_items:
        corp_code = item["corp_code"]
        if corp_code not in seen:
            seen[corp_code] = item

    trades = []
    for item in seen.values():
        rcept_no = item["rcept_no"]
        corp_name = item["corp_name"]
        rcept_dt = item["rcept_dt"]  # YYYYMMDD
        dart_url = f"https://dart.fss.or.kr/dsaf001/main.do?rcpNo={rcept_no}"

        report_nm = item.get("report_nm", "")
        detail = _fetch_majorstock_detail(dart_key, item["corp_code"], rcept_no, report_nm)
        direction, prev_ratio, curr_ratio, qty_change = detail

        stock_code = item.get("stock_code", "")
        if fetch_prices:
            price, total_amount = _get_stock_price_and_amount(stock_code, rcept_dt, qty_change)
        else:
            price, total_amount = None, None

        trades.append({
            "corp_name": corp_name,
            "date": f"{rcept_dt[:4]}.{rcept_dt[4:6]}.{rcept_dt[6:]}",
            "direction": direction,
            "prev_ratio": prev_ratio,
            "curr_ratio": curr_ratio,
            "qty_change": qty_change,
            "price": price,
            "total_amount": total_amount,
            "url": dart_url,
        })

    trades.sort(key=lambda x: x["date"], reverse=True)
    new_rcept_nos = {item["rcept_no"] for item in nps_items}
    return trades[:30], new_rcept_nos


def _fetch_majorstock_detail(dart_key, corp_code, rcept_no, report_nm=""):
    """공시 타입에 따라 majorstock 또는 elestock API 조회 → (방향, 이전보유율, 현재보유율, 변동주식수)"""
    try:
        is_elestock = "임원" in report_nm or "주요주주" in report_nm

        if is_elestock:
            resp = requests.get(
                f"{DART_API_BASE}/elestock.json",
                params={"crtfc_key": dart_key, "corp_code": corp_code},
                timeout=10,
            )
            data = resp.json()
            if data.get("status") != "000":
                return ("변동", None, None, None)

            records = [r for r in data.get("list", []) if "국민연금" in r.get("repror", "")]
            records.sort(key=lambda r: r.get("rcept_dt", ""), reverse=True)
            if not records:
                return ("변동", None, None, None)

            matched = next((r for r in records if r.get("rcept_no") == rcept_no), records[0])
            curr_ratio = _parse_ratio(matched.get("sp_stock_lmp_rate", "0"))
            qty_change = _parse_ratio(matched.get("sp_stock_lmp_irds_cnt", "0"))
            ratio_change = _parse_ratio(matched.get("sp_stock_lmp_irds_rate", "0"))
        else:
            resp = requests.get(
                f"{DART_API_BASE}/majorstock.json",
                params={"crtfc_key": dart_key, "corp_code": corp_code},
                timeout=10,
            )
            data = resp.json()
            if data.get("status") != "000":
                return ("변동", None, None, None)

            records = [r for r in data.get("list", []) if "국민연금" in r.get("repror", "")]
            records.sort(key=lambda r: r.get("rcept_dt", ""), reverse=True)
            if not records:
                return ("변동", None, None, None)

            matched = next((r for r in records if r.get("rcept_no") == rcept_no), records[0])
            curr_ratio = _parse_ratio(matched.get("stkrt", "0"))
            qty_change = _parse_ratio(matched.get("stkqy_irds", "0"))
            ratio_change = _parse_ratio(matched.get("stkrt_irds", "0"))

        direction = "매수" if qty_change >= 0 else "매도"
        prev_ratio = curr_ratio - ratio_change if ratio_change != 0 else None
        return (direction, prev_ratio, curr_ratio, qty_change)
    except Exception:
        return ("변동", None, None, None)


def _get_stock_price_and_amount(stock_code, rcept_dt, qty_change):
    """공시일 기준 종가 조회 → (주당가격, 총거래금액)"""
    if not stock_code or qty_change is None:
        return (None, None)
    try:
        # 공시일부터 최대 5 영업일 앞뒤로 시도 (휴장일 대응)
        from datetime import datetime
        dt = datetime.strptime(rcept_dt, "%Y%m%d")
        start = (dt - timedelta(days=7)).strftime("%Y%m%d")
        end = rcept_dt

        df = krx_stock.get_market_ohlcv(start, end, stock_code)
        if df.empty:
            return (None, None)

        close_price = int(df["종가"].iloc[-1])  # 공시일 또는 직전 거래일 종가
        total = abs(qty_change) * close_price
        return (close_price, total)
    except Exception:
        return (None, None)


def _parse_ratio(val):
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return 0.0


def build_html(items, period_label, value_col, trades=None):
    today = date.today()
    total = sum(i["value"] for i in items if "합계" not in i["name"] and i["name"] not in ("합 계",))
    colors = ["#1a237e", "#1565c0", "#0277bd", "#00838f", "#2e7d32",
              "#f57f17", "#e65100", "#b71c1c", "#6a1b9a", "#4a148c"]

    rows_html = ""
    for idx, item in enumerate(items):
        is_total = "합계" in item["name"] or item["name"] in ("합 계",)
        pct = (item["value"] / total * 100) if total > 0 and not is_total else None
        bar_width = f"{min(pct, 100):.1f}%" if pct is not None else "0%"
        color = colors[idx % len(colors)]
        pct_str = f"{pct:.1f}%" if pct is not None else "-"
        row_style = "background:#f8f9ff; font-weight:700;" if is_total else ""
        rows_html += f"""
        <tr style="{row_style}">
            <td style="padding:10px 16px; border-bottom:1px solid #f0f0f0; color:#333; font-size:14px;">{item['name']}</td>
            <td style="padding:10px 16px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:14px; font-weight:600; color:#1a237e;">{item['value']:,.0f}</td>
            <td style="padding:10px 16px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:13px; color:#888;">{pct_str}</td>
            <td style="padding:10px 16px; border-bottom:1px solid #f0f0f0; width:120px;">
                <div style="background:#eee; border-radius:4px; height:8px;">
                    <div style="background:{color}; border-radius:4px; height:8px; width:{bar_width};"></div>
                </div>
            </td>
        </tr>"""

    unit = "십억원" if value_col and "십억" in value_col else (value_col or "")
    # 조원 단위 환산 (십억원 기준: 1,000 십억원 = 1조원)
    total_jo = total / 1000 if unit == "십억원" else total
    total_display = f"{total_jo:,.1f}조원"

    # DART 매수/매도 섹션
    if trades:
        trade_rows = ""
        for t in trades:
            is_buy = t["direction"] == "매수"
            badge_color = "#1b5e20" if is_buy else "#b71c1c"
            badge_bg = "#e8f5e9" if is_buy else "#ffebee"
            badge_text = "▲ 매수" if is_buy else "▼ 매도"
            ratio_text = ""
            if t["prev_ratio"] is not None and t["curr_ratio"] is not None:
                ratio_text = f"{round(t['prev_ratio'], 2):.2f}% → {round(t['curr_ratio'], 2):.2f}%"
            elif t["curr_ratio"] is not None:
                ratio_text = f"{round(t['curr_ratio'], 2):.2f}%"
            qty_text = f"{abs(int(t['qty_change'])):,}주" if t.get("qty_change") is not None else "-"
            price_text = f"{t['price']:,}원" if t.get("price") else "-"
            amount_text = f"{int(t['total_amount'] / 1e8):,}억원" if t.get("total_amount") else "-"
            trade_rows += f"""
            <tr>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#555; white-space:nowrap;">{t['date']}</td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:13px; font-weight:600; color:#1a237e; white-space:nowrap;">
                <a href="{t['url']}" style="color:#1a237e; text-decoration:none;">{t['corp_name']}</a>
              </td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; text-align:center; white-space:nowrap;">
                <span style="background:{badge_bg}; color:{badge_color}; font-size:11px; font-weight:700; padding:2px 8px; border-radius:10px;">{badge_text}</span>
              </td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#666; text-align:right; white-space:nowrap;">{ratio_text}</td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#555; text-align:right; white-space:nowrap;">{qty_text}</td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#555; text-align:right; white-space:nowrap;">{price_text}</td>
              <td style="padding:8px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; font-weight:600; color:#333; text-align:right; white-space:nowrap;">{amount_text}</td>
            </tr>"""
        dart_section = f"""
    <div style="padding:28px 36px; border-top:2px solid #e3e8f0;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 6px; font-weight:700;">최근 30일 국민연금 매수/매도 내역</h2>
      <p style="font-size:11px; color:#9e9e9e; margin:0 0 16px;">출처: DART 주식등의대량보유상황보고서 &nbsp;|&nbsp; 주가: 공시일 종가 기준</p>
      <div style="overflow-x:auto;">
      <table style="width:100%; min-width:560px; border-collapse:collapse;">
        <thead>
          <tr style="background:#f5f7ff;">
            <th style="padding:8px 10px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">공시일</th>
            <th style="padding:8px 10px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">종목</th>
            <th style="padding:8px 10px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">구분</th>
            <th style="padding:8px 10px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">보유비율</th>
            <th style="padding:8px 10px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">변동주식수</th>
            <th style="padding:8px 10px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">주당가격</th>
            <th style="padding:8px 10px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">추정금액</th>
          </tr>
        </thead>
        <tbody>{trade_rows}</tbody>
      </table>
      </div>
    </div>"""
    else:
        dart_section = """
    <div style="padding:20px 36px; border-top:2px solid #e3e8f0; text-align:center;">
      <p style="font-size:13px; color:#9e9e9e; margin:0;">최근 30일간 국민연금 대량보유 공시 내역이 없습니다.</p>
    </div>"""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background:#eef2f7; font-family:-apple-system,'Apple SD Gothic Neo','Malgun Gothic',sans-serif;">
  <div style="max-width:640px; margin:40px auto; background:#fff; border-radius:16px; overflow:hidden; box-shadow:0 4px 24px rgba(0,0,0,0.10);">

    <div style="background:linear-gradient(135deg, #0d1b4b 0%, #1a237e 60%, #283593 100%); padding:44px 36px 36px; text-align:center;">
      <div style="display:inline-block; background:rgba(255,255,255,0.12); border-radius:8px; padding:4px 14px; font-size:11px; color:#90caf9; letter-spacing:2px; margin-bottom:14px;">MONTHLY NEWSLETTER</div>
      <h1 style="margin:0 0 10px; color:#fff; font-size:26px; font-weight:800;">국민연금 포트폴리오 현황</h1>
      <p style="margin:0; color:#90caf9; font-size:14px;">{period_label} 기준 &nbsp;|&nbsp; 발송일 {today.strftime('%Y.%m.%d')}</p>
    </div>

    <div style="background:#f0f4ff; padding:28px 36px; border-bottom:1px solid #e3e8f0;">
      <div style="font-size:12px; color:#7986cb; font-weight:600; letter-spacing:1px; margin-bottom:6px;">총 운용자산</div>
      <div style="font-size:36px; font-weight:800; color:#1a237e;">{total_display}</div>
    </div>

    <div style="padding:28px 36px;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 16px; font-weight:700;">자산 분류별 현황</h2>
      <table style="width:100%; border-collapse:collapse;">
        <thead>
          <tr style="background:#f5f7ff;">
            <th style="padding:10px 16px; text-align:left; font-size:12px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">구분</th>
            <th style="padding:10px 16px; text-align:right; font-size:12px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">{unit}</th>
            <th style="padding:10px 16px; text-align:right; font-size:12px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">비중</th>
            <th style="padding:10px 16px; border-bottom:2px solid #e3e8f0; width:120px;"></th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>

    {dart_section}

    <div style="background:#f5f7ff; padding:20px 36px; border-top:1px solid #e3e8f0; text-align:center;">
      <p style="margin:0 0 4px; font-size:12px; color:#9e9e9e;">출처: 국민연금공단 &nbsp;|&nbsp; 공공데이터포털 (data.go.kr) &nbsp;|&nbsp; DART 전자공시</p>
      <p style="margin:0; font-size:11px; color:#bdbdbd;">이 메일은 매월 1일 자동 발송됩니다.</p>
    </div>
  </div>
</body>
</html>"""


def send_email(html_content, subject):
    payload = {
        "personalizations": [{"to": [{"email": RECIPIENT_EMAIL}]}],
        "from": {"email": SENDER_EMAIL},
        "subject": subject,
        "content": [{"type": "text/html", "value": html_content}],
    }
    resp = requests.post(
        "https://api.sendgrid.com/v3/mail/send",
        headers={"Authorization": f"Bearer {SENDGRID_API_KEY}", "Content-Type": "application/json"},
        data=json.dumps(payload),
        timeout=15,
    )
    if resp.status_code == 202:
        print(f"  이메일 전송 완료 → {RECIPIENT_EMAIL}")
    else:
        print(f"  [오류] SendGrid 응답: {resp.status_code} {resp.text}")
        sys.exit(1)


def check_env():
    missing = [k for k in ["NPS_API_KEY", "SENDGRID_API_KEY", "SENDER_EMAIL", "RECIPIENT_EMAIL", "DART_API_KEY"] if not os.getenv(k)]
    if missing:
        print(f"[오류] .env 파일에 다음 항목이 없습니다: {', '.join(missing)}")
        sys.exit(1)


def main():
    check_env()
    print("국민연금 공시 알림 확인 중...")

    # 1. 이미 발송된 공시 로드
    sent_rcept_nos = load_sent_rcept_nos()
    print(f"[1/4] 기발송 공시 {len(sent_rcept_nos)}건 로드")

    # 2. 신규 공시 조회
    print("[2/4] DART 신규 공시 스캔 중...")
    trades, new_rcept_nos = fetch_dart_nps_trades(days=30, sent_rcept_nos=sent_rcept_nos)

    if not trades:
        print("  → 신규 공시 없음. 발송 생략.")
        return

    print(f"  → 신규 공시 {len(trades)}건 발견. 이메일 발송 진행.")

    # 3. 포트폴리오 현황 조회 (컨텍스트용)
    print("[3/4] 포트폴리오 현황 조회 중...")
    url, period_label = get_latest_endpoint()
    data = fetch_portfolio_data(url)
    items, value_col = parse_items(data)
    print(f"  {len(items)}개 항목 조회 완료")

    # 4. 이메일 발송
    print("[4/4] 이메일 발송 중...")
    latest_date = trades[0]["date"]
    subject = f"[국민연금] 매수/매도 공시 알림 - {latest_date}"
    html = build_html(items, period_label, value_col, trades=trades)
    send_email(html, subject)

    # 5. 발송 상태 저장
    save_sent_rcept_nos(sent_rcept_nos | new_rcept_nos)
    print("완료!")


if __name__ == "__main__":
    main()
