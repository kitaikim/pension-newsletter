#!/usr/bin/env python3
"""국민연금 포트폴리오 뉴스레터 자동 발송 스크립트"""

import os
import sys
import json
import smtplib
import requests
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
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
GMAIL_USER = _get_secret("GMAIL_USER")
GMAIL_APP_PASSWORD = _get_secret("GMAIL_APP_PASSWORD")
RECIPIENT_EMAIL = _get_secret("RECIPIENT_EMAIL")

API_BASE = "https://api.odcloud.kr/api"
DART_API_BASE = "https://opendart.fss.or.kr/api"
KIS_BASE = "https://openapi.koreainvestment.com:9443"
KIS_TOKEN_FILE = os.path.join(os.path.dirname(__file__), "kis_token.json")
STATE_FILE = os.path.join(os.path.dirname(__file__), "last_sent.json")
TRADES_CACHE_FILE = os.path.join(os.path.dirname(__file__), "trades_cache.json")


def load_trades_cache():
    """대시보드용 캐시 로드"""
    if not os.path.exists(TRADES_CACHE_FILE):
        return [], None
    with open(TRADES_CACHE_FILE, encoding="utf-8") as f:
        data = json.load(f)
    return data.get("trades", []), data.get("updated")


def save_trades_cache(trades):
    """대시보드용 캐시 저장"""
    with open(TRADES_CACHE_FILE, "w", encoding="utf-8") as f:
        json.dump({"trades": trades, "updated": date.today().isoformat()}, f, ensure_ascii=False, indent=2)


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
    for attempt in range(2):
        try:
            resp = requests.get(url, params={"serviceKey": API_KEY, "perPage": 1, "returnType": "JSON"}, timeout=30)
            resp.raise_for_status()
            data = resp.json()
            break
        except Exception as e:
            if attempt == 1:
                raise
            print(f"  [재시도] 엔드포인트 조회 실패: {e}")
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


def fetch_market_summary():
    """KOSPI/KOSDAQ 전일 등락률 + KOSPI YTD(KODEX200 proxy) + USD/KRW 환율

    - 지수 현재가/등락률: Naver Finance 비공식 API (pykrx get_index_ohlcv는 KRX 로그인 필요)
    - KOSPI YTD: KODEX 200 (069500) ETF로 근사 (pykrx get_market_ohlcv는 정상 작동)
    - USD/KRW: open.er-api.com 무료 환율 API
    """
    result = {}
    try:
        for nv_code, key in [("KOSPI", "kospi"), ("KOSDAQ", "kosdaq")]:
            try:
                resp = requests.get(
                    f"https://m.stock.naver.com/api/index/{nv_code}/price",
                    headers={"User-Agent": "Mozilla/5.0"},
                    timeout=5,
                )
                if resp.ok:
                    items = resp.json()
                    item = items[0] if isinstance(items, list) and items else {}
                    close_str = item.get("closePrice", "").replace(",", "")
                    chg_str = item.get("fluctuationsRatio", "")
                    if close_str:
                        result[key] = {
                            "close": float(close_str),
                            "chg": float(chg_str) if chg_str else None,
                            "date": item.get("localTradedAt", "")[-5:].replace("-", "/"),
                        }
            except Exception:
                pass

        # KOSPI YTD — KODEX 200 (069500) ETF 수익률로 근사
        try:
            ytd_start = date(date.today().year, 1, 2).strftime("%Y%m%d")
            ytd_end = date.today().strftime("%Y%m%d")
            df_ytd = krx_stock.get_market_ohlcv(ytd_start, ytd_end, "069500")
            if df_ytd is not None and not df_ytd.empty and len(df_ytd) >= 2:
                ytd = (float(df_ytd["종가"].iloc[-1]) - float(df_ytd["종가"].iloc[0])) / float(df_ytd["종가"].iloc[0]) * 100
                result.setdefault("kospi", {})["ytd"] = ytd
        except Exception:
            pass

        # USD/KRW (open.er-api.com 무료, 인증 불필요)
        try:
            resp = requests.get("https://open.er-api.com/v6/latest/USD", timeout=5)
            if resp.ok:
                krw = resp.json().get("rates", {}).get("KRW")
                if krw:
                    result["usdkrw"] = float(krw)
        except Exception:
            pass

    except Exception as e:
        print(f"[WARN] 시장 요약 조회 실패: {e}")
    return result


def fetch_portfolio_data(url):
    params = {"serviceKey": API_KEY, "page": 1, "perPage": 100, "returnType": "JSON"}
    for attempt in range(2):
        try:
            resp = requests.get(url, params=params, timeout=30)
            resp.raise_for_status()
            return resp.json()
        except Exception as e:
            if attempt == 1:
                raise
            print(f"  [재시도] 포트폴리오 조회 실패: {e}")
    return None


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

    # KOSPI 52주 수익률은 종목마다 반복 조회 방지하기 위해 1회 계산
    kospi_52w = _get_kospi_52w_return() if fetch_prices else None

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
            tech = _get_technical_indicators(stock_code, kospi_52w_return=kospi_52w)
            current_price = tech.get("current_price") or _get_current_price(stock_code)
            since_return = None
            if price and current_price and price > 0:
                since_return = (current_price - price) / price * 100
            investor_daily = _get_investor_daily_kis(stock_code, num_days=5)
            foreign_daily = [{"date": d["date"], "net": d["frgn"]} for d in investor_daily]
            foreign_net = sum(d["frgn"] for d in investor_daily) if investor_daily else None
            org_net = sum(d["orgn"] for d in investor_daily) if investor_daily else None
            prsn_net = sum(d["prsn"] for d in investor_daily) if investor_daily else None
            scrt_net = sum(d["scrt"] for d in investor_daily) if investor_daily else None
            # 외국인 연속 순매수/순매도 일수 (최신→과거 순으로 계산)
            frgn_streak = 0
            if investor_daily:
                sign = 1 if investor_daily[0]["frgn"] > 0 else -1 if investor_daily[0]["frgn"] < 0 else 0
                for d in investor_daily:
                    cur_sign = 1 if d["frgn"] > 0 else -1 if d["frgn"] < 0 else 0
                    if cur_sign == sign and sign != 0:
                        frgn_streak += 1
                    else:
                        break
                frgn_streak = frgn_streak * sign  # 양수=연속매수, 음수=연속매도
        else:
            price, total_amount = None, None
            current_price = None
            since_return = None
            tech = {}
            foreign_daily = []
            foreign_net = None
            org_net = None
            prsn_net = None
            scrt_net = None
            frgn_streak = 0

        trades.append({
            "corp_name": corp_name,
            "date": f"{rcept_dt[:4]}.{rcept_dt[4:6]}.{rcept_dt[6:]}",
            "direction": direction,
            "prev_ratio": prev_ratio,
            "curr_ratio": curr_ratio,
            "qty_change": qty_change,
            "price": price,
            "current_price": current_price,
            "since_return": since_return,
            "total_amount": total_amount,
            "foreign_net": foreign_net,
            "foreign_daily": foreign_daily,
            "frgn_streak": frgn_streak,
            "org_net": org_net,
            "prsn_net": prsn_net,
            "scrt_net": scrt_net,
            "tech": tech,
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


def _get_kis_token():
    """KIS OAuth 토큰 발급 (하루 캐시)"""
    appkey = _get_secret("KIS_APPKEY")
    appsecret = _get_secret("KIS_APPSECRET")
    if not appkey or not appsecret:
        return None, None, None

    # 캐시된 토큰 확인
    if os.path.exists(KIS_TOKEN_FILE):
        with open(KIS_TOKEN_FILE) as f:
            cached = json.load(f)
        if cached.get("expires", "") >= date.today().isoformat():
            return cached["access_token"], appkey, appsecret

    # 새 토큰 발급
    try:
        resp = requests.post(
            f"{KIS_BASE}/oauth2/tokenP",
            json={"grant_type": "client_credentials", "appkey": appkey, "appsecret": appsecret},
            timeout=10,
        )
        token = resp.json().get("access_token")
        if not token:
            return None, None, None
        with open(KIS_TOKEN_FILE, "w") as f:
            json.dump({"access_token": token, "expires": (date.today() + timedelta(days=1)).isoformat()}, f)
        return token, appkey, appsecret
    except Exception:
        return None, None, None


def _get_investor_daily_kis(stock_code, num_days=5):
    """KIS '종목별 투자자매매동향(일별)' (FHPTJ04160001) 으로 외국인/기관/금융투자/개인 일별 순매수.

    한 번 호출로 약 30일치 list 반환됨 (output2). 단위: 백만원, 최신순.
    Returns: [{"date": "MMDD", "frgn": int, "orgn": int, "prsn": int, "scrt": int}, ...]
    실패/없음 시 빈 리스트.

    필드 매핑:
      frgn_ntby_tr_pbmn → frgn (외국인)
      orgn_ntby_tr_pbmn → orgn (기관 합계 = 금융투자+투신+사모+보험+은행+연기금+기타금융)
      prsn_ntby_tr_pbmn → prsn (개인)
      scrt_ntby_tr_pbmn → scrt (금융투자, 증권사 자기매매)
    """
    if not stock_code:
        return []
    token, appkey, appsecret = _get_kis_token()
    if not token:
        return []
    try:
        # endpoint는 장 마감(15:40 KST) 후만 당일 데이터 제공 → 가장 최근 완료 거래일로 호출
        # 토/일은 보정 (한국 공휴일은 별도 처리 안 함 — endpoint가 rt_cd!=0 반환하면 빈 결과)
        d = date.today() - timedelta(days=1)
        while d.weekday() >= 5:
            d -= timedelta(days=1)
        target_str = d.strftime("%Y%m%d")
        resp = requests.get(
            f"{KIS_BASE}/uapi/domestic-stock/v1/quotations/investor-trade-by-stock-daily",
            headers={
                "authorization": f"Bearer {token}",
                "appkey": appkey,
                "appsecret": appsecret,
                "tr_id": "FHPTJ04160001",
            },
            params={
                "FID_COND_MRKT_DIV_CODE": "J",
                "FID_INPUT_ISCD": stock_code,
                "FID_INPUT_DATE_1": target_str,
                "FID_ORG_ADJ_PRC": "",
                "FID_ETC_CLS_CODE": "",
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("rt_cd") != "0":
            return []
        output = data.get("output2", [])
        if not isinstance(output, list):
            return []
        daily = []
        for item in output:
            if len(daily) >= num_days:
                break
            d = item.get("stck_bsop_date", "")
            if not d or len(d) != 8:
                continue
            f_raw = item.get("frgn_ntby_tr_pbmn", "")
            o_raw = item.get("orgn_ntby_tr_pbmn", "")
            p_raw = item.get("prsn_ntby_tr_pbmn", "")
            s_raw = item.get("scrt_ntby_tr_pbmn", "")
            if all(v in ("", None) for v in (f_raw, o_raw, p_raw, s_raw)):
                continue
            try:
                daily.append({
                    "date": d[4:8],
                    "frgn": int(f_raw) if f_raw not in ("", None) else 0,
                    "orgn": int(o_raw) if o_raw not in ("", None) else 0,
                    "prsn": int(p_raw) if p_raw not in ("", None) else 0,
                    "scrt": int(s_raw) if s_raw not in ("", None) else 0,
                })
            except (ValueError, TypeError):
                continue
        return daily
    except Exception:
        return []


def _get_foreign_daily_kis(stock_code, num_days=5):
    """[backward compat] 외국인 일별만 반환. 새 코드는 _get_investor_daily_kis 사용."""
    return [{"date": d["date"], "net": d["frgn"]} for d in _get_investor_daily_kis(stock_code, num_days)]


def _fetch_market_cap_top_kis(market_iscd: str, limit: int = 30):
    """KIS '국내주식 시가총액 상위'(FHPST01740000)로 종목 리스트 반환.

    market_iscd: '0001'=KOSPI 전체, '1001'=KOSDAQ, '2001'=KOSPI200
    한 번 호출에 30종목 반환 (페이지네이션 안 됨).
    Returns: [{"code": "005930", "name": "삼성전자"}, ...]
    """
    token, appkey, appsecret = _get_kis_token()
    if not token:
        return []
    try:
        resp = requests.get(
            f"{KIS_BASE}/uapi/domestic-stock/v1/ranking/market-cap",
            headers={
                "authorization": f"Bearer {token}",
                "appkey": appkey,
                "appsecret": appsecret,
                "tr_id": "FHPST01740000",
            },
            params={
                "fid_input_price_2": "",
                "fid_cond_mrkt_div_code": "J",
                "fid_cond_scr_div_code": "20174",
                "fid_div_cls_code": "0",
                "fid_input_iscd": market_iscd,
                "fid_trgt_cls_code": "0",
                "fid_trgt_exls_cls_code": "0",
                "fid_input_price_1": "",
                "fid_vol_cnt": "",
            },
            timeout=10,
        )
        data = resp.json()
        if data.get("rt_cd") != "0":
            return []
        return [
            {"code": it.get("mksc_shrn_iscd", ""), "name": it.get("hts_kor_isnm", "")}
            for it in data.get("output", [])[:limit]
            if it.get("mksc_shrn_iscd")
        ]
    except Exception:
        return []


def fetch_market_movers(top_n_per_market: int = 30, ranking_size: int = 5):
    """KOSPI/KOSDAQ 시가총액 상위 종목 대상 외인/기관/금융투자 5일합 ranking.

    Returns: {
      "frgn": {"buy": [{"code","name","market","net"}], "sell": [...]},
      "orgn": {"buy": [...], "sell": [...]},
      "scrt": {"buy": [...], "sell": [...]},
    }
    net 단위: 백만원. 빈 dict 반환 가능 (KIS 토큰 없음 등).
    """
    kospi = _fetch_market_cap_top_kis("0001", top_n_per_market)
    kosdaq = _fetch_market_cap_top_kis("1001", top_n_per_market)
    if not kospi and not kosdaq:
        return {}

    universe = (
        [{**s, "market": "KOSPI"} for s in kospi]
        + [{**s, "market": "KOSDAQ"} for s in kosdaq]
    )
    print(f"  시장 종목 {len(universe)}개 ({len(kospi)} KOSPI + {len(kosdaq)} KOSDAQ)")

    rows = []
    for s in universe:
        daily = _get_investor_daily_kis(s["code"], num_days=5)
        if not daily:
            continue
        rows.append({
            "code": s["code"],
            "name": s["name"],
            "market": s["market"],
            "frgn": sum(d["frgn"] for d in daily),
            "orgn": sum(d["orgn"] for d in daily),
            "scrt": sum(d["scrt"] for d in daily),
        })

    if not rows:
        return {}

    result = {}
    for key in ("frgn", "orgn", "scrt"):
        sorted_rows = sorted(rows, key=lambda r: r[key], reverse=True)
        result[key] = {
            "buy": [
                {"code": r["code"], "name": r["name"], "market": r["market"], "net": r[key]}
                for r in sorted_rows[:ranking_size] if r[key] > 0
            ],
            "sell": [
                {"code": r["code"], "name": r["name"], "market": r["market"], "net": r[key]}
                for r in sorted_rows[-ranking_size:][::-1] if r[key] < 0
            ],
        }
    return result


def _get_foreign_trading_kis(stock_code, rcept_dt):
    """[backward compat] 최근 5일 외국인 순매수 합산 (백만원). 새 코드는 _get_investor_daily_kis 사용."""
    daily = _get_foreign_daily_kis(stock_code, num_days=5)
    if not daily:
        return None
    total = sum(d["net"] for d in daily)
    return total if total != 0 else None


def _get_stock_price_and_amount(stock_code, rcept_dt, qty_change):
    """공시일 기준 종가 조회 → (주당가격, 총거래금액)"""
    if not stock_code or qty_change is None:
        return (None, None)
    try:
        from datetime import datetime
        dt = datetime.strptime(rcept_dt, "%Y%m%d")
        start = (dt - timedelta(days=7)).strftime("%Y%m%d")
        end = rcept_dt

        df = krx_stock.get_market_ohlcv(start, end, stock_code)
        if df.empty:
            return (None, None)

        close_price = int(df["종가"].iloc[-1])
        total = abs(qty_change) * close_price
        return (close_price, total)
    except Exception:
        return (None, None)




def _get_current_price(stock_code):
    """오늘 기준 최근 종가 (공시 이후 수익률 계산용)"""
    if not stock_code:
        return None
    try:
        end = date.today()
        start = (end - timedelta(days=7)).strftime("%Y%m%d")
        df = krx_stock.get_market_ohlcv(start, end.strftime("%Y%m%d"), stock_code)
        if df is None or df.empty:
            return None
        return int(df["종가"].iloc[-1])
    except Exception:
        return None


def _calc_rsi(closes, period=14):
    """Wilder's RSI 계산 (closes: 오래된→최신 순 float list, 최소 period+1개 필요)"""
    if len(closes) < period + 1:
        return None
    deltas = [closes[i] - closes[i - 1] for i in range(1, len(closes))]
    gains = [d if d > 0 else 0.0 for d in deltas]
    losses = [-d if d < 0 else 0.0 for d in deltas]
    avg_gain = sum(gains[:period]) / period
    avg_loss = sum(losses[:period]) / period
    for i in range(period, len(deltas)):
        avg_gain = (avg_gain * (period - 1) + gains[i]) / period
        avg_loss = (avg_loss * (period - 1) + losses[i]) / period
    if avg_loss == 0:
        return 100.0
    return round(100.0 - 100.0 / (1 + avg_gain / avg_loss), 1)


def _get_kospi_52w_return():
    """KOSPI(069500 KODEX200) 52주 수익률 — 종목별 RS 계산용 기준값"""
    try:
        end = date.today()
        start = (end - timedelta(days=380)).strftime("%Y%m%d")
        df = krx_stock.get_market_ohlcv(start, end.strftime("%Y%m%d"), "069500")
        if df is None or df.empty or len(df) < 2:
            return None
        return (float(df["종가"].iloc[-1]) - float(df["종가"].iloc[0])) / float(df["종가"].iloc[0]) * 100
    except Exception:
        return None


def _get_technical_indicators(stock_code, kospi_52w_return=None):
    """280일 OHLCV → 기술적 지표 dict 반환.

    Returns:
      current_price (int), rsi (float), ma20_above (bool), ma200_above (bool),
      ma200_gap_pct (float), week52_pos (float 0~100%),
      week52_low (int), week52_high (int), rs_vs_kospi (float | None)
    실패 시 빈 dict {}
    """
    if not stock_code:
        return {}
    try:
        end = date.today()
        start = (end - timedelta(days=380)).strftime("%Y%m%d")  # 52주+버퍼
        df = krx_stock.get_market_ohlcv(start, end.strftime("%Y%m%d"), stock_code)
        if df is None or df.empty or len(df) < 30:
            return {}

        closes = [float(p) for p in df["종가"].tolist()]
        current = int(closes[-1])

        # RSI(14)
        rsi = _calc_rsi(closes[-30:], period=14)  # 최근 30개 충분

        # 이평선
        ma20 = sum(closes[-20:]) / 20 if len(closes) >= 20 else None
        ma200 = sum(closes[-200:]) / 200 if len(closes) >= 200 else None
        ma20_above = (current > ma20) if ma20 else None
        ma200_above = (current > ma200) if ma200 else None
        ma200_gap_pct = ((current - ma200) / ma200 * 100) if ma200 else None

        # 52주 고/저 (최근 252 거래일)
        recent_252 = closes[-252:] if len(closes) >= 252 else closes
        week52_low = int(min(recent_252))
        week52_high = int(max(recent_252))
        rng = week52_high - week52_low
        week52_pos = ((current - week52_low) / rng * 100) if rng > 0 else 50.0

        # 52주 수익률 vs KOSPI
        stock_52w = (current - recent_252[0]) / recent_252[0] * 100
        rs_vs_kospi = (stock_52w - kospi_52w_return) if kospi_52w_return is not None else None

        return {
            "current_price": current,
            "rsi": rsi,
            "ma20_above": ma20_above,
            "ma200_above": ma200_above,
            "ma200_gap_pct": round(ma200_gap_pct, 1) if ma200_gap_pct is not None else None,
            "week52_pos": round(week52_pos, 1),
            "week52_low": week52_low,
            "week52_high": week52_high,
            "rs_vs_kospi": round(rs_vs_kospi, 1) if rs_vs_kospi is not None else None,
        }
    except Exception:
        return {}


def _parse_ratio(val):
    try:
        return float(str(val).replace(",", "").replace("%", "").strip())
    except (ValueError, TypeError):
        return 0.0


def _build_foreign_daily_section(trades):
    """외국인 일별 매매 매트릭스 섹션 — 종목 × 거래일 5컬럼.

    상위 20종목 (5거래일 합 절대값 큰 순) 표시.
    날짜 헤더는 trades 전체에서 가장 흔한 5거래일을 추출 (휴장일 보정).
    """
    rows_data = [t for t in (trades or []) if t.get("foreign_daily")]
    if not rows_data:
        return ""

    from collections import Counter
    date_counter = Counter()
    for t in rows_data:
        for d in t["foreign_daily"]:
            date_counter[d["date"]] += 1
    top_dates = sorted([d for d, _ in date_counter.most_common(5)])  # 오래된 → 최신
    if not top_dates:
        return ""

    sorted_trades = sorted(rows_data, key=lambda t: abs(t.get("foreign_net") or 0), reverse=True)[:20]

    def _fmt_cell(net):
        if net is None:
            return '<td style="padding:7px 8px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:12px; color:#ccc;">·</td>'
        eok = net // 100
        if eok > 0:
            return f'<td style="padding:7px 8px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:12px; color:#1b5e20; font-weight:700;">▲{eok:,}</td>'
        if eok < 0:
            return f'<td style="padding:7px 8px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:12px; color:#b71c1c; font-weight:700;">▼{abs(eok):,}</td>'
        return '<td style="padding:7px 8px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:12px; color:#999;">-</td>'

    def _fmt_sum(fn):
        eok = (fn or 0) // 100
        color = "#1b5e20" if eok > 0 else "#b71c1c" if eok < 0 else "#888"
        sign = "▲" if eok > 0 else "▼" if eok < 0 else ""
        return f'<td style="padding:7px 10px; border-bottom:1px solid #f0f0f0; border-left:1px solid #e3e8f0; text-align:right; font-size:12px; font-weight:700; color:{color}; background:#fafbff;">{sign}{abs(eok):,}억</td>'

    body_rows = ""
    for t in sorted_trades:
        daily_map = {d["date"]: d["net"] for d in t["foreign_daily"]}
        cells = "".join(_fmt_cell(daily_map.get(d)) for d in top_dates)
        body_rows += f"""
        <tr>
          <td style="padding:7px 10px; border-bottom:1px solid #f0f0f0; font-size:12px; font-weight:600; color:#1a237e; white-space:nowrap;">{t['corp_name']}</td>
          {cells}
          {_fmt_sum(t.get('foreign_net'))}
        </tr>"""

    date_headers = "".join(
        f'<th style="padding:8px 8px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">{d[:2]}.{d[2:]}</th>'
        for d in top_dates
    )

    return f"""
    <div style="padding:24px 16px; border-top:2px solid #e3e8f0;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 6px; font-weight:700;">🌏 외국인 일별 매매 (NPS 종목)</h2>
      <p style="font-size:11px; color:#9e9e9e; margin:0 0 16px;">출처: KIS Open API · 최근 5거래일 일별 외국인 순매수 (단위: 억원, 절대값 큰 순 상위 20)</p>
      <div style="overflow-x:auto;">
      <table style="width:100%; border-collapse:collapse; min-width:480px;">
        <thead>
          <tr style="background:#f5f7ff;">
            <th style="padding:8px 10px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">종목</th>
            {date_headers}
            <th style="padding:8px 10px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; border-left:1px solid #e3e8f0; white-space:nowrap; background:#fafbff;">합계</th>
          </tr>
        </thead>
        <tbody>{body_rows}</tbody>
      </table>
      </div>
    </div>"""


def _build_market_movers_section(movers):
    """시장 전체 매매 동향 섹션 — 외인/기관/금융투자 각 매수·매도 상위.

    movers: fetch_market_movers() 결과 dict. 빈 dict이면 빈 문자열 반환.
    """
    if not movers:
        return ""

    labels = [
        ("frgn", "🌏", "외국인"),
        ("orgn", "🏛", "기관"),
        ("scrt", "💼", "금융투자"),
    ]

    def _fmt_eok(net, color_pos="#1b5e20", color_neg="#b71c1c"):
        eok = int(net) // 100
        if eok > 0:
            return f"<span style='color:{color_pos}; font-weight:700;'>▲{eok:,}억</span>"
        if eok < 0:
            return f"<span style='color:{color_neg}; font-weight:700;'>▼{abs(eok):,}억</span>"
        return "<span style='color:#888;'>-</span>"

    def _mini_table(items, key):
        if not items:
            return "<p style='color:#bbb; font-size:11px; margin:6px 0;'>해당 없음</p>"
        out = "<table style='width:100%; border-collapse:collapse;'>"
        for r in items:
            mkt = "#1565c0" if r["market"] == "KOSPI" else "#558b2f"
            out += f"""
            <tr>
              <td style="padding:5px 6px; border-bottom:1px solid #f0f0f0; font-size:10px;">
                <span style="background:{mkt}; color:#fff; padding:1px 5px; border-radius:3px; font-weight:700;">{r['market']}</span>
              </td>
              <td style="padding:5px 6px; border-bottom:1px solid #f0f0f0; font-size:12px; font-weight:600; color:#1a237e;">{r['name']}</td>
              <td style="padding:5px 6px; border-bottom:1px solid #f0f0f0; text-align:right; font-size:12px;">{_fmt_eok(r['net'])}</td>
            </tr>"""
        out += "</table>"
        return out

    blocks = ""
    for key, emoji, name in labels:
        m = movers.get(key, {})
        blocks += f"""
        <div style="margin:0 0 18px;">
          <h3 style="font-size:13px; color:#1a237e; margin:0 0 8px; font-weight:700;">{emoji} {name}</h3>
          <table style="width:100%; border-collapse:separate; border-spacing:8px 0;">
            <tr>
              <td style="vertical-align:top; width:50%;">
                <p style="font-size:10px; color:#1b5e20; margin:0 0 4px; font-weight:700;">▲ 매수 상위</p>
                {_mini_table(m.get("buy", []), key)}
              </td>
              <td style="vertical-align:top; width:50%;">
                <p style="font-size:10px; color:#b71c1c; margin:0 0 4px; font-weight:700;">▼ 매도 상위</p>
                {_mini_table(m.get("sell", []), key)}
              </td>
            </tr>
          </table>
        </div>"""

    return f"""
    <div style="padding:24px 16px; border-top:2px solid #e3e8f0;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 6px; font-weight:700;">📈 시장 전체 매매 동향</h2>
      <p style="font-size:11px; color:#9e9e9e; margin:0 0 16px;">KOSPI/KOSDAQ 시가총액 상위 60종목 · 최근 5거래일 합산 순매수 (단위: 억원)</p>
      {blocks}
    </div>"""


def _build_technical_section(trades):
    """공시 종목별 기술적 신호 섹션.

    RSI(14) · 200일 이평선 · 52주 위치 · 외국인 연속 · RS vs KOSPI · 매수 적합도 요약
    """
    rows_data = [t for t in (trades or []) if t.get("tech")]
    if not rows_data:
        return ""

    def _rsi_badge(rsi):
        if rsi is None:
            return "<span style='color:#bbb;'>-</span>"
        if rsi >= 70:
            return f"<span style='background:#ffebee; color:#b71c1c; padding:2px 6px; border-radius:8px; font-size:11px; font-weight:700; white-space:nowrap;'>과열 {rsi}</span>"
        if rsi <= 30:
            return f"<span style='background:#e8f5e9; color:#1b5e20; padding:2px 6px; border-radius:8px; font-size:11px; font-weight:700; white-space:nowrap;'>과매도 {rsi}</span>"
        return f"<span style='background:#fff8e1; color:#f57f17; padding:2px 6px; border-radius:8px; font-size:11px; font-weight:700; white-space:nowrap;'>중립 {rsi}</span>"

    def _ma200_badge(above, gap):
        if above is None:
            return "<span style='color:#bbb;'>-</span>"
        color = "#1b5e20" if above else "#b71c1c"
        arrow = "▲" if above else "▼"
        gap_str = f"{gap:+.1f}%" if gap is not None else ""
        return f"<span style='color:{color}; font-weight:700;'>{arrow}{gap_str}</span>"

    def _w52_bar(pos):
        if pos is None:
            return "-"
        # 6칸 텍스트 바 + 수치
        filled = round(pos / 100 * 6)
        bar = "█" * filled + "░" * (6 - filled)
        color = "#1b5e20" if pos >= 70 else "#b71c1c" if pos <= 30 else "#f57f17"
        return f"<span style='font-family:monospace; color:{color}; font-size:10px;'>{bar}</span> <span style='font-size:11px; color:#555;'>{pos:.0f}%</span>"

    def _streak_badge(streak):
        if not streak:
            return "<span style='color:#bbb;'>-</span>"
        if streak > 0:
            return f"<span style='color:#1b5e20; font-weight:700;'>▲{streak}일 연속</span>"
        return f"<span style='color:#b71c1c; font-weight:700;'>▼{abs(streak)}일 연속</span>"

    def _rs_badge(rs):
        if rs is None:
            return "<span style='color:#bbb;'>-</span>"
        color = "#1b5e20" if rs >= 5 else "#b71c1c" if rs <= -5 else "#555"
        return f"<span style='color:{color}; font-weight:700;'>{rs:+.1f}%p</span>"

    def _signal_summary(tech, direction, frgn_streak):
        """3-check: 200일선 위 + RSI<70 + RS>0 → 신호등"""
        score = 0
        checks = []
        rsi = tech.get("rsi")
        ma200 = tech.get("ma200_above")
        rs = tech.get("rs_vs_kospi")
        streak = frgn_streak or 0

        if ma200 is True:
            score += 1
            checks.append("200일↑")
        if rsi is not None and rsi < 70:
            score += 1
            checks.append("RSI OK")
        if rs is not None and rs >= 0:
            score += 1
            checks.append("RS↑")
        if streak > 0:
            score += 1
            checks.append(f"외국인{streak}일↑")

        if score >= 3:
            return f"<span style='background:#e8f5e9; color:#1b5e20; padding:3px 6px; border-radius:10px; font-size:11px; font-weight:700; white-space:nowrap;'>✅매수</span>"
        elif score >= 2:
            return f"<span style='background:#fff8e1; color:#e65100; padding:3px 6px; border-radius:10px; font-size:11px; font-weight:700; white-space:nowrap;'>⚠️주의</span>"
        else:
            return f"<span style='background:#ffebee; color:#b71c1c; padding:3px 6px; border-radius:10px; font-size:11px; font-weight:700; white-space:nowrap;'>❌비추</span>"

    rows_html = ""
    for t in rows_data:
        tech = t.get("tech", {})
        is_buy = t.get("direction") == "매수"
        name_color = "#1b5e20" if is_buy else "#b71c1c"
        streak = t.get("frgn_streak", 0)
        rows_html += f"""
        <tr>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:13px; font-weight:600; color:{name_color}; white-space:nowrap;">
            <a href="{t['url']}" style="color:{name_color}; text-decoration:none;">{t['corp_name']}</a>
            <span style="font-size:10px; color:#999; margin-left:4px;">{t['date'][5:]}</span>
          </td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_rsi_badge(tech.get('rsi'))}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_ma200_badge(tech.get('ma200_above'), tech.get('ma200_gap_pct'))}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_w52_bar(tech.get('week52_pos'))}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_streak_badge(streak)}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_rs_badge(tech.get('rs_vs_kospi'))}</td>
          <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center;">{_signal_summary(tech, t.get('direction'), streak)}</td>
        </tr>"""

    return f"""
    <div style="padding:24px 16px; border-top:2px solid #e3e8f0;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 4px; font-weight:700;">📊 공시 종목 기술적 신호</h2>
      <p style="font-size:11px; color:#9e9e9e; margin:0 0 16px;">RSI(14) · 200일 이평선 · 52주 위치 · 외국인 연속 · RS vs KOSPI(52주) · 매수 신호 판단</p>
      <p style="font-size:10px; color:#bbb; margin:0 0 12px;">✅ 매수 검토 = 200일↑ + RSI&lt;70 + RS&gt;0 중 3개 이상 충족</p>
      <table style="width:100%; border-collapse:collapse;">
        <thead>
          <tr style="background:#f5f7ff;">
            <th style="padding:6px 8px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">종목</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">RSI</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">200일</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">52주</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">외국인</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">RS</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">신호</th>
          </tr>
        </thead>
        <tbody>{rows_html}</tbody>
      </table>
    </div>"""


def build_html(items, period_label, value_col, trades=None, market_movers=None, market_summary=None):
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
            # 투자자별 5거래일 합산 (외국인/기관/개인) — 각 셀에 표시
            def _fmt_investor_sum(val):
                if val is None:
                    return "<span style='color:#bbb;'>-</span>"
                eok = int(val) // 100
                if eok > 0:
                    return f"<span style='color:#1b5e20; font-weight:700;'>▲{eok:,}</span>"
                if eok < 0:
                    return f"<span style='color:#b71c1c; font-weight:700;'>▼{abs(eok):,}</span>"
                return "<span style='color:#888;'>·</span>"
            fn_text = _fmt_investor_sum(t.get("foreign_net"))
            org_text = _fmt_investor_sum(t.get("org_net"))
            scrt_text = _fmt_investor_sum(t.get("scrt_net"))
            prsn_text = _fmt_investor_sum(t.get("prsn_net"))
            sr = t.get("since_return")
            if sr is None:
                since_text = "<span style='color:#bbb;'>-</span>"
            elif sr > 0.05:
                since_text = f"<span style='color:#1b5e20; font-weight:700;'>▲{sr:+.1f}%</span>"
            elif sr < -0.05:
                since_text = f"<span style='color:#b71c1c; font-weight:700;'>▼{sr:.1f}%</span>"
            else:
                since_text = f"<span style='color:#888;'>{sr:+.1f}%</span>"
            trade_rows += f"""
            <tr>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#555; white-space:nowrap;">{t['date']}</td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:13px; font-weight:600; color:#1a237e; white-space:nowrap;">
                <a href="{t['url']}" style="color:#1a237e; text-decoration:none;">{t['corp_name']}</a>
              </td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; text-align:center; white-space:nowrap;">
                <span style="background:{badge_bg}; color:{badge_color}; font-size:11px; font-weight:700; padding:2px 6px; border-radius:10px;">{badge_text}</span>
              </td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:12px; color:#666; text-align:right; white-space:nowrap;">{ratio_text}</td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:12px; font-weight:600; color:#333; text-align:right; white-space:nowrap;">{amount_text}</td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:12px; text-align:right; white-space:nowrap;">{since_text}</td>
              <td style="padding:6px 8px; border-bottom:1px solid #f0f0f0; font-size:12px; text-align:right; white-space:nowrap;">{fn_text}</td>
            </tr>"""
        dart_section = f"""
    <div style="padding:24px 16px; border-top:2px solid #e3e8f0;">
      <h2 style="font-size:15px; color:#1a237e; margin:0 0 6px; font-weight:700;">최근 30일 국민연금 매수/매도 내역</h2>
      <p style="font-size:11px; color:#9e9e9e; margin:0 0 16px;">출처: DART 주식등의대량보유상황보고서 &nbsp;|&nbsp; 추정금액: 공시일 종가 기준 &nbsp;|&nbsp; 외국인: 최근 5거래일 합산</p>
      <table style="width:100%; border-collapse:collapse;">
        <thead>
          <tr style="background:#f5f7ff;">
            <th style="padding:6px 8px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">공시일</th>
            <th style="padding:6px 8px; text-align:left; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0;">종목</th>
            <th style="padding:6px 8px; text-align:center; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">구분</th>
            <th style="padding:6px 8px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">보유비율</th>
            <th style="padding:6px 8px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">추정금액</th>
            <th style="padding:6px 8px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">공시후</th>
            <th style="padding:6px 8px; text-align:right; font-size:11px; color:#7986cb; font-weight:600; border-bottom:2px solid #e3e8f0; white-space:nowrap;">외국인(5일)</th>
          </tr>
        </thead>
        <tbody>{trade_rows}</tbody>
      </table>
    </div>"""
    else:
        dart_section = """
    <div style="padding:16px 16px; border-top:2px solid #e3e8f0; text-align:center;">
      <p style="font-size:13px; color:#9e9e9e; margin:0;">최근 30일간 국민연금 대량보유 공시 내역이 없습니다.</p>
    </div>"""

    foreign_daily_section = _build_foreign_daily_section(trades)
    market_movers_section = _build_market_movers_section(market_movers)
    technical_section = _build_technical_section(trades)

    # 시장 요약 섹션 (KOSPI / KOSDAQ / USD·KRW)
    def _fmt_chg(chg):
        if chg is None:
            return ""
        color = "#1b5e20" if chg >= 0 else "#b71c1c"
        arrow = "▲" if chg >= 0 else "▼"
        return f"<div style='font-size:13px; color:{color}; font-weight:700;'>{arrow} {chg:+.2f}%</div>"

    if market_summary:
        kospi = market_summary.get("kospi", {})
        kosdaq = market_summary.get("kosdaq", {})
        usdkrw = market_summary.get("usdkrw")
        kospi_ytd = kospi.get("ytd")
        ytd_html = ""
        if kospi_ytd is not None:
            ytd_color = "#1b5e20" if kospi_ytd >= 0 else "#b71c1c"
            ytd_html = f"<div style='font-size:11px; color:{ytd_color}; margin-top:2px;'>YTD {kospi_ytd:+.1f}%</div>"
        market_summary_html = f"""
    <div style="background:#f8f9ff; padding:16px 36px; border-bottom:1px solid #e3e8f0;">
      <div style="display:flex; gap:0; text-align:center;">
        <div style="flex:1; padding:8px 12px; border-right:1px solid #e0e4f0;">
          <div style="font-size:11px; color:#7986cb; font-weight:600; letter-spacing:1px; margin-bottom:4px;">KOSPI</div>
          <div style="font-size:20px; font-weight:800; color:#1a237e;">{kospi.get('close', 0):,.2f}</div>
          {_fmt_chg(kospi.get('chg'))}
          {ytd_html}
        </div>
        <div style="flex:1; padding:8px 12px; border-right:1px solid #e0e4f0;">
          <div style="font-size:11px; color:#7986cb; font-weight:600; letter-spacing:1px; margin-bottom:4px;">KOSDAQ</div>
          <div style="font-size:20px; font-weight:800; color:#1a237e;">{kosdaq.get('close', 0):,.2f}</div>
          {_fmt_chg(kosdaq.get('chg'))}
        </div>
        <div style="flex:1; padding:8px 12px;">
          <div style="font-size:11px; color:#7986cb; font-weight:600; letter-spacing:1px; margin-bottom:4px;">USD/KRW</div>
          <div style="font-size:20px; font-weight:800; color:#1a237e;">{f'{usdkrw:,.1f}' if usdkrw else '-'}</div>
        </div>
      </div>
    </div>"""
    else:
        market_summary_html = ""

    return f"""<!DOCTYPE html>
<html lang="ko">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
</head>
<body style="margin:0; padding:0; background:#eef2f7; font-family:-apple-system,'Apple SD Gothic Neo','Malgun Gothic',sans-serif;">
  <div style="max-width:640px; margin:40px auto; background:#fff; border-radius:16px; overflow:hidden; box-shadow:0 4px 24px rgba(0,0,0,0.10);">

    <div style="background:linear-gradient(135deg, #0d1b4b 0%, #1a237e 60%, #283593 100%); padding:44px 36px 36px; text-align:center;">
      <div style="display:inline-block; background:rgba(255,255,255,0.12); border-radius:8px; padding:4px 14px; font-size:11px; color:#90caf9; letter-spacing:2px; margin-bottom:14px;">DAILY NEWSLETTER</div>
      <h1 style="margin:0 0 10px; color:#fff; font-size:26px; font-weight:800;">국민연금 포트폴리오 현황</h1>
      <p style="margin:0; color:#90caf9; font-size:14px;">{period_label} 기준 &nbsp;|&nbsp; 발송일 {today.strftime('%Y.%m.%d')}</p>
    </div>

    <div style="background:#f0f4ff; padding:24px 16px; border-bottom:1px solid #e3e8f0;">
      <div style="font-size:12px; color:#7986cb; font-weight:600; letter-spacing:1px; margin-bottom:6px;">총 운용자산</div>
      <div style="font-size:36px; font-weight:800; color:#1a237e;">{total_display}</div>
    </div>

    {market_summary_html}

    <div style="padding:24px 16px;">
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

    {technical_section}

    {foreign_daily_section}

    {market_movers_section}

    <div style="background:#f5f7ff; padding:16px 16px; border-top:1px solid #e3e8f0; text-align:center;">
      <p style="margin:0 0 4px; font-size:12px; color:#9e9e9e;">출처: 국민연금공단 &nbsp;|&nbsp; 공공데이터포털 (data.go.kr) &nbsp;|&nbsp; DART 전자공시</p>
      <p style="margin:0; font-size:11px; color:#bdbdbd;">이 메일은 평일 매일 자동 발송됩니다 (휴장일 제외).</p>
    </div>
  </div>
</body>
</html>"""


def send_email(html_content, subject):
    # RECIPIENT_EMAIL은 콤마 구분 다중 주소 지원 (예: "primary@x.com,a@y.com,b@z.com")
    # 첫 번째 주소는 To 헤더에 노출, 나머지는 BCC (헤더 미노출)
    recipients = [e.strip() for e in (RECIPIENT_EMAIL or "").split(",") if e.strip()]
    if not recipients:
        raise RuntimeError("RECIPIENT_EMAIL이 비어있습니다.")

    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"] = GMAIL_USER
    msg["To"] = recipients[0]  # 대표 수신자만 헤더 노출, 친구들은 BCC로 숨김
    msg.attach(MIMEText(html_content, "html", "utf-8"))

    with smtplib.SMTP("smtp.gmail.com", 587) as smtp:
        smtp.ehlo()
        smtp.starttls()
        smtp.login(GMAIL_USER, GMAIL_APP_PASSWORD)
        # 실제 전송 대상은 To + BCC 전부 (sendmail의 to_addrs로 전달)
        smtp.sendmail(GMAIL_USER, recipients, msg.as_string())
    bcc_count = len(recipients) - 1
    extra = f" + BCC {bcc_count}명" if bcc_count > 0 else ""
    print(f"  이메일 전송 완료 → {recipients[0]}{extra} (총 {len(recipients)}명)")


def check_env():
    missing = [k for k in ["NPS_API_KEY", "GMAIL_USER", "GMAIL_APP_PASSWORD", "RECIPIENT_EMAIL", "DART_API_KEY"] if not os.getenv(k)]
    if missing:
        print(f"[오류] .env 파일에 다음 항목이 없습니다: {', '.join(missing)}")
        sys.exit(1)


def main():
    check_env()

    # 휴장일(주말) 발송 생략. 한국 공휴일은 별도 처리 미적용 — 다음 작업으로 분리.
    today = date.today()
    if today.weekday() >= 5:
        print(f"휴장일({today.strftime('%Y-%m-%d %a')}) — 발송 생략")
        return

    print("국민연금 일별 뉴스레터 처리 시작...")

    # 1. 이미 발송된 공시 로드 (신규 공시 강조용)
    sent_rcept_nos = load_sent_rcept_nos()
    print(f"[1/5] 기발송 공시 {len(sent_rcept_nos)}건 로드")

    # 2. 대시보드 캐시 갱신 (90일치, 외국인 일별 포함) — 메일은 이 캐시에서 가져옴
    print("[2/5] 최근 90일 NPS 공시 + 외국인 일별 수집 중...")
    all_trades, all_rcept_nos = fetch_dart_nps_trades(days=90, sent_rcept_nos=None, fetch_prices=True)
    save_trades_cache(all_trades)
    print(f"  → 공시 {len(all_trades)}건 (각 종목 외국인 최근 5거래일 포함)")

    # 3. 신규 공시 산출 (제목 표기·발송 상태 추적용)
    new_rcept_nos = all_rcept_nos - sent_rcept_nos
    n_new = len(new_rcept_nos)

    # 4. 포트폴리오 현황 조회
    print("[3/6] 포트폴리오 현황 조회 중...")
    url, period_label = get_latest_endpoint()
    data = fetch_portfolio_data(url)
    items, value_col = parse_items(data)
    print(f"  {len(items)}개 항목 조회 완료")

    # 5. 시장 전체 매매 동향 (KOSPI/KOSDAQ 시총 상위 60종목 외인/기관/금융투자)
    print("[4/6] 시장 전체 매매 동향 수집 중...")
    market_movers = fetch_market_movers(top_n_per_market=30, ranking_size=5)
    print(f"  movers: {len(market_movers)}개 카테고리")

    # 5.5. 시장 지수 요약 (KOSPI/KOSDAQ 등락률 + USD/KRW)
    print("[4.5/6] 시장 지수 요약 조회 중...")
    market_summary = fetch_market_summary()
    print(f"  kospi={market_summary.get('kospi', {}).get('chg', 'N/A'):.2f}%" if market_summary.get('kospi') else "  시장 요약 없음")

    # 6. 이메일 발송 — 평일은 무조건 발송 (공시 없는 날도 시장 동향 표시)
    print("[5/6] 이메일 발송 중...")
    if n_new > 0:
        subject = f"[국민연금] 신규 공시 {n_new}건 + 시장 매매 동향 - {today.strftime('%Y.%m.%d')}"
    else:
        subject = f"[국민연금] 일별 매매 동향 ({today.strftime('%Y.%m.%d')})"
    html = build_html(items, period_label, value_col, trades=all_trades[:30], market_movers=market_movers, market_summary=market_summary)
    send_email(html, subject)

    # 7. 발송 상태 저장 (신규 공시 rcept_no 합치기)
    save_sent_rcept_nos(sent_rcept_nos | new_rcept_nos)
    print("[6/6] 완료!")


if __name__ == "__main__":
    main()
