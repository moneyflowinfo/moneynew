import flet as ft
import pandas as pd
import FinanceDataReader as fdr
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import os
import threading
import time
import warnings # 경고 메시지 필터링용

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import uvicorn

# Pandas FutureWarning 필터링
warnings.filterwarnings("ignore", category=FutureWarning, module="FinanceDataReader")
warnings.filterwarnings("ignore", category=FutureWarning, message="ChainedAssignmentError")


app = FastAPI()

# CORS 설정 (개발 중에는 모든 오리진을 허용)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# main.py에서 복사한 데이터 처리 로직
# --------------------------------------------------------------------------

US_TICKER_MAP = {
    "AAPL": "애플", "MSFT": "마이크로소프트", "AMZN": "아마존", "NVDA": "엔비디아",
    "GOOGL": "구글A", "GOOG": "구글C", "META": "메타", "TSLA": "테슬라",
    "AVGO": "브로드컴", "AMD": "AMD", "QCOM": "퀄컴", "INTC": "인텔",
    "CSCO": "시스코", "ORCL": "오라클", "CRM": "세일즈포스", "ADBE": "어도비",
    "NFLX": "넷플릭스", "PYPL": "페이팔", "NOW": "서비스나우", "PANW": "팔란티어",
    "JPM": "JP모건", "V": "비자", "MA": "마스터카드", "BRK-B": "버크셔B",
    "BAC": "뱅크오브아메리카", "WFC": "웰스파고", "UNH": "유나이티드헬스",
    "LLY": "일라이릴리", "JNJ": "존슨앤존슨", "MRK": "머크", "ABBV": "앱비",
    "PFE": "화이자", "BMY": "브리스톨마이어스", "KO": "코카콜라",
    "PEP": "펩시코", "PG": "프록터앤갬블", "WMT": "월마트", "COST": "코스트코",
    "HD": "홈디포", "XOM": "엑슨모빌", "CVX": "쉐브론",
    "SCHD": "SCHD(배당ETF)", "DIVO": "DIVO", "JEPQ": "JEPQ", "JEPI": "JEPI",
    "SPY": "SPY(S&P500)", "QQQ": "QQQ(나스닥100)", "QQQM": "QQQM", "VOO": "VOO",
    "SPHD": "SPHD", "O": "리얼티인컴", "MAIN": "메인스트리트", "PLTR": "팔란티어",
    "MCK": "맥케슨", "HSY": "허쉬", "COR": "코어사이언티픽", "CAH": "카디널헬스",
    "TPR": "태퍼웨어", "CI": "씨그나", "CPAY": "코페이먼트", "CMS": "CMS에너지",
    "EBAY": "이베이", "DTE": "DTE에너지", "ICE": "인터컨티넨탈익스체인지",
}

sector_cache = {}

def get_sector(ticker):
    if ticker in sector_cache:
        return sector_cache[ticker]
    try:
        listings = fdr.StockListing('S&P500')
        row = listings[listings['Symbol'] == ticker.replace('-', '.')]
        if not row.empty:
            sector = row['Sector'].iloc[0]
            sector_cache[ticker] = sector
            return sector
    except:
        pass
    sector_cache[ticker] = "기타/ETF"
    return "기타/ETF"

def get_scan_tickers():
    try:
        sp500 = fdr.StockListing('S&P500')
        tickers = sp500['Symbol'].tolist()
        tickers = [t.replace('.', '-') for t in tickers if isinstance(t, str) and len(t) > 0]
    except Exception as e:
        print("S&P500 목록 가져오기 실패:", e)
        tickers = list(US_TICKER_MAP.keys())[:50] # S&P500 실패 시 대체 (상위 50개)

    extra_etfs = list(set(US_TICKER_MAP.keys()) - set(tickers))
    tickers = list(set(tickers + extra_etfs))
    print(f"스캔 대상 종목 수: {len(tickers)}개")
    return sorted(tickers)

def calculate_rsi(series, period=14):
    try:
        delta = series.diff()
        up = delta.clip(lower=0)
        down = -1 * delta.clip(upper=0)
        ema_up = up.ewm(com=period-1, adjust=False).mean()
        ema_down = down.ewm(com=period-1, adjust=False).mean()
        rs = ema_up / (ema_down + 1e-9)
        return 100 - (100 / (1 + rs)).iloc[-1]
    except:
        return 50.0

CACHE_FILE = "market_data_cache.pkl"
data_cache, result_cache = {}, {} # 전역 변수로 데이터 관리

def load_cache():
    global data_cache, result_cache
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'rb') as f:
                data_cache_loaded, result_cache_loaded = pickle.load(f)
            data_cache.update(data_cache_loaded)
            result_cache.update(result_cache_loaded)
            print(f"캐시 로드 완료: {len(data_cache)}개 데이터, {len(result_cache)}개 결과")
            for t, df in data_cache.items():
                try:
                    df.index = pd.to_datetime(df.index)
                except:
                    print(f"{t} 캐시 index 변환 실패")
        except Exception as e:
            print("캐시 로드 실패:", e)
    else:
        print("캐시 파일 없음 → 새로 시작")


def save_cache():
    global data_cache, result_cache
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump((data_cache, result_cache), f)
        print("캐시 저장 완료")
    except Exception as e:
        print("캐시 저장 실패:", e)

def normalize_ticker(ticker):
    return ticker.replace('-', '.')

def fetch_data_with_retry(ticker_orig, start_date):
    """주어진 티커에 대해 여러 변형을 시도하여 데이터를 가져옵니다."""
    variations = [ticker_orig, ticker_orig.replace('-', '.'), ticker_orig.replace('.', '-')]
    if '-' in ticker_orig and len(ticker_orig.split('-')) == 2:
        variations.append(ticker_orig.split('-')[0])
    if '.' in ticker_orig or '-' in ticker_orig:
        variations.append(ticker_orig.replace('-', '').replace('.', ''))

    unique_variations = list(dict.fromkeys(variations))

    for variation in unique_variations:
        try:
            df = fdr.DataReader(variation, start=start_date)
            if not df.empty:
                # print(f"데이터 가져오기 성공: {ticker_orig} (시도한 티커: {variation})")
                return df
        except Exception:
            continue
    # print(f"데이터 로드 실패: {ticker_orig} (모든 변형 시도 실패)")
    return None


def update_latest_price(df, ticker_orig):
    # 최신 가격 업데이트 시, 최근 5일치 데이터를 요청하여 curr/prev 계산의 안정성 확보
    today = datetime.now()
    fetch_start_date = (today - timedelta(days=5)).strftime('%Y-%m-%d')
    try:
        latest_df = fetch_data_with_retry(ticker_orig, start_date=fetch_start_date)
        if latest_df is not None and not latest_df.empty:
            # 기존 df와 최신 데이터를 병합 (겹치는 날짜는 최신 데이터로 업데이트)
            df = pd.concat([df, latest_df.tail(5)]).drop_duplicates(keep='last')
            df = df.sort_index()
    except Exception as e:
        print(f"{ticker_orig} 최신 가격 업데이트 실패: {e}")
    return df

def analyze_ticker(ticker, force_refresh=False):
    global data_cache, result_cache

    print(f"\n--- 분석 시작: {ticker} (force_refresh={force_refresh}) ---")

    df = None
    if not force_refresh and ticker in data_cache: # 강제 새로고침이 아니고, 데이터 캐시에 있으면 사용
        df = data_cache[ticker].copy()
        df.index = pd.to_datetime(df.index, errors='coerce')
        df = update_latest_price(df, ticker) # 항상 최신 가격 업데이트 시도
        print(f"DEBUG: {ticker} - 데이터 캐시에서 로드 후 최신 가격 업데이트. len(df)={len(df) if df is not None else 'N/A'}")
    else: # 강제 새로고침이거나, 캐시에 없는 경우
        start_date = (datetime.now() - timedelta(days=500)).strftime('%Y-%m-%d')
        df = fetch_data_with_retry(ticker, start_date=start_date)
        if df is not None:
            df.index = pd.to_datetime(df.index)
            data_cache[ticker] = df.copy() # 새로 가져온 데이터를 캐시에 저장
            print(f"DEBUG: {ticker} - 데이터 인터넷에서 새로 가져옴. len(df)={len(df)}")
        else:
            print(f"DEBUG: {ticker} - 데이터 가져오기 실패, 분석 건너뛰기.")
            return None

    # 최소 2일치 데이터는 있어야 curr, prev 계산 가능
    if df is None or df.empty or len(df) < 2:
        print(f"DEBUG: {ticker} - df.empty={df.empty if df is not None else 'N/A'}, len(df)={len(df) if df is not None else 'N/A'}. 최소 데이터(2일 미만) 부족으로 분석 건너뛰기.")
        return None

    df['Close'] = df['Close'].ffill()
    df_weekly = df.resample('W').last()

    curr = float(df['Close'].iloc[-1])
    prev = float(df['Close'].iloc[-2])
    change_pct = ((curr - prev) / prev) * 100 if prev != 0 else 0
    print(f"DEBUG: {ticker} - curr={curr:.2f}, prev={prev:.2f}, change_pct={change_pct:.2f}%")


    ma = {}
    ma_vals = [5, 20, 60, 120, 200]
    available_ma_vals = []
    for n in ma_vals:
        if len(df) >= n:
            ma[n] = df['Close'].rolling(window=n).mean().iloc[-1]
            available_ma_vals.append(n)
        else:
            ma[n] = None # 계산 불가 표시
    print(f"DEBUG: {ticker} - 계산된 MA: {ma}")

    prev_ma = {}
    for n in ma_vals:
        if len(df) >= n + 1:
            prev_ma[n] = df['Close'].rolling(window=n).mean().iloc[-2]
        else:
            prev_ma[n] = None
    
    # 분석에 필요한 최소한의 MA가 계산 가능한지 확인 (예: 20일선)
    if 20 not in available_ma_vals:
        print(f"DEBUG: {ticker} - 20일선 MA 계산 불가, 분석 건너뛰기.")
        return None

    vol_mean_20 = df['Volume'].iloc[-21:-1].mean() + 1e-9 if 'Volume' in df.columns else 1.0
    vol_ratio = df['Volume'].iloc[-1] / vol_mean_20 if 'Volume' in df.columns else 1.0
    print(f"DEBUG: {ticker} - vol_ratio={vol_ratio:.2f}")

    rsi_d = calculate_rsi(df['Close'])
    rsi_w = calculate_rsi(df_weekly['Close']) if len(df_weekly) > 10 else 50.0
    print(f"DEBUG: {ticker} - rsi_d={rsi_d:.1f}, rsi_w={rsi_w:.1f}")

    # --- 정배열 조건 검사 ---
    today_align_conditions = []
    yesterday_align_conditions = []
    
    # 5 > 20 > 60 > 120 > 200 이동평균선 순서
    # MA 값이 None이 아닌 경우에만 조건에 포함
    for i in range(len(ma_vals) - 1):
        n1, n2 = ma_vals[i], ma_vals[i+1]
        if ma[n1] is not None and ma[n2] is not None:
            today_align_conditions.append(ma[n1] > ma[n2])
            print(f"DEBUG: {ticker} - today MA({n1})={ma[n1]:.2f} vs MA({n2})={ma[n2]:.2f} -> {ma[n1] > ma[n2]}")
        else:
            today_align_conditions.append(False) # MA 중 하나라도 없으면 조건 불충족
            print(f"DEBUG: {ticker} - today MA({n1}) 또는 MA({n2}) 계산 불가, 조건 불충족.")

        if prev_ma[n1] is not None and prev_ma[n2] is not None:
            yesterday_align_conditions.append(prev_ma[n1] > prev_ma[n2])
            print(f"DEBUG: {ticker} - yesterday MA({n1})={prev_ma[n1]:.2f} vs MA({n2})={prev_ma[n2]:.2f} -> {prev_ma[n1] > prev_ma[n2]}")
        else:
            yesterday_align_conditions.append(False)
            print(f"DEBUG: {ticker} - yesterday MA({n1}) 또는 MA({n2}) 계산 불가, 조건 불충족.")
    
    # 현재가가 20일선 위에 있는 조건
    if ma.get(20) is not None and curr >= ma[20]:
        today_align_conditions.append(True)
        print(f"DEBUG: {ticker} - today curr({curr:.2f}) >= MA(20)({ma[20]:.2f}) -> True")
    else:
        today_align_conditions.append(False)
        print(f"DEBUG: {ticker} - today curr({curr:.2f}) >= MA(20)({ma.get(20, 'N/A'):.2f}) -> False")

    if prev_ma.get(20) is not None and prev >= prev_ma[20]:
        yesterday_align_conditions.append(True)
        print(f"DEBUG: {ticker} - yesterday prev({prev:.2f}) >= MA(20)({prev_ma[20]:.2f}) -> True")
    else:
        yesterday_align_conditions.append(False)
        print(f"DEBUG: {ticker} - yesterday prev({prev:.2f}) >= MA(20)({prev_ma.get(20, 'N/A'):.2f}) -> False")


    today_align = all(today_align_conditions)
    yesterday_align = all(yesterday_align_conditions)

    print(f"DEBUG: {ticker} - today_align_conditions: {today_align_conditions}, today_align: {today_align}")
    print(f"DEBUG: {ticker} - yesterday_align_conditions: {yesterday_align_conditions}, yesterday_align: {yesterday_align}")

    is_perfect_align = today_align
    is_new_entry = today_align and not yesterday_align

    print(f"DEBUG: {ticker} - is_perfect_align: {is_perfect_align}, is_new_entry: {is_new_entry}")


    is_breakout_attempt = False
    breakout_type = ""

    # 정배열이 아닌 경우에만 돌파 시도 검사
    if not is_perfect_align: 
        # 200일선 강한 돌파
        cond1 = (ma.get(200) is not None and prev_ma.get(200) is not None and
                 prev < prev_ma[200] and curr >= ma[200] and
                 vol_ratio >= 1.5 and rsi_d < 75)
        print(f"DEBUG: {ticker} - 돌파 조건1 (200일선 강한 돌파) 검사: {cond1}")
        if cond1:
            is_breakout_attempt = True
            breakout_type = "200일선 강한 돌파"
        
        # 60일선 돌파 + 20선 우위
        cond2 = (not is_breakout_attempt and # 이미 조건1이 만족되면 검사 안함
                 ma.get(60) is not None and prev_ma.get(60) is not None and
                 ma.get(20) is not None and 
                 prev < prev_ma[60] and curr >= ma[60] and
                 vol_ratio >= 1.8 and ma[20] > ma[60])
        print(f"DEBUG: {ticker} - 돌파 조건2 (60일선 돌파 + 20선 우위) 검사: {cond2}")
        if cond2:
            is_breakout_attempt = True
            breakout_type = "60일선 돌파 + 20선 우위"

        # 200일선 접근 (±3%)
        cond3 = (not is_breakout_attempt and # 이미 조건1,2가 만족되면 검사 안함
                 ma.get(200) is not None and 
                 abs(curr - ma[200]) / ma[200] <= 0.03 and
                 vol_ratio >= 1.5 and rsi_d < 75)
        print(f"DEBUG: {ticker} - 돌파 조건3 (200일선 접근 (±3%)) 검사: {cond3}")
        if cond3:
            is_breakout_attempt = True
            breakout_type = "200일선 접근 (±3%)"
    
    print(f"DEBUG: {ticker} - is_breakout_attempt: {is_breakout_attempt}, breakout_type: {breakout_type}")


    if is_perfect_align:
        result = {
            'ticker': ticker, 'name': US_TICKER_MAP.get(ticker, ticker), 'sector': get_sector(ticker),
            'rsi_d': rsi_d, 'rsi_w': rsi_w, 'category': '완벽 정배열',
            'break_msg': '정배열 / 신규 진입' if is_new_entry else '정배열',
            'sort_score': 100 + min(vol_ratio * 3, 30) + (50 if is_new_entry else 0),
            'price': f"${curr:,.2f}", 'raw_price': curr, 'change': f"{change_pct:+.2f}%", 'change_raw': change_pct,
            'rsi_d_str': f"{rsi_d:.1f}", 'rsi_w_str': f"{rsi_w:.1f}", 'vol': f"{vol_ratio:.1f}배", 'vol_raw': vol_ratio,
        }
        print(f"DEBUG: {ticker} - 최종 결과: {result['category']}")
    elif is_breakout_attempt:
        result = {
            'ticker': ticker, 'name': US_TICKER_MAP.get(ticker, ticker), 'sector': get_sector(ticker),
            'rsi_d': rsi_d, 'rsi_w': rsi_w, 'category': '상승 돌파 시도중',
            'break_msg': breakout_type, 'sort_score': 85 + min(vol_ratio * 8, 50),
            'price': f"${curr:,.2f}", 'raw_price': curr, 'change': f"{change_pct:+.2f}%", 'change_raw': change_pct,
            'rsi_d_str': f"{rsi_d:.1f}", 'rsi_w_str': f"{rsi_w:.1f}", 'vol': f"{vol_ratio:.1f}배", 'vol_raw': vol_ratio,
        }
        print(f"DEBUG: {ticker} - 최종 결과: {result['category']} ({result['break_msg']})")
    else:
        print(f"DEBUG: {ticker} - 분석 기준에 부합하는 결과 없음.")
        return None
    
    result_cache[ticker] = result
    return result

# --------------------------------------------------------------------------

@app.on_event("startup")
def startup_event():
    load_cache()

@app.get("/api/scan")
async def scan_stocks(use_cache: bool = True):
    global data_cache, result_cache
    
    if not use_cache:
        print("새로고침 요청: 모든 캐시를 비우고 다시 시작합니다.")
        data_cache.clear()
        result_cache.clear()

    tickers = get_scan_tickers()
    
    results = {'완벽 정배열': [], '상승 돌파 시도중': []}
    
    with ThreadPoolExecutor(max_workers=10) as executor:
        future_to_ticker = {executor.submit(analyze_ticker, t, force_refresh=(not use_cache)): t for t in tickers}
        
        for i, future in enumerate(as_completed(future_to_ticker)):
            try:
                res = future.result()
                if res:
                    if res['category'] not in results:
                        results[res['category']] = []
                    results[res['category']].append(res)
            except Exception as e:
                print(f"종목 처리 중 에러: {e}")
            
            if (i + 1) % 50 == 0:
                print(f"스캔 진행 중: {i+1}/{len(tickers)} 완료...")

            time.sleep(0.1)

    for cat in results:
        results[cat].sort(key=lambda x: x['sort_score'], reverse=True)
    
    save_cache()
    
    total = sum(len(v) for v in results.values())
    print(f"스캔 완료: 정배열 {len(results.get('완벽 정배열', []))}개 / 돌파시도 {len(results.get('상승 돌파 시도중', []))}개 (총 {total}개)")
    
    return results

@app.get("/")
def read_root():
    return {"message": "Stock analysis API is running."}
