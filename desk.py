import sys
import pandas as pd
import FinanceDataReader as fdr
from PyQt6.QtWidgets import (
    QApplication, QMainWindow, QTableWidget, QTableWidgetItem,
    QVBoxLayout, QWidget, QLabel, QHBoxLayout, QPushButton,
    QHeaderView, QMessageBox, QInputDialog, QScrollArea,
    QComboBox, QLineEdit
)
from PyQt6.QtCore import Qt, QThread, pyqtSignal
from PyQt6.QtGui import QColor, QFont
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
import pickle
import os
import schedule
import time
import threading

# 미국 주식 한글 매핑
US_TICKER_MAP = {
    "AAPL": "애플", "MSFT": "마이크로소프트", "AMZN": "아마존", "NVDA": "엔비디아",
    "GOOGL": "구글A", "GOOG": "구글C", "META": "메타", "TSLA": "테슬라",
    "AVGO": "브로드컴", "AMD": "AMD", "QCOM": "퀄컴", "INTC": "인텔",
    "CSCO": "시스코", "ORCL": "오라클", "CRM": "세일즈포스", "ADBE": "어도비",
    "NFLX": "넷플릭스", "PYPL": "페이팔", "NOW": "서비스나우", "PANW": "팔로알토",
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
        tickers = list(US_TICKER_MAP.keys())[:50]

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


# 캐시 파일 경로
CACHE_FILE = "market_data_cache.pkl"


def load_cache():
    if os.path.exists(CACHE_FILE):
        try:
            with open(CACHE_FILE, 'rb') as f:
                data_cache, result_cache = pickle.load(f)
            print(f"캐시 로드 완료: {len(data_cache)}개 데이터, {len(result_cache)}개 결과")
            return data_cache, result_cache
        except Exception as e:
            print("캐시 로드 실패:", e)
            return {}, {}
    print("캐시 파일 없음 → 새로 시작")
    return {}, {}


def save_cache(data_cache, result_cache):
    try:
        with open(CACHE_FILE, 'wb') as f:
            pickle.dump((data_cache, result_cache), f)
        print("캐시 저장 완료")
    except Exception as e:
        print("캐시 저장 실패:", e)


# 1시간마다 데이터 새로고침을 요청하는 백그라운드 스케줄러 스레드
class SchedulerThread(QThread):
    refresh_signal = pyqtSignal()

    def run(self):
        # UI가 먼저 뜨도록 초기 지연
        time.sleep(5)
        
        # 매시간 정각에 실행되도록 설정
        schedule.every().hour.at(":00").do(self.emit_signal)
        
        print("자동 새로고침 스케줄러 시작 (매시간 정각)")

        while True:
            schedule.run_pending()
            time.sleep(1)

    def emit_signal(self):
        print(f"{datetime.now()}: 스케줄러가 새로고침 신호를 보냅니다.")
        self.refresh_signal.emit()


class MarketDataLoader(QThread):
    data_loaded = pyqtSignal(dict, str)

    def __init__(self, tickers, data_cache, result_cache):
        super().__init__()
        self.tickers = tickers
        self.data_cache = data_cache
        self.result_cache = result_cache

    def update_latest_price(self, df):
        today = datetime.now().strftime('%Y-%m-%d')
        try:
            latest = fdr.DataReader(df.index.name, start=today)
            if not latest.empty:
                latest_row = latest.iloc[-1]
                df.loc[today] = latest_row
                df = df.sort_index()
        except:
            pass
        return df

    def analyze_ticker(self, ticker):
        if ticker in self.result_cache:
            return self.result_cache[ticker]

        if ticker in self.data_cache:
            df = self.data_cache[ticker].copy()
            df = self.update_latest_price(df)
        else:
            try:
                df = fdr.DataReader(ticker, start=(datetime.now() - timedelta(days=1500)).strftime('%Y-%m-%d'))
                if df.empty or len(df) < 201:
                    return None
                self.data_cache[ticker] = df.copy()
            except Exception:
                return None

        df['Close'] = df['Close'].ffill()
        df_weekly = df.resample('W').last()

        curr = float(df['Close'].iloc[-1])
        prev = float(df['Close'].iloc[-2])
        change_pct = ((curr - prev) / prev) * 100 if prev != 0 else 0

        ma = {}
        ma_vals = [5, 20, 60, 120, 200]
        for n in ma_vals:
            if len(df) >= n:
                ma[n] = df['Close'].rolling(window=n).mean().iloc[-1]

        prev_ma = {}
        for n in ma_vals:
            if len(df) >= n + 1:
                prev_ma[n] = df['Close'].rolling(window=n).mean().iloc[-2]

        vol_mean_20 = df['Volume'].iloc[-21:-1].mean() + 1e-9 if 'Volume' in df.columns else 1.0
        vol_ratio = df['Volume'].iloc[-1] / vol_mean_20 if 'Volume' in df.columns else 1.0

        rsi_d = calculate_rsi(df['Close'])
        rsi_w = calculate_rsi(df_weekly['Close']) if len(df_weekly) > 10 else 50.0

        today_align = all([
            ma.get(5, 0) > ma.get(20, 0),
            ma.get(20, 0) > ma.get(60, 0),
            ma.get(60, 0) > ma.get(120, 0),
            ma.get(120, 0) > ma.get(200, 0),
            curr >= ma.get(20, 0)
        ]) and all(k in ma for k in [5,20,60,120,200])

        yesterday_align = all([
            prev_ma.get(5, 0) > prev_ma.get(20, 0),
            prev_ma.get(20, 0) > prev_ma.get(60, 0),
            prev_ma.get(60, 0) > prev_ma.get(120, 0),
            prev_ma.get(120, 0) > prev_ma.get(200, 0),
            prev >= prev_ma.get(20, 0)
        ]) and all(k in prev_ma for k in [5,20,60,120,200])

        is_perfect_align = today_align
        is_new_entry = today_align and not yesterday_align

        is_breakout_attempt = False
        breakout_type = ""

        if (200 in prev_ma and prev < prev_ma[200] and curr >= ma[200] and
            vol_ratio >= 1.5 and rsi_d < 75):
            is_breakout_attempt = True
            breakout_type = "200일선 강한 돌파"

        elif (60 in prev_ma and prev < prev_ma[60] and curr >= ma[60] and
              vol_ratio >= 1.8 and 20 in ma and ma[20] > ma[60]):
            is_breakout_attempt = True
            breakout_type = "60일선 돌파 + 20선 우위"

        elif (200 in ma and abs(curr - ma[200]) / ma[200] <= 0.03 and
              curr > prev and vol_ratio >= 1.3 and 20 in ma and ma[20] > ma[60]):
            is_breakout_attempt = True
            breakout_type = "200일선 근처 반등"

        result = {
            'ticker': ticker,
            'name': US_TICKER_MAP.get(ticker, ticker),
            'price': f"${curr:,.2f}",
            'raw_price': curr,
            'change': f"{change_pct:+.2f}%",
            'change_raw': change_pct,
            'rsi_d': rsi_d,
            'rsi_d_str': f"{rsi_d:.1f}",
            'rsi_w': rsi_w,
            'rsi_w_str': f"{rsi_w:.1f}",
            'vol': f"{vol_ratio:.1f}배",
            'vol_raw': vol_ratio,
            'sector': get_sector(ticker),
            'is_new_entry': is_new_entry,
        }

        if is_perfect_align:
            result.update({
                'category': '완벽 정배열',
                'signal': '🔥',
                'break_msg': '정배열 / 신규 진입' if is_new_entry else '정배열',
                'sort_score': 100 + min(vol_ratio * 3, 30) + (50 if is_new_entry else 0),
                'change_raw_for_sort': change_pct,
            })
        elif is_breakout_attempt:
            result.update({
                'category': '상승 돌파 시도중',
                'signal': '🚀',
                'break_msg': breakout_type,
                'sort_score': 85 + min(vol_ratio * 8, 50),
                'change_raw_for_sort': change_pct,
            })
        else:
            return None

        self.result_cache[ticker] = result
        return result

    def run(self):
        results = {'완벽 정배열': [], '상승 돌파 시도중': []}

        with ThreadPoolExecutor(max_workers=8) as executor:  # 12 → 8로 낮춤 (권장)
            futures = [executor.submit(self.analyze_ticker, t) for t in self.tickers]
            for future in as_completed(futures):
                res = future.result()
                if res and 'category' in res:
                    results[res['category']].append(res)

        for cat in results:
            results[cat].sort(key=lambda x: x['sort_score'], reverse=True)

        total = sum(len(v) for v in results.values())
        msg = f"완료: 정배열 {len(results['완벽 정배열'])}개 / 돌파시도 {len(results['상승 돌파 시도중'])}개 (총 {total}개)"
        self.data_loaded.emit(results, msg)


class FinanceScannerApp(QMainWindow):
    def __init__(self):
        super().__init__()
        self.tickers = get_scan_tickers()
        self.all_data = {'완벽 정배열': [], '상승 돌파 시도중': []}

        # 캐시 로드
        self.data_cache, self.result_cache = load_cache()

        self.initUI()
        self.start_scan()
        
        # 자동 새로고침 스케줄러 시작
        self.scheduler = SchedulerThread()
        self.scheduler.refresh_signal.connect(self.start_scan)
        self.scheduler.start()

    def initUI(self):
        self.setWindowTitle("미국 주식 스캐너 - 정배열 & 상승 돌파")
        self.setGeometry(100, 50, 1800, 1050)

        self.setStyleSheet("""
            QMainWindow { background-color: #f8f9fc; }
            QTableWidget { 
                background: white; 
                border: 1px solid #d0d4e0; 
                gridline-color: #e8ecf4; 
                font-family: Malgun Gothic; 
                font-size: 13px; 
            }
            QHeaderView::section { 
                background-color: #e2e8f0; 
                font-weight: bold; 
                padding: 8px; 
                border: 1px solid #cbd5e1; 
                font-size: 13px;
            }
            QComboBox { 
                padding: 4px 6px; 
                font-size: 12px; 
                border: 1px solid #cbd5e1; 
                border-radius: 4px;
                min-height: 26px;
                min-width: 120px;
            }
            QLineEdit { 
                padding: 4px 6px; 
                font-size: 12px; 
                border: 1px solid #cbd5e1; 
                border-radius: 4px;
                min-height: 26px;
            }
            QScrollArea { border: none; }
        """)

        central_widget = QWidget()
        main_layout = QVBoxLayout(central_widget)
        main_layout.setSpacing(10)
        main_layout.setContentsMargins(15, 15, 15, 15)

        header = QLabel("미국 주식 스캐너\n(완벽 정배열 & 상승 돌파 시도)")
        header.setFont(QFont("Malgun Gothic", 20, QFont.Weight.Bold))
        header.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(header)

        btn_layout = QHBoxLayout()
        refresh_btn = QPushButton("새로고침")
        refresh_btn.setFixedHeight(40)
        refresh_btn.clicked.connect(self.start_scan)
        btn_layout.addWidget(refresh_btn)

        add_btn = QPushButton("종목 추가")
        add_btn.setFixedHeight(40)
        add_btn.clicked.connect(self.add_ticker)
        btn_layout.addWidget(add_btn)
        btn_layout.addStretch()
        main_layout.addLayout(btn_layout)

        self.status_label = QLabel(f"준비 완료 (총 {len(self.tickers)}개 종목)")
        self.status_label.setAlignment(Qt.AlignmentFlag.AlignCenter)
        main_layout.addWidget(self.status_label)

        scroll = QScrollArea()
        scroll.setWidgetResizable(True)
        scroll_content = QWidget()
        scroll_layout = QVBoxLayout(scroll_content)
        scroll_layout.setSpacing(20)

        # 완벽 정배열 섹션
        perfect_label = QLabel("완벽 정배열 (5 > 20 > 60 > 120 > 200)")
        perfect_label.setFont(QFont("Malgun Gothic", 16, QFont.Weight.Bold))
        perfect_label.setStyleSheet("color: #1e40af;")
        scroll_layout.addWidget(perfect_label)

        perfect_container = QVBoxLayout()
        perfect_container.setSpacing(0)

        filter_widget_p = QWidget()
        filter_layout_p = QHBoxLayout(filter_widget_p)
        filter_layout_p.setContentsMargins(0, 0, 0, 0)
        filter_layout_p.setSpacing(0)

        self.filters_p = [None] * 9

        self.name_search_p = QLineEdit()
        self.name_search_p.setPlaceholderText("종목명")
        self.name_search_p.textChanged.connect(self.apply_filters)
        self.filters_p[0] = self.name_search_p

        self.ticker_search_p = QLineEdit()
        self.ticker_search_p.setPlaceholderText("티커")
        self.ticker_search_p.textChanged.connect(self.apply_filters)
        self.filters_p[1] = self.ticker_search_p

        self.price_combo_p = QComboBox()
        self.price_combo_p.addItems(["현재가", "< $100", "$100~200", "$200~300", "$300~500", "$500+"])
        self.price_combo_p.setPlaceholderText("현재가")
        self.price_combo_p.setCurrentIndex(-1)
        self.price_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[2] = self.price_combo_p

        self.change_combo_p = QComboBox()
        self.change_combo_p.addItems(["전일대비", "상승률 ↑", "하락률 ↓"])
        self.change_combo_p.setPlaceholderText("전일대비")
        self.change_combo_p.setCurrentIndex(-1)
        self.change_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[3] = self.change_combo_p

        self.status_combo_p = QComboBox()
        self.status_combo_p.addItems(["상태/돌파", "정배열", "신규 진입"])
        self.status_combo_p.setPlaceholderText("상태/돌파")
        self.status_combo_p.setCurrentIndex(-1)
        self.status_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[4] = self.status_combo_p

        self.rsi_d_combo_p = QComboBox()
        self.rsi_d_combo_p.addItems(["RSI(일)", "70↑", "60~70", "50~60", "40~50", "30↓"])
        self.rsi_d_combo_p.setPlaceholderText("RSI(일)")
        self.rsi_d_combo_p.setCurrentIndex(-1)
        self.rsi_d_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[5] = self.rsi_d_combo_p

        self.rsi_w_combo_p = QComboBox()
        self.rsi_w_combo_p.addItems(["RSI(주)", "70↑", "60~70", "50~60", "40~50", "30↓"])
        self.rsi_w_combo_p.setPlaceholderText("RSI(주)")
        self.rsi_w_combo_p.setCurrentIndex(-1)
        self.rsi_w_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[6] = self.rsi_w_combo_p

        self.filters_p[7] = QLabel("")

        self.sector_combo_p = QComboBox()
        self.sector_combo_p.addItem("섹터")
        self.sector_combo_p.setPlaceholderText("섹터")
        self.sector_combo_p.setCurrentIndex(-1)
        self.sector_combo_p.setMinimumWidth(180)
        self.sector_combo_p.currentIndexChanged.connect(self.apply_filters)
        self.filters_p[8] = self.sector_combo_p

        for widget in self.filters_p:
            container = QWidget()
            lay = QHBoxLayout(container)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.setSpacing(0)
            lay.addWidget(widget)
            filter_layout_p.addWidget(container)

        perfect_container.addWidget(filter_widget_p)

        self.table_perfect = QTableWidget()
        self.table_perfect.setColumnCount(10)
        self.table_perfect.setHorizontalHeaderLabels([
            "종목명", "티커", "현재가", "전일대비",
            "상태/돌파", "RSI(일)", "RSI(주)", "거래량", "섹터", ""
        ])
        self.table_perfect.setMinimumHeight(550)
        perfect_container.addWidget(self.table_perfect)

        scroll_layout.addLayout(perfect_container)

        # 상승 돌파 시도중 섹션
        breakout_label = QLabel("상승 돌파 시도중")
        breakout_label.setFont(QFont("Malgun Gothic", 16, QFont.Weight.Bold))
        breakout_label.setStyleSheet("color: #b45309;")
        scroll_layout.addWidget(breakout_label)

        breakout_container = QVBoxLayout()
        breakout_container.setSpacing(0)

        filter_widget_b = QWidget()
        filter_layout_b = QHBoxLayout(filter_widget_b)
        filter_layout_b.setContentsMargins(0, 0, 0, 0)
        filter_layout_b.setSpacing(0)

        self.filters_b = [None] * 9

        self.filters_b[0] = QLabel("")
        self.filters_b[1] = QLabel("")

        self.filters_b[2] = QComboBox()
        self.filters_b[2].addItems(["현재가", "< $100", "$100~200", "$200~300", "$300~500", "$500+"])
        self.filters_b[2].setPlaceholderText("현재가")
        self.filters_b[2].setCurrentIndex(-1)
        self.filters_b[2].currentIndexChanged.connect(self.apply_filters)

        self.filters_b[3] = QComboBox()
        self.filters_b[3].addItems(["전일대비", "상승률 ↑", "하락률↓"])
        self.filters_b[3].setPlaceholderText("전일대비")
        self.filters_b[3].setCurrentIndex(-1)
        self.filters_b[3].currentIndexChanged.connect(self.apply_filters)

        self.filters_b[4] = QLabel("")

        self.filters_b[5] = QComboBox()
        self.filters_b[5].addItems(["RSI(일)", "70↑", "60~70", "50~60", "40~50", "30↓"])
        self.filters_b[5].setPlaceholderText("RSI(일)")
        self.filters_b[5].setCurrentIndex(-1)
        self.filters_b[5].currentIndexChanged.connect(self.apply_filters)

        self.filters_b[6] = QComboBox()
        self.filters_b[6].addItems(["RSI(주)", "70↑", "60~70", "50~60", "40~50", "30↓"])
        self.filters_b[6].setPlaceholderText("RSI(주)")
        self.filters_b[6].setCurrentIndex(-1)
        self.filters_b[6].currentIndexChanged.connect(self.apply_filters)

        self.filters_b[7] = QLabel("")

        self.filters_b[8] = QComboBox()
        self.filters_b[8].addItem("섹터")
        self.filters_b[8].setPlaceholderText("섹터")
        self.filters_b[8].setCurrentIndex(-1)
        self.filters_b[8].setMinimumWidth(180)
        self.filters_b[8].currentIndexChanged.connect(self.apply_filters)

        for widget in self.filters_b:
            container = QWidget()
            lay = QHBoxLayout(container)
            lay.setContentsMargins(0, 0, 0, 0)
            lay.setSpacing(0)
            lay.addWidget(widget)
            filter_layout_b.addWidget(container)

        breakout_container.addWidget(filter_widget_b)

        self.table_breakout = QTableWidget()
        self.table_breakout.setColumnCount(10)
        self.table_breakout.setHorizontalHeaderLabels([
            "종목명", "티커", "현재가", "전일대비",
            "상태/돌파", "RSI(일)", "RSI(주)", "거래량", "섹터", ""
        ])
        self.table_breakout.setMinimumHeight(550)
        breakout_container.addWidget(self.table_breakout)

        scroll_layout.addLayout(breakout_container)

        scroll_layout.addStretch()
        scroll.setWidget(scroll_content)
        main_layout.addWidget(scroll)

        self.setCentralWidget(central_widget)

        # 이벤트 연결
        for combo in [
            self.price_combo_p, self.change_combo_p, self.status_combo_p,
            self.rsi_d_combo_p, self.rsi_w_combo_p, self.sector_combo_p,
            self.filters_b[2], self.filters_b[3], self.filters_b[5],
            self.filters_b[6], self.filters_b[8]
        ]:
            combo.currentIndexChanged.connect(self.apply_filters)

        self.name_search_p.textChanged.connect(self.apply_filters)
        self.ticker_search_p.textChanged.connect(self.apply_filters)

    def start_scan(self):
        self.status_label.setText(f"데이터 수집 중... (총 {len(self.tickers)}개 종목, 캐시 활용)")
        self.loader = MarketDataLoader(self.tickers, self.data_cache, self.result_cache)
        self.loader.data_loaded.connect(self.display_results)
        self.loader.start()

    def display_results(self, results_dict, msg):
        self.status_label.setText(msg)
        self.all_data = results_dict

        # CSV 파일로 내보내기
        self.export_to_csv(self.all_data)

        sectors = set(item['sector'] for cat in results_dict for item in results_dict[cat])
        sector_list = ["전체"] + sorted(sectors)

        self.sector_combo_p.clear()
        self.sector_combo_p.addItems(sector_list)
        self.sector_combo_p.setCurrentIndex(-1)

        self.filters_b[8].clear()
        self.filters_b[8].addItems(sector_list)
        self.filters_b[8].setCurrentIndex(-1)

        self.apply_filters()

        # 스캔 완료 후 캐시 저장
        save_cache(self.data_cache, self.result_cache)

    def export_to_csv(self, all_results):
        try:
            combined_data = all_results.get('완벽 정배열', []) + all_results.get('상승 돌파 시도중', [])
            if not combined_data:
                print("CSV 내보내기: 데이터 없음.")
                return

            df = pd.DataFrame(combined_data)
            
            columns_to_export = {
                'signal': '신호', 'name': '종목명', 'ticker': '티커',
                'price': '현재가', 'change': '전일대비', 'break_msg': '상태/돌파',
                'rsi_d_str': 'RSI(일)', 'rsi_w_str': 'RSI(주)',
                'vol': '거래량', 'sector': '섹터', 'category': '분류'
            }
            # DataFrame에 없는 열은 제외
            df_export = df[[col for col in columns_to_export if col in df.columns]].copy()
            df_export.rename(columns=columns_to_export, inplace=True)

            csv_filename = "market_data_analysis.csv"
            df_export.to_csv(csv_filename, index=False, encoding='utf-8-sig')
            print(f"'{csv_filename}' 파일로 데이터 저장 완료.")

            # Git 자동화 (사용자 요청에 따라 추가)
            try:
                import subprocess
                project_root = os.path.dirname(os.path.abspath(__file__))
                
                # Git add
                add_command = ["git", "add", csv_filename]
                subprocess.run(add_command, cwd=project_root, check=True, capture_output=True, text=True)
                print(f"Git: '{csv_filename}' 파일 추가 완료.")

                # Git commit
                commit_message = f"Update market data analysis CSV - {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}"
                commit_command = ["git", "commit", "-m", commit_message]
                subprocess.run(commit_command, cwd=project_root, check=True, capture_output=True, text=True)
                print(f"Git: 커밋 완료 - '{commit_message}'")

                # 현재 브랜치 이름 가져오기
                branch_proc = subprocess.run(
                    ["git", "rev-parse", "--abbrev-ref", "HEAD"],
                    cwd=project_root, check=True, capture_output=True, text=True
                )
                current_branch = branch_proc.stdout.strip()
                print(f"Git: 현재 브랜치 '{current_branch}' 확인.")

                # Git push
                push_command = ["git", "push", "origin", current_branch]
                subprocess.run(push_command, cwd=project_root, check=True, capture_output=True, text=True)
                print(f"Git: 원격 저장소 ('{current_branch}' 브랜치)로 푸시 완료.")

            except subprocess.CalledProcessError as e:
                print(f"Git 자동화 중 오류 발생: {e}")
                print(f"Git stdout: {e.stdout}")
                print(f"Git stderr: {e.stderr}")
            except Exception as e:
                print(f"Git 자동화 중 예상치 못한 오류 발생: {e}")

        except Exception as e:
            print(f"CSV 파일 저장 중 오류 발생: {e}")

    def apply_filters(self):
        for category, table, filters in [
            ("완벽 정배열", self.table_perfect, self.filters_p),
            ("상승 돌파 시도중", self.table_breakout, self.filters_b)
        ]:
            data = self.all_data.get(category, [])
            filtered = data.copy()

            # 종목명 검색
            if category == "완벽 정배열":
                name_text = filters[0].text().strip().lower() if isinstance(filters[0], QLineEdit) else ""
                if name_text:
                    filtered = [item for item in filtered if name_text in item['name'].lower()]

            # 티커 검색
            if category == "완벽 정배열":
                ticker_text = filters[1].text().strip().lower() if isinstance(filters[1], QLineEdit) else ""
                if ticker_text:
                    filtered = [item for item in filtered if ticker_text in item['ticker'].lower()]

            # 현재가 필터
            price_combo = filters[2]
            price_filter = price_combo.currentText()
            if price_filter and price_filter != "현재가":
                filtered = [item for item in filtered if self._price_match(item['raw_price'], price_filter)]

            # 전일대비 정렬
            change_combo = filters[3]
            change_sort = change_combo.currentText()
            if change_sort and change_sort != "전일대비":
                if change_sort == "상승률 ↑":
                    filtered.sort(key=lambda x: x['change_raw_for_sort'], reverse=True)
                elif change_sort == "하락률 ↓":
                    filtered.sort(key=lambda x: x['change_raw_for_sort'])

            # 상태/돌파
            if category == "완벽 정배열":
                status_combo = filters[4]
                status_filter = status_combo.currentText()
                if status_filter and status_filter != "상태/돌파":
                    if status_filter == "정배열":
                        filtered = [item for item in filtered if not item.get('is_new_entry', False)]
                    elif status_filter == "신규 진입":
                        filtered = [item for item in filtered if item.get('is_new_entry', False)]

            # RSI(일)
            rsi_d_combo = filters[5]
            rsi_d_filter = rsi_d_combo.currentText()
            if rsi_d_filter and rsi_d_filter != "RSI(일)":
                filtered = [item for item in filtered if self._rsi_match(item['rsi_d'], rsi_d_filter)]

            # RSI(주)
            rsi_w_combo = filters[6]
            rsi_w_filter = rsi_w_combo.currentText()
            if rsi_w_filter and rsi_w_filter != "RSI(주)":
                filtered = [item for item in filtered if self._rsi_match(item['rsi_w'], rsi_w_filter)]

            # 섹터
            sector_combo = filters[8]
            sector_filter = sector_combo.currentText()
            if sector_filter and sector_filter != "섹터" and sector_filter != "전체":
                filtered = [item for item in filtered if item['sector'] == sector_filter]

            self._fill_table(table, filtered)

    def _rsi_match(self, v, filter_str):
        if filter_str in ["70↑", "70 이상"]: return v >= 70
        if filter_str == "60~70": return 60 <= v < 70
        if filter_str == "50~60": return 50 <= v < 60
        if filter_str == "40~50": return 40 <= v < 50
        if filter_str in ["30↓", "30 이하"]: return v <= 30
        return True

    def _price_match(self, p, filter_str):
        if filter_str == "< $100": return p < 100
        if filter_str == "$100~200": return 100 <= p < 200
        if filter_str == "$200~300": return 200 <= p < 300
        if filter_str == "$300~500": return 300 <= p < 500
        if filter_str == "$500+": return p >= 500
        return True

    def _fill_table(self, table, data_list):
        table.setRowCount(len(data_list))

        for i, item in enumerate(data_list):
            name = QTableWidgetItem(item['name'])
            name.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(i, 0, name)

            ticker_item = QTableWidgetItem(item['ticker'])
            ticker_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(i, 1, ticker_item)

            price = QTableWidgetItem(item['price'])
            price.setTextAlignment(Qt.AlignmentFlag.AlignRight | Qt.AlignmentFlag.AlignVCenter)
            table.setItem(i, 2, price)

            change = QTableWidgetItem(item['change'])
            change.setTextAlignment(Qt.AlignmentFlag.AlignRight)
            if item['change_raw'] > 0:
                change.setForeground(QColor("#ef4444"))
            elif item['change_raw'] < 0:
                change.setForeground(QColor("#3b82f6"))
            table.setItem(i, 3, change)

            status_text = item.get('break_msg', '—')
            status = QTableWidgetItem(status_text)
            status.setTextAlignment(Qt.AlignmentFlag.AlignLeft | Qt.AlignmentFlag.AlignVCenter)
            if "신규 진입" in status_text:
                status.setForeground(QColor("#7c3aed"))
            elif "정배열" in status_text:
                status.setForeground(QColor("#15803d"))
            elif "돌파" in status_text or "반등" in status_text:
                status.setForeground(QColor("#b45309"))
            status.setFont(QFont("Malgun Gothic", 10, QFont.Weight.Bold))
            table.setItem(i, 4, status)

            rsi_d = QTableWidgetItem(item['rsi_d_str'])
            rsi_d.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            try:
                v = item['rsi_d']
                if v >= 70:
                    rsi_d.setForeground(QColor("#3b82f6"))
                    rsi_d.setFont(QFont("Malgun Gothic", 10, QFont.Weight.Bold))
                elif v <= 30:
                    rsi_d.setForeground(QColor("#ef4444"))
            except:
                pass
            table.setItem(i, 5, rsi_d)

            rsi_w = QTableWidgetItem(item['rsi_w_str'])
            rsi_w.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            try:
                v = item['rsi_w']
                if v >= 70:
                    rsi_w.setForeground(QColor("#3b82f6"))
                    rsi_w.setFont(QFont("Malgun Gothic", 10, QFont.Weight.Bold))
                elif v <= 30:
                    rsi_w.setForeground(QColor("#ef4444"))
            except:
                pass
            table.setItem(i, 6, rsi_w)

            vol = QTableWidgetItem(item['vol'])
            vol.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            if item['vol_raw'] >= 2.0:
                vol.setForeground(QColor("#b91c1c"))
                vol.setFont(QFont("Malgun Gothic", 10, QFont.Weight.Bold))
            table.setItem(i, 7, vol)

            sector_item = QTableWidgetItem(item['sector'])
            sector_item.setTextAlignment(Qt.AlignmentFlag.AlignCenter)
            table.setItem(i, 8, sector_item)

            table.setItem(i, 9, QTableWidgetItem(""))

        table.resizeColumnsToContents()
        table.setColumnWidth(0, 260)   # 종목명
        table.setColumnWidth(1, 100)   # 티커
        table.setColumnWidth(2, 160)   # 현재가
        table.setColumnWidth(3, 110)   # 전일대비
        table.setColumnWidth(4, 320)   # 상태/돌파
        table.setColumnWidth(5, 90)    # RSI(일)
        table.setColumnWidth(6, 90)    # RSI(주)
        table.setColumnWidth(8, 220)   # 섹터

    def add_ticker(self):
        text, ok = QInputDialog.getText(self, "종목 추가", "티커 입력 (쉼표로 여러개 가능):")
        if ok and text.strip():
            new_tickers = [t.strip().upper() for t in text.split(',') if t.strip()]
            added = []
            for t in new_tickers:
                if t not in self.tickers:
                    self.tickers.append(t)
                    added.append(t)
            if added:
                QMessageBox.information(self, "추가 완료", f"추가됨: {', '.join(added)}\n새로고침하세요.")
                self.start_scan()
            else:
                QMessageBox.information(self, "알림", "이미 모두 존재하는 종목입니다.")

    def closeEvent(self, event):
        # 창 닫을 때도 캐시 저장 (안전장치)
        save_cache(self.data_cache, self.result_cache)
        super().closeEvent(event)


if __name__ == "__main__":
    app = QApplication(sys.argv)
    win = FinanceScannerApp()
    win.show()
    sys.exit(app.exec())