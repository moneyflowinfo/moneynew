import flet as ft
from pyodide.http import pyfetch
API_BASE_URL = "http://localhost:8001"

# --- 상태 메시지에 따른 우선순위 (정렬용) ---
BREAKOUT_PRIORITY = {
    "200일선 강한 돌파": 3,
    "60일선 돌파 + 20선 우위": 2,
    "200일선 접근 (±3%)": 1,
}

# --- UI 헬퍼 함수 ---

def create_data_table(page, category):
    columns = [
        ft.DataColumn(ft.Text("종목명", weight=ft.FontWeight.BOLD)),
        ft.DataColumn(ft.Text("티커", weight=ft.FontWeight.BOLD)),
        ft.DataColumn(ft.Text("현재가", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.RIGHT)),
        ft.DataColumn(ft.Text("전일대비", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.RIGHT)),
        ft.DataColumn(ft.Text("상태", weight=ft.FontWeight.BOLD)),
        ft.DataColumn(ft.Text("RSI(일)", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
        ft.DataColumn(ft.Text("RSI(주)", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
        ft.DataColumn(ft.Text("거래량", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
        ft.DataColumn(ft.Text("섹터", weight=ft.FontWeight.BOLD, text_align=ft.TextAlign.CENTER)),
    ]
    table = ft.DataTable(
        columns=columns, rows=[], border=ft.Border.all(1, ft.Colors.GREY_300),
        heading_row_color=ft.Colors.GREY_100, heading_row_height=40,
        data_row_max_height=35, column_spacing=15, expand=True,
    )
    # page.session.set(f"table_{category}", table) # Removed old session usage
    # app_state is not directly accessible here, it will be handled when calling create_data_table
    return table

def get_change_color(value):
    return ft.Colors.RED_600 if value > 0 else ft.Colors.BLUE_600 if value < 0 else ft.Colors.BLACK

def get_rsi_color(value):
    if value >= 70: return ft.Colors.ORANGE_700
    if value <= 30: return ft.Colors.INDIGO_700
    return ft.Colors.BLACK

def get_vol_color(value):
    return ft.Colors.RED_900 if value >= 2.0 else ft.Colors.BLACK

def fill_table_data(app_state, page, category, data_list):
    table = app_state[f"table_{category}"]
    if not table: return
    table.rows.clear()
    for item in data_list:
        table.rows.append(ft.DataRow(cells=[
            ft.DataCell(ft.Text(item['name'], size=12)),
            ft.DataCell(ft.Text(item['ticker'], size=12)),
            ft.DataCell(ft.Text(item['price'], text_align=ft.TextAlign.RIGHT, size=12)),
            ft.DataCell(ft.Text(item['change'], color=get_change_color(item['change_raw']), text_align=ft.TextAlign.RIGHT, size=12)),
            ft.DataCell(ft.Text(item.get('break_msg', '—'), size=12)),
            ft.DataCell(ft.Text(item['rsi_d_str'], color=get_rsi_color(item['rsi_d']), text_align=ft.TextAlign.CENTER, size=12)),
            ft.DataCell(ft.Text(item['rsi_w_str'], color=get_rsi_color(item['rsi_w']), text_align=ft.TextAlign.CENTER, size=12)),
            ft.DataCell(ft.Text(item['vol'], color=get_vol_color(item['vol_raw']), text_align=ft.TextAlign.CENTER, size=12)),
            ft.DataCell(ft.Text(item['sector'], text_align=ft.TextAlign.CENTER, size=12)),
        ]))

# --- 데이터 처리 및 필터링 로직 ---

def _rsi_match(v, filter_str):
    if filter_str in ["70↑", "70 이상"]: return v >= 70
    if filter_str == "60~70": return 60 <= v < 70
    if filter_str == "50~60": return 50 <= v < 60
    if filter_str == "40~50": return 40 <= v < 50
    if filter_str in ["30↓", "30 이하"]: return v <= 30
    return True

def _price_match(p, filter_str):
    if filter_str == "< $100": return p < 100
    if filter_str == "$100~200": return 100 <= p < 200
    if filter_str == "$200~300": return 200 <= p < 300
    if filter_str == "$300~500": return 300 <= p < 500
    if filter_str == "$500+": return p >= 500
    return True

# --- 메인 앱 로직 ---

def main(page: ft.Page):
    page.title = "미국 주식 스캐너"
    page.theme_mode = ft.ThemeMode.LIGHT
    page.padding = 20
    page.scroll = ft.ScrollMode.ADAPTIVE
    page.fonts = {"Roboto": "https://fonts.googleapis.com/css2?family=Roboto:wght@400;500;700&display=swap"}
    page.theme = ft.Theme(font_family="Roboto")

    # page.session 대신 app_state 딕셔너리로 상태 관리
    app_state = {
        "initialized": False,
        "all_data": {},
        "is_loading": False,
        "table_perfect": None,
        "table_breakout": None,
        "controls_perfect": {},
        "controls_breakout": {},
    }
    
    # 초기화 로직 (app_state 사용)
    if not app_state["initialized"]:
        app_state["initialized"] = True
        # all_data, is_loading은 이미 초기화됨

    def apply_filters(app_state, e): # e 매개변수가 이벤트 객체일 수도 있고 아닐 수도 있으므로 유연하게 처리
        all_data = {}
        try:
            all_data = app_state["all_data"]
        except KeyError:
            pass # all_data 키가 없으면 빈 딕셔너리로 유지

        # --- 완벽 정배열 필터링 ---
        filtered_p = all_data.get('완벽 정배열', []).copy()
        controls_p = app_state.get("controls_perfect")
        
        if name_text := controls_p["name_search"].value.strip().lower():
            filtered_p = [item for item in filtered_p if name_text in item['name'].lower()]
        if ticker_text := controls_p["ticker_search"].value.strip().lower():
            filtered_p = [item for item in filtered_p if ticker_text in item['ticker'].lower()]
        if controls_p["price_combo"].value != "현재가":
            filtered_p = [item for item in filtered_p if _price_match(item['raw_price'], controls_p["price_combo"].value)]
        if controls_p["change_combo"].value != "전일대비":
            rev = controls_p["change_combo"].value == "상승률 ↑"
            filtered_p.sort(key=lambda x: x['change_raw'], reverse=rev)
        if (status_filter := controls_p["status_combo"].value) != "상태":
            if status_filter == "정배열": filtered_p = [item for item in filtered_p if "신규 진입" not in item.get('break_msg', '')]
            elif status_filter == "신규 진입": filtered_p = [item for item in filtered_p if "신규 진입" in item.get('break_msg', '')]
        if controls_p["rsi_d_combo"].value != "RSI(일)":
            filtered_p = [item for item in filtered_p if _rsi_match(item['rsi_d'], controls_p["rsi_d_combo"].value)]
        if controls_p["rsi_w_combo"].value != "RSI(주)":
            filtered_p = [item for item in filtered_p if _rsi_match(item['rsi_w'], controls_p["rsi_w_combo"].value)]
        if controls_p["sector_combo"].value != "섹터":
            filtered_p = [item for item in filtered_p if item['sector'] == controls_p["sector_combo"].value]
        fill_table_data(app_state, page, 'perfect', filtered_p)
        
        # --- 상승 돌파 시도중 필터링 ---
        filtered_b = all_data.get('상승 돌파 시도중', []).copy()
        controls_b = app_state.get("controls_breakout")

        if controls_b["price_combo"].value != "현재가":
            filtered_b = [item for item in filtered_b if _price_match(item['raw_price'], controls_b["price_combo"].value)]
        if controls_b["change_combo"].value != "전일대비":
            rev = controls_b["change_combo"].value == "상승률 ↑"
            filtered_b.sort(key=lambda x: x['change_raw'], reverse=rev)
        if controls_b["rsi_d_combo"].value != "RSI(일)":
            filtered_b = [item for item in filtered_b if _rsi_match(item['rsi_d'], controls_b["rsi_d_combo"].value)]
        if controls_b["rsi_w_combo"].value != "RSI(주)":
            filtered_b = [item for item in filtered_b if _rsi_match(item['rsi_w'], controls_b["rsi_w_combo"].value)]
        if controls_b["sector_combo"].value != "섹터":
            filtered_b = [item for item in filtered_b if item['sector'] == controls_b["sector_combo"].value]
        fill_table_data(app_state, page, 'breakout', filtered_b)
        
        page.update()
        
    def create_category_view(app_state, category, title, color):
        controls, filter_items = {}, []
        
        if category == 'perfect':
            name_search_field = ft.TextField(hint_text="종목명", width=120, text_size=12)
            name_search_field.on_change = lambda e: apply_filters(app_state, e) # Use lambda to pass app_state
            controls["name_search"] = name_search_field
            ticker_search_field = ft.TextField(hint_text="티커", width=90, text_size=12)
            ticker_search_field.on_change = lambda e: apply_filters(app_state, e)
            controls["ticker_search"] = ticker_search_field
            filter_items.extend([controls["name_search"], controls["ticker_search"]])

        price_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option(o) for o in ["현재가", "< $100", "$100~200", "$200~300", "$300~500", "$500+"]], value="현재가", width=110, text_size=12)
        price_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
        controls["price_combo"] = price_combo_dropdown
        filter_items.append(controls["price_combo"])

        change_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option(o) for o in ["전일대비", "상승률 ↑", "하락률 ↓"]], value="전일대비", width=120, text_size=12)
        change_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
        controls["change_combo"] = change_combo_dropdown
        filter_items.append(controls["change_combo"])

        if category == 'perfect':
            status_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option(o) for o in ["상태", "정배열", "신규 진입"]], value="상태", width=100, text_size=12)
            status_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
            controls["status_combo"] = status_combo_dropdown
            filter_items.append(controls["status_combo"])

        rsi_d_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option(o) for o in ["RSI(일)", "70↑", "60~70", "50~60", "40~50", "30↓"]], value="RSI(일)", width=110, text_size=12)
        rsi_d_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
        controls["rsi_d_combo"] = rsi_d_combo_dropdown
        rsi_w_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option(o) for o in ["RSI(주)", "70↑", "60~70", "50~60", "40~50", "30↓"]], value="RSI(주)", width=110, text_size=12)
        rsi_w_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
        controls["rsi_w_combo"] = rsi_w_combo_dropdown
        sector_combo_dropdown = ft.Dropdown(options=[ft.dropdown.Option("섹터")], value="섹터", width=130, text_size=12)
        sector_combo_dropdown.on_change = lambda e: apply_filters(app_state, e)
        controls["sector_combo"] = sector_combo_dropdown
        filter_items.extend([controls["rsi_d_combo"], controls["rsi_w_combo"], controls["sector_combo"]])

        # Add an "Apply Filters" button
        apply_filters_button = ft.Button("필터 적용", on_click=lambda e: apply_filters(app_state, e))
        filter_items.append(apply_filters_button) # Add button to filter_items

        app_state[f"controls_{category}"] = controls
        
        table_widget = create_data_table(page, category)
        app_state[f"table_{category}"] = table_widget # Store in app_state
        
        return ft.Container(
            ft.Column([
                ft.Text(title, size=20, weight=ft.FontWeight.BOLD, color=color),
                ft.Row(controls=filter_items, wrap=True, spacing=8, run_spacing=8), # wrap=True 다시 추가
                ft.Divider(height=8),
                ft.Column([table_widget], scroll=ft.ScrollMode.ADAPTIVE, expand=True, height=500) # height=500 다시 추가
            ], spacing=12, horizontal_alignment=ft.CrossAxisAlignment.START, alignment=ft.MainAxisAlignment.START, expand=True),
            padding=15, border_radius=10,
            expand=True # 컨테이너 자체도 expand=True
        )

    async def scan_from_api_thread(app_state):
        app_state["is_loading"] = True
        loading_overlay.visible = True
        status_label.value = "데이터를 불러오는 중입니다... 잠시만 기다려주세요."
        page.update()

        try:
            url = f"{API_BASE_URL}/api/scan?use_cache=True"
            response = await pyfetch(url) # timeout은 pyfetch에서 직접 지원하지 않으므로 제거
            if response.status == 200: # HTTP 상태 코드 확인
                results = await response.json()
            else:
                # 오류 응답 처리
                error_text = await response.text()
                raise Exception(f"API 응답 오류: {response.status} - {error_text}")
            
            perfect_list = results.get('완벽 정배열', [])
            breakout_list = results.get('상승 돌파 시도중', [])
            
            perfect_list.sort(key=lambda x: (x.get('break_msg', '') == '정배열 / 신규 진입', x.get('change_raw', 0), x.get('vol_raw', 0)), reverse=True)
            breakout_list.sort(key=lambda x: (BREAKOUT_PRIORITY.get(x.get('break_msg', ''), 0), x.get('change_raw', 0), x.get('vol_raw', 0)), reverse=True)
            
            app_state["all_data"] = {'완벽 정배열': perfect_list, '상승 돌파 시도중': breakout_list}
            
            all_sectors = set()
            for item in perfect_list + breakout_list:
                all_sectors.add(item['sector'])
            sector_options = [ft.dropdown.Option("섹터")] + [ft.dropdown.Option(s) for s in sorted(list(all_sectors))]
            
            app_state["controls_perfect"]["sector_combo"].options = sector_options
            app_state["controls_breakout"]["sector_combo"].options = sector_options
            
            status_label.value = f"로드 완료: 총 {len(perfect_list) + len(breakout_list)}개 종목 발견"
            
        except Exception as e:
            status_label.value = f"오류: API 서버 연결 실패 또는 데이터 처리 중 오류 발생: {type(e).__name__} - {e}"

        app_state["is_loading"] = False
        loading_overlay.visible = False
        
        apply_filters(app_state, None)
        
        page.update()        
    status_label = ft.Text("준비 완료.", size=14)
    header = ft.Row([
        ft.Text("미국 주식 스캐너", size=24, weight=ft.FontWeight.BOLD),
        status_label,
    ], alignment=ft.MainAxisAlignment.SPACE_BETWEEN)
    
    view_perfect = create_category_view(app_state, 'perfect', "✅ 완벽 정배열 리스트", ft.Colors.BLUE_800)
    view_breakout = create_category_view(app_state, 'breakout', "🔥 상승 돌파 시도중 리스트", ft.Colors.AMBER_800)
    
    main_content_area = ft.Row([
        view_perfect,
        view_breakout,
    ], spacing=20, expand=True, vertical_alignment=ft.CrossAxisAlignment.START)
    
    loading_overlay = ft.Container(
        ft.Column([ft.ProgressRing(width=50, height=50, stroke_width=5), ft.Text("데이터 로드 중...", size=18)], 
                   alignment=ft.MainAxisAlignment.CENTER, horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=20),
        alignment=ft.Alignment(0, 0), bgcolor=ft.Colors.with_opacity(0.85, ft.Colors.BLACK), expand=True, visible=False
    )

    page.add(
        ft.Stack([
            ft.Column([
                header,
                main_content_area
            ], expand=True, horizontal_alignment=ft.CrossAxisAlignment.CENTER, alignment=ft.MainAxisAlignment.START),
            loading_overlay
        ])
    )
    
    page.run_task(scan_from_api_thread, app_state)

if __name__ == "__main__":
    ft.run(main)