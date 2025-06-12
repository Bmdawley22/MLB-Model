
import gspread
from oauth2client.service_account import ServiceAccountCredentials
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from webdriver_manager.chrome import ChromeDriverManager
from bs4 import BeautifulSoup
import pandas as pd
import time

BASE_URL = (
    "https://www.fangraphs.com/leaders/major-league?"
    "stats={stats}&lg=all&type={type_id}&season=2025&month=0"
    "&season1=2025&ind=0&rost=&age=&filter=&players=0&team=0"
    "&pageitems=100&pos=np&qual=10&page={page}"
)

DATA_TYPES = [
    {"name": "batter_pitch_type_splits", "stats": "bat", "type_id": "7"},
    {"name": "pitcher_pitch_type_splits", "stats": "pit", "type_id": "7"},
]


def setup_driver():
    chrome_options = Options()
    # chrome_options.add_argument("--headless=new")
    chrome_options.add_argument("--disable-gpu")
    chrome_options.add_argument("--window-size=1920,1080")
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument(
        "user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64)")
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    return driver


def extract_data(driver, url):
    driver.get(url)
    driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
    time.sleep(3)
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located(
                (By.CSS_SELECTOR, "table.table-stats tbody tr, table#leadersTable tbody tr"))
        )
    except:
        print("Table did not load in time.")
        return []

    soup = BeautifulSoup(driver.page_source, "html.parser")
    table = soup.find(
        "table", class_="table-stats") or soup.find("table", id="leadersTable")
    if not table:
        return []

    headers = [th.text.strip() for th in table.find(
        "thead").find_all("th") if th.text.strip() != "#"]
    rows = table.find("tbody").find_all("tr")
    player_data = []
    for row in rows:
        cells = row.find_all(["td", "th"])
        if len(cells) < len(headers):
            continue
        row_data = {}
        start_idx = 1 if cells[0].text.strip().isdigit() else 0
        for i, header in enumerate(headers):
            idx = start_idx + i
            row_data[header] = cells[idx].text.strip(
            ) if idx < len(cells) else ""
        if row_data.get("Name"):
            player_data.append(row_data)
    return player_data


def paginate_and_scrape(driver, stat_type, stat_id):
    page = 1
    all_data = []
    while True:
        url = BASE_URL.format(stats=stat_type, type_id=stat_id, page=page)
        page_data = extract_data(driver, url)
        if not page_data:
            break
        all_data.extend(page_data)
        print(f"Page {page} complete, total records: {len(all_data)}")
        page += 1
    return all_data


def write_to_google_sheets(sheet_title, sheet_data_dict):
    scope = ["https://spreadsheets.google.com/feeds",
             "https://www.googleapis.com/auth/drive"]
    creds = ServiceAccountCredentials.from_json_keyfile_name(
        "credentials.json", scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(sheet_title)
    except gspread.SpreadsheetNotFound:
        sheet = client.create(sheet_title)

    for tab_name, data in sheet_data_dict.items():
        df = pd.DataFrame(data)
        df = df.sort_values(by=["Team", "Name"]
                            ) if "Team" in df.columns else df
        if tab_name in [ws.title for ws in sheet.worksheets()]:
            worksheet = sheet.worksheet(tab_name)
            sheet.del_worksheet(worksheet)
        worksheet = sheet.add_worksheet(title=tab_name, rows=str(
            len(df) + 10), cols=str(len(df.columns)))
        worksheet.update([df.columns.values.tolist()] + df.values.tolist())


def main():
    driver = setup_driver()
    try:
        output = {}
        for data_type in DATA_TYPES:
            print(f"Scraping {data_type['name']}...")
            all_data = paginate_and_scrape(
                driver, data_type["stats"], data_type["type_id"])
            output[data_type["name"]] = all_data
        write_to_google_sheets("FanGraphs 2025 Stats", output)
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
