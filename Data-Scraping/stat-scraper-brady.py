import time
import random
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, WebDriverException
import gspread
from google.oauth2.service_account import Credentials
from gspread_dataframe import set_with_dataframe
from selenium.webdriver.common.desired_capabilities import DesiredCapabilities

# Define the URLs and their corresponding sheet names
STAT_URLS = [
    {
        "url": "https://www.fangraphs.com/leaders/major-league?stats=bat&lg=all&type=14&season=2025&month=0&season1=2025&ind=0&rost=&age=&filter=&players=0&team=0&pageitems=2000000000&pos=np&qual=10",
        "sheet_name": "(B) Pitch Val / 100",
        "parent_div_class_target": "table-scroll"
    },
    {
        "url": "https://www.fangraphs.com/leaders/major-league?stats=bat&lg=all&season=2025&season1=2025&ind=0&rost=&filter=&players=0&team=0&pageitems=2000000000&pos=np&qual=10&type=0&month=13",
        "sheet_name": "(B) Standard vs LHP",
        "parent_div_class_target": "table-scroll"
    },
    {
        "url": "https://www.fangraphs.com/leaders/major-league?stats=bat&lg=all&season=2025&season1=2025&ind=0&rost=&filter=&players=0&team=0&pageitems=2000000000&pos=np&qual=10&type=0&month=14",
        "sheet_name": "(B) Standard vs RHP",
        "parent_div_class_target": "table-scroll"
    },
    {
        "url": "https://www.fangraphs.com/leaders/major-league?stats=pit&lg=all&type=9&season=2025&season1=2025&ind=0&rost=&age=&filter=&players=0&team=0&pageitems=2000000000&pos=all&qual=10&month=0",
        "sheet_name": "(P) Pitch Splits",
        "parent_div_class_target": "table-scroll"
    },
    {
        "url": "https://www.fangraphs.com/leaders/major-league?stats=pit&lg=all&type=13&season=2025&season1=2025&ind=0&rost=&age=&filter=&players=0&team=0&pageitems=2000000000&pos=all&qual=10&month=0",
        "sheet_name": "(P) Pitch Val / 100",
        "parent_div_class_target": "table-scroll"
    },

    # Add more URL and sheet name pairs as needed
]


def setup_driver():
    caps = DesiredCapabilities.CHROME
    caps["goog:loggingPrefs"] = {"performance": "ALL"}

    options = uc.ChromeOptions()
    options.headless = False
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_argument("--disable-gpu")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-dev-shm-usage")
    prefs = {"profile.managed_default_content_settings.images": 2}
    options.add_experimental_option("prefs", prefs)

    driver = uc.Chrome(options=options, desired_capabilities=caps)
    driver.set_page_load_timeout(30)
    time.sleep(2)
    return driver


def safe_get(driver, url, retries=3, wait_time=5):
    for attempt in range(retries):
        try:
            print(f"Navigating to URL (Attempt {attempt + 1}): {url}")
            driver.get(url)
            print("✅ Page loaded successfully.")
            return True
        except (TimeoutException, WebDriverException) as e:
            print(
                f"⚠️ Attempt {attempt + 1} failed: {e}. Retrying in {wait_time} seconds...")
            time.sleep(wait_time)
    print(f"❌ Failed to load page after {retries} attempts: {url}")
    return False


def scroll_to_bottom(driver):
    print("Scrolling to load full page...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    for i in range(3):
        print(f"🔄 Scrolling down. Attempt: {i+1}")
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            print("✅ Reached the bottom of the page.")
            break
        last_height = new_height


def scrape_table(driver, url, parent_div_class_target="table-scroll", debug=False):
    print("\n🔄 Scraping data from:", url)
    if not safe_get(driver, url):
        return [], []

    scroll_to_bottom(driver)

    try:
        print(f"Waiting for div.{parent_div_class_target} to appear...")
        table_container = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located(
                (By.CLASS_NAME, f"{parent_div_class_target}"))
        )
    except:
        print(f"❌ .{parent_div_class_target} container not found.")
        with open("page_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        return [], []
    print(f"✅ Found div.{parent_div_class_target}.")

    try:
        print(f"Finding table inside .{parent_div_class_target}...")
        table = table_container.find_element(By.TAG_NAME, "table")
    except:
        print(f"❌ No <table> inside .{parent_div_class_target}.")
        with open("page_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        return [], []
    print("✅ Found table. Now extracting headers...")

    headers = [th.text.strip()
               for th in table.find_elements(By.XPATH, ".//thead/tr/th")]
    if not headers:
        print("❌ No headers found.")
        with open("scraper_debug.html", "w", encoding="utf-8") as f:
            f.write(table.get_attribute("outerHTML"))
        return [], []
    print(f"✅ Headers found: {headers}")

    print("Now extracting data rows...")
    rows = table.find_elements(By.XPATH, ".//tbody/tr")
    data = []
    i = 0
    numRows = 5 if debug else len(rows)
    for row in rows:
        if i < numRows:  # Limit to first 5 rows if debugging
            i += 1
            cells = row.find_elements(By.TAG_NAME, "td")
            row_data = [c.text.strip() for c in cells]
            if len(row_data) == len(headers):
                data.append(row_data)
        else:
            break

    if not data:
        print("❌ No data rows found.")
        with open("table_debug.html", "w", encoding="utf-8") as f:
            f.write(table.get_attribute("outerHTML"))
        return headers, []
    print(f"✅ Found {len(data)} data rows.")
    return headers, data


def upload_to_google_sheets(df, spreadsheet_name, worksheet_name="Sheet1"):
    print("🔄 Uploading to Google Sheets...")
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(
        "credentials.json", scopes=scope)
    client = gspread.authorize(creds)

    try:
        sheet = client.open(spreadsheet_name)
        print(f"✅ Connected to spreadsheet: {spreadsheet_name}")
    except gspread.SpreadsheetNotFound:
        print(f"❌ Spreadsheet '{spreadsheet_name}' not found.")
        return

    try:
        worksheet = sheet.worksheet(worksheet_name)
        print(f"✅ Connected to worksheet: {worksheet_name}")
    except gspread.WorksheetNotFound:
        print(
            f"⚠️ Worksheet '{worksheet_name}' not found. Creating a new one.")
        worksheet = sheet.add_worksheet(
            title=worksheet_name, rows="1000", cols="40")

    worksheet.clear()
    set_with_dataframe(worksheet, df)
    print(
        f"✅ Upload complete to Google Sheet: {spreadsheet_name} -> {worksheet_name}")


def main():
    driver = setup_driver()
    try:
        for url_data in STAT_URLS:
            url = url_data["url"]
            sheet_name = url_data["sheet_name"]
            parent_div_class_target = url_data["parent_div_class_target"]
            print(f"\nProcessing {sheet_name}...")

            headers, data = scrape_table(
                driver, url, parent_div_class_target, True)
            if not data:
                print(f"⚠️ No data rows found for {sheet_name}. Skipping...")
                continue

            df = pd.DataFrame(data, columns=headers)
            if "Team" in df.columns and "Name" in df.columns:
                df.sort_values(by=["Team", "Name"], inplace=True)

            upload_to_google_sheets(
                df,
                spreadsheet_name="MLB Stats",
                worksheet_name=sheet_name
            )

            time.sleep(random.uniform(2, 4))
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
