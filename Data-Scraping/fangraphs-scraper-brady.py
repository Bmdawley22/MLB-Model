import time
import random
import pandas as pd
import undetected_chromedriver as uc
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC

# URL to scrape
URL = "https://www.fangraphs.com/leaders/major-league?stats=bat&lg=all&type=7&season=2025&month=0&season1=2025&ind=0&rost=&age=&filter=&players=0&team=0&pageitems=2000000000&pos=np&qual=10"


def setup_driver():
    options = uc.ChromeOptions()
    options.headless = False  # set to True if you want to run invisibly
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-blink-features=AutomationControlled")
    return uc.Chrome(options=options)


def wait_for_data_rows(driver):
    print("Waiting for player table row text to appear...")
    WebDriverWait(driver, 30).until(
        EC.presence_of_element_located(
            (By.XPATH, "//table//tbody//tr[1]//td[1]"))
    )
    time.sleep(1)


def scroll_to_bottom(driver):
    print("Scrolling to load full page...")
    last_height = driver.execute_script("return document.body.scrollHeight")
    for _ in range(3):  # scroll several times to trigger lazy load
        driver.execute_script(
            "window.scrollTo(0, document.body.scrollHeight);")
        time.sleep(3)
        new_height = driver.execute_script("return document.body.scrollHeight")
        if new_height == last_height:
            break
        last_height = new_height


def scrape_table(driver):
    print("Navigating to FanGraphs page...")
    driver.get(URL)

    scroll_to_bottom(driver)

    try:
        print("Waiting for div.table-scroll to appear...")
        table_container = WebDriverWait(driver, 30).until(
            EC.presence_of_element_located((By.CLASS_NAME, "table-scroll"))
        )
    except:
        print("❌ .table-scroll container not found.")
        with open("page_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        raise RuntimeError("table-scroll container not found.")

    try:
        print("Finding table inside .table-scroll...")
        table = table_container.find_element(By.TAG_NAME, "table")
    except:
        print("❌ No <table> inside .table-scroll.")
        with open("page_debug.html", "w", encoding="utf-8") as f:
            f.write(driver.page_source)
        raise RuntimeError("Expected table not found inside .table-scroll.")

    print("✅ Found table. Now extracting headers...")
    headers = [th.text.strip()
               for th in table.find_elements(By.XPATH, ".//thead/tr/th")]
    if headers:
        print("Headers found:", headers)
    else:
        print("❌ No headers found. Check fangraphs_table_debug.html for clues.")
        with open("fangraphs_table_debug.html", "w", encoding="utf-8") as f:
            f.write(table.get_attribute("outerHTML"))
        raise RuntimeError("No headers found in the table.")
        return [], []

    print("Now extracting data rows...")
    rows = table.find_elements(By.XPATH, ".//tbody/tr")

    data = []
    for row in rows:
        cells = row.find_elements(By.TAG_NAME, "td")
        row_data = [c.text.strip() for c in cells]
        if len(row_data) == len(headers):
            data.append(row_data)

    if data:
        print(f"✅ Found {len(data)} data rows.")
    else:
        print("❌ No data rows found. Check fangraphs_table_debug.html for clues.")
        with open("fangraphs_table_debug.html", "w", encoding="utf-8") as f:
            f.write(table.get_attribute("outerHTML"))
        return headers, []

    return headers, data


def main():
    driver = setup_driver()
    try:
        headers, data = scrape_table(driver)
        if not data:
            print("⚠️ No data rows found. Check fangraphs_table_debug.html for clues.")
            return

        df = pd.DataFrame(data, columns=headers)

        if "Team" in df.columns and "Name" in df.columns:
            df.sort_values(by=["Team", "Name"], inplace=True)

        df.to_csv("fangraphs_pitch_splits_2025.csv", index=False)
        print("✅ Saved to fangraphs_pitch_splits_2025.csv")
    finally:
        driver.quit()


if __name__ == "__main__":
    main()
