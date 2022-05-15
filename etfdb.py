from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from selenium.webdriver.chrome.options import Options

URL = "https://etfdb.com"

options = webdriver.ChromeOptions()
options.add_argument("--headless")
options.add_argument("--no-sandbox")
options.add_argument("--disable-dev-shm-usage")

service = Service(executable_path=ChromeDriverManager().install())

driver = webdriver.Chrome(service=service, options=options)


def extract_etfs_by_vol():
    symbols = []
    url = URL + "/compare/volume/"
    driver.get(url)
    print("ETFs scrapping...")
    rows = driver.find_elements(
        By.CSS_SELECTOR, "div.fixed-table-body > table > tbody > tr"
    )
    for row in rows:
        symbol = row.find_element(By.CSS_SELECTOR, 'td[data-th="Symbol"] > a').text
        symbols.append(symbol)
    driver.quit()

    return symbols


# if __name__ == "__main__":
