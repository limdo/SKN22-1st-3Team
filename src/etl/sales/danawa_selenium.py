# src/etl/sales/danawa_selenium.py

from selenium import webdriver
from selenium.webdriver.chrome.options import Options


def get_driver(headless: bool = True) -> webdriver.Chrome:
    """
    팀원이 쓰던 옵션을 함수로 분리.
    chromedriver는 PATH에 있거나, 시스템에 설치되어 있어야 한다.
    """
    options = Options()
    if headless:
        # 최신 크롬에서는 --headless=new 권장, 안되면 --headless로 변경
        options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")

    driver = webdriver.Chrome(options=options)
    return driver
