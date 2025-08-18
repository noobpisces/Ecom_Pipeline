from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
import time
import threading
import json
import requests
from confluent_kafka import Producer
from webdriver_manager.chrome import ChromeDriverManager
from datetime import datetime


from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
print("haaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")
# Lấy ngày hôm nay
from datetime import datetime, timedelta


today = datetime.today()
preday = today - timedelta(days=1)
preday_str = preday.strftime("%m/%d/%Y")
today_str = today.strftime("%m/%d/%Y")
user_input_date1 = preday_str
user_input_date2 = today_str

# ---------------------------
# Cấu hình Kafka Producer
# ---------------------------
import os

# Lấy bootstrap server từ biến môi trường (trong Docker Compose)
KAFKA_CONFIG = {"bootstrap.servers": "kafka:9092"}


TOPIC_MOVIES = "tmdb_movies"
TOPIC_CREWS = "tmdb_crews"
TOPIC_KEYWORDS = "tmdb_keywords"

TMDB_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmODFmNGU3ODhhZjU0NzVkMzg4ZDIxMzRiMmZlZGE2NiIsIm5iZiI6MTczMTkzNzAwMS4xOTkwMDAxLCJzdWIiOiI2NzNiNDJlOTgzYjY2NmE0ZTlhMmQ3NmMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.Yp5aH_C1KA4iflmeDGQ7JrWi1NfwlKzRRUT20vJs47s"
TMDB_API_URL_MOVIES = "https://api.themoviedb.org/3/movie/{}?language=en-US"
TMDB_API_URL_CREWS = "https://api.themoviedb.org/3/movie/{}/credits?language=en-US"
TMDB_API_URL_KEYWORDS = "https://api.themoviedb.org/3/movie/{}/keywords"
HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

# ---------------------------jupyter server list
# Cấu hình Selenium
# ---------------------------


chrome_options = Options()
chrome_options.add_argument("--headless")
chrome_options.add_argument("--window-size=1920x3000")
chrome_options.add_argument("--disable-gpu")  # Đảm bảo sử dụng GPU nếu có
chrome_options.add_argument("--disable-software-rasterizer")  # Tắt phần mềm rasterizer
chrome_options.add_argument("--no-sandbox")  # Cần trong Docker hoặc môi trường không có giao diện người dùng
chrome_options.add_argument("--disable-dev-shm-usage")  # Cần khi chạy trên Docker hoặc môi trường hạn chế
chrome_options.add_argument("--disable-blink-features=AutomationControlled") 
chrome_options.add_argument("user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/90.0.4430.93 Safari/537.36")
chrome_options.binary_location = "/usr/bin/chromium"
service = Service("/usr/bin/chromedriver")  
driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get("https://www.themoviedb.org/movie")
time.sleep(2)

wait = WebDriverWait(driver, 10)
try:
    cookie_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept All Cookies')]")
    cookie_button.click()
except:
    pass

def fetch_data(movie_id, url):
    """Gọi API TMDB để lấy dữ liệu phim"""
    response = requests.get(url.format(movie_id), headers=HEADERS)
    return response.json() if response.status_code == 200 else None

def send_to_kafka(producer, data, topic):
    """Gửi dữ liệu phim vào Kafka"""
    try:
        producer.produce(topic, key=str(data["id"]), value=json.dumps(data))
        producer.poll(0)
        print(f"✅ Gửi phim ID {data['id']} vào Kafka! TOPIC: {topic}")
    except Exception as e:
        print(f"❌ Lỗi khi gửi phim ID {data['id']}: {e}, TOPIC: {topic}")

def process_movie_ids(movie_ids):
    producer = Producer(KAFKA_CONFIG)
    threads = []
    def worker(movie_id):
        movie_data = fetch_data(movie_id, TMDB_API_URL_MOVIES)
        crew_data = fetch_data(movie_id, TMDB_API_URL_CREWS)
        keyword_data = fetch_data(movie_id, TMDB_API_URL_KEYWORDS)
        if movie_data and crew_data and keyword_data:
            send_to_kafka(producer, movie_data, TOPIC_MOVIES)
            send_to_kafka(producer, crew_data, TOPIC_CREWS)
            send_to_kafka(producer, keyword_data, TOPIC_KEYWORDS)
        else:
            print(f"Không lấy đủ dữ liệu cAho ID {movie_id}")
    for movie_id in movie_ids:
        t = threading.Thread(target=worker, args=(movie_id,))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    producer.flush()

    print("🔥 Hoàn tất xử lý API, tiếp tục lấy ID mới...")

actions = ActionChains(driver)

try:
    all_releases_button = wait.until(EC.element_to_be_clickable((By.ID, "all_releases")))
    print("🎯 Click #all_releases")
    all_releases_button.click()
    time.sleep(1)

    actions.send_keys(Keys.TAB).perform()
    time.sleep(0.8)
    for _ in range(6):
        actions.send_keys(Keys.UP).perform()
        time.sleep(0.4)

    release_date_gte_field = wait.until(EC.element_to_be_clickable((By.ID, "release_date_gte")))
    release_date_gte_field.clear()
    release_date_gte_field.send_keys(user_input_date1)
    time.sleep(2)
    release_date_lte_field = wait.until(EC.element_to_be_clickable((By.ID, "release_date_lte")))
    release_date_lte_field.clear()
    release_date_lte_field.send_keys(user_input_date2)

except Exception as e:
    print(f"❌ Lỗi khi thao tác: {e}")
time.sleep(2)
search_button = wait.until(EC.element_to_be_clickable((By.XPATH, "//a[contains(@class, 'load_more') and text()='Search']")))
search_button.click()

try:
    load_more_link = driver.find_element(By.CSS_SELECTOR, "a.no_click.load_more[href*='page=2']")
    if load_more_link.is_displayed():
        ActionChains(driver).move_to_element(load_more_link).click().perform()
        time.sleep(2) 
except Exception as e:
    print(f"❌ Không tìm thấy hoặc không thể click Load More: {e}")
all_movie_ids = set()
max_loops = 10000 
haha = 2
for _ in range(max_loops):
    movie_elements = driver.find_elements(By.CLASS_NAME, "card.style_1")
    new_ids = set()
    for elem in movie_elements:
        try:
            movie_id = elem.find_element(By.CLASS_NAME, "options").get_attribute("data-id")
            if movie_id and movie_id not in all_movie_ids:
                new_ids.add(movie_id)
        except:
            pass
    if not new_ids:
        print("🚫 Không còn ID mới, dừng thu thập sớm.")
        break

    print(f"🔄 Tìm thấy {len(new_ids)} ID mới, gửi đến API...")
    process_movie_ids(new_ids)
    all_movie_ids.update(new_ids)  


    try:
        load_more_link = driver.find_element(By.CSS_SELECTOR, f"a.no_click.load_more[href*='page={haha}']")
    except:
            break
    if load_more_link.is_displayed():
        ActionChains(driver).move_to_element(load_more_link).click().perform()
        time.sleep(2)  
    haha = haha + 1
    time.sleep(2)

driver.quit()
print("✅ Hoàn thành quá trình thu thập dữ liệu!")
