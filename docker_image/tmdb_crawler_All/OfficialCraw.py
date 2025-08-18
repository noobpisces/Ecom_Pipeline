from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains

from selenium.webdriver.common.by import By
import time
import threading
import json
import requests
from confluent_kafka import Producer
from webdriver_manager.chrome import ChromeDriverManager

# ---------------------------
# Cấu hình Kafka Producer
# ---------------------------
KAFKA_CONFIG = {"bootstrap.servers": "kafka:9092"}
TOPIC_MOVIES = "tmdb_movies"
TOPIC_CREWS = "tmdb_crews"
TOPIC_KEYWORDS = "tmdb_keywords"

TMDB_API_KEY = "eyJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJmODFmNGU3ODhhZjU0NzVkMzg4ZDIxMzRiMmZlZGE2NiIsIm5iZiI6MTczMTkzNzAwMS4xOTkwMDAxLCJzdWIiOiI2NzNiNDJlOTgzYjY2NmE0ZTlhMmQ3NmMiLCJzY29wZXMiOlsiYXBpX3JlYWQiXSwidmVyc2lvbiI6MX0.Yp5aH_C1KA4iflmeDGQ7JrWi1NfwlKzRRUT20vJs47s"
TMDB_API_URL_MOVIES = "https://api.themoviedb.org/3/movie/{}?language=en-US"
TMDB_API_URL_CREWS = "https://api.themoviedb.org/3/movie/{}/credits?language=en-US"
TMDB_API_URL_KEYWORDS = "https://api.themoviedb.org/3/movie/{}/keywords"
HEADERS = {"accept": "application/json", "Authorization": f"Bearer {TMDB_API_KEY}"}

# ---------------------------
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
service = Service("/usr/bin/chromedriver")  # hoặc dùng ChromeDriverManager nếu bạn cần động

driver = webdriver.Chrome(service=service, options=chrome_options)
driver.get("https://www.themoviedb.org/movie")
time.sleep(2)

# Xử lý cookie nếu có
try:
    cookie_button = driver.find_element(By.XPATH, "//button[contains(text(), 'Accept All Cookies')]")
    cookie_button.click()
except:
    pass

# ---------------------------
# Hàm hỗ trợ
# ---------------------------
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

# def process_movie_ids(movie_ids):
#     """Gọi API lấy thông tin phim song song và gửi vào Kafka"""
#     producer = Producer(KAFKA_CONFIG)
#     threads = []

#     def worker(movie_id):
#         movie_data = fetch_data(movie_id, TMDB_API_URL_MOVIES)
#         crew_data = fetch_data(movie_id, TMDB_API_URL_CREWS)
#         keyword_data = fetch_data(movie_id, TMDB_API_URL_KEYWORDS)
        
#         if movie_data and crew_data and keyword_data:
#             send_to_kafka(producer, movie_data, TOPIC_MOVIES)
#             send_to_kafka(producer, crew_data, TOPIC_CREWS)
#             send_to_kafka(producer, keyword_data, TOPIC_KEYWORDS)
#         else:
#             print(f"Không lấy đủ dữ liệu cho ID {movie_id}")
    
#     for movie_id in movie_ids:
#         t = threading.Thread(target=worker, args=(movie_id,))
#         t.start()
#         threads.append(t)
    
#     for t in threads:
#         t.join()
#     print("🔥 Hoàn tất xử lý API, tiếp tục lấy ID mới...")
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

    # 👇 Thêm dòng này để đảm bảo mọi dữ liệu đều được gửi đi
    producer.flush()

    print("🔥 Hoàn tất xử lý API, tiếp tục lấy ID mới...")

# ---------------------------
# Thu thập ID phim từ Selenium
# ---------------------------
# all_movie_ids = set()
# max_loops = 50  # Số lần lấy dữ liệu
# for _ in range(max_loops):
#     movie_elements = driver.find_elements(By.CLASS_NAME, "card.style_1")
#     new_ids = set()
    
#     for elem in movie_elements:
#         try:
#             movie_id = elem.find_element(By.CLASS_NAME, "options").get_attribute("data-id")
#             if movie_id:
#                 new_ids.add(movie_id)
#         except:
#             pass
    
#     # Nếu có ID mới, xử lý trước khi tiếp tục cuộn trang
#     if new_ids:
#         all_movie_ids.update(new_ids)
#         print(f"🔄 Tìm thấy {len(new_ids)} ID mới, gửi đến API...")
#         process_movie_ids(new_ids)
    
#     # Cuộn xuống cuối trang để lấy thêm dữ liệu
#     driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
#     time.sleep(2)
try:
    load_more_link = driver.find_element(By.CSS_SELECTOR, "a.no_click.load_more[href*='page=2']")
    if load_more_link.is_displayed():
        print("🔘 Đã tìm thấy nút Load More. Đang click lần đầu...")
        ActionChains(driver).move_to_element(load_more_link).click().perform()
        print("🔍 Đã click nút Load mỏe lần đầu!")
        time.sleep(2)  # Đợi trang load thêm nội dung
except Exception as e:
    print(f"❌ Không tìm thấy hoặc không thể click Load More: {e}")
all_movie_ids = set()
max_loops = 20  # Số lần lấy dữ liệu
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
    # 👇 Nếu không có ID mới, dừng loop
    if not new_ids:
        print("🚫 Không còn ID mới, dừng thu thập sớm.")
        break

    print(f"🔄 Tìm thấy {len(new_ids)} ID mới, gửi đến API...")
    process_movie_ids(new_ids)
    all_movie_ids.update(new_ids)  # ✅ Cập nhật sau khi gửi xong
    time.sleep(2)
    # driver.execute_script("window.scrollBy(0, 100000);")
    try:
        load_more_link = driver.find_element(By.CSS_SELECTOR, f"a.no_click.load_more[href*='page={haha}']")
    except:
            break
    if load_more_link.is_displayed():
        print("🔘 Đã tìm thấy nút Load More 2 . Đang click lần đầ   222222222222     u...")
        ActionChains(driver).move_to_element(load_more_link).click().perform()
        print("🔍 Đã click nút Load mỏe lần       222222222222222 đầu!")
        time.sleep(2)  # Đợi trang load thêm nội dung
    haha = haha + 1
    time.sleep(2)
driver.quit()
print("✅ Hoàn thành quá trình thu thập dữ liệu!")
