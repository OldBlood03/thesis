#! ./venv/bin/python3
import pandas as pd
from sqlalchemy import URL, create_engine

from selenium import webdriver
from selenium.webdriver.firefox.options import Options
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from multiprocessing.pool import ThreadPool

firefox_options = Options()
Options.binary_location = "/usr/bin/firefox"
firefox_options.add_argument('--headless')
firefox_options.timeouts = {"pageLoad" : 10000, "implicit": 10000}
firefox_options.page_load_strategy = 'eager'

dburl = URL.create("postgresql",
                   username="postgres",
                   password="",
                   host="localhost",
                   database="thesis")

engine = create_engine(dburl)
query = """SELECT DISTINCT "DOI" FROM dblp_snowball_depth_1
            WHERE not is_empty("DOI");
        """
df = pd.read_sql(query,engine)
POOL_SIZE = 7
pool = ThreadPool(POOL_SIZE)
df["html"] = None
nrows = df.shape[0]
frame_size = nrows//POOL_SIZE
start_indexes = [ i for i in range(0, nrows, frame_size)]
end_indexes = [ i for i in range(frame_size, nrows, frame_size)] + [nrows]
windows = list(zip(start_indexes, end_indexes))


def scrape_url (url, browser):
    try:
        browser.get(url)
        wait = WebDriverWait(browser, 10)
        #guarantees that page_source is not from the doi redirect page
        wait.until(EC.url_changes(url)) 
    except Exception as e:
        print(f"EXCEPTION OCCURRED: {e}")
    return browser.page_source


def scraper(frame):
    global df
    urls_processed = 0
    browser = webdriver.Firefox(options=firefox_options)

    frame_start = frame[0]
    frame_end = frame[1]
    size = frame_end - frame_start
    block_size = 1
    starting_blocks = [ i for i in range(frame_start,frame_end,block_size)]
    ending_blocks = [ i for i in range(frame_start + block_size, frame_end, block_size)] + [size]
    blocks = zip(starting_blocks, ending_blocks)

    for start, end in blocks:
        print(f"urls processed in frame {frame}: {urls_processed}", flush=True)
        doi_slice = df.loc[start:end, "DOI"]
        df.loc[start:end, "html"] = ("https://doi.org/" + doi_slice).apply(lambda url: scrape_url(url, browser))
        df.iloc[start:end].to_sql("dblp_snowball_augmented", engine, if_exists = 'append')
        urls_processed += block_size
        df = df.drop(range(start,end))
    browser.Dispose()

for frame in windows:
    print(f"frame: {frame}")
    pool.apply_async(scraper, args = [frame])
pool.close()
pool.join()

print("SCRAPE COMPLETE")
