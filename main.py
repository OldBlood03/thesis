#! ./venv/bin/python3
import urllib
import time
from sqlalchemy import URL, create_engine
import pandas as pd
import requests
import json
import os


def dblp_get():
    print("requesting data from dblp ... \n")
    page_size = 1000
    max_page = 6000
    og = None
    for page in range(0,max_page,page_size):

        print(f"dblp page: {page} out of {max_page - page_size}\n", flush=True)

        payload = {"q" : "Game+Theory", "format":"json", "h":page_size, "f" : page}
        url = "https://dblp.org/search/publ/api?"
        r = requests.get(url, params=payload)

        print(f"request status: {"ok" if r.status_code == 200 else r.status_code}\n", flush= True)

        sources = r.json()["result"]["hits"]["hit"]
        if og is None:
            og = pd.DataFrame(sources)
        else:
            og = og._append(pd.DataFrame(sources))

    def format_authors(row):
        if (isinstance(row, float)): return None
        if ("author" not in row): return None
        if isinstance(row["author"], list):
            return [author["text"] for author in row["author"]]
        else:
            return [row["author"]["text"]]

    og = og["info"]
    og = pd.DataFrame(list(og))
    og["authors"] = og["authors"].apply(format_authors)
    print("requesting complete\n", flush=True)
    return og


def serp_get():
    page_size = 20
    max_page = 75000
    for page in range(0,max_page,page_size):
        payload = {"q":"Number+Theory+Hidden+Role+Social+Deduction",
                   "engine" : "google_scholar",
                   "format":"json", "num":page_size, 
                   "start" : page, 
                   "api_key" : os.getenv("SERP_API_KEY"),
                   "hl" : "en"}
        url = "https://serpapi.com/search"
        r = requests.get(url, params=payload)
        json.dump(r.json(), f"data/serp/page_{page//page_size}.json")

def arxiv_get():

    base_url = 'http://export.arxiv.org/api/query?';
    search_query = 'all:game+theory'
    start = 0
    total_results = 17000
    results_per_iteration = 100
    wait_time = 3
    og = None

    print ('Searching arXiv for %s\n' % search_query)

    for i in range(start,total_results,results_per_iteration):
        
        print( "Results %i - %i\n" % (i,i+results_per_iteration))
        
        query = 'search_query=%s&start=%i&max_results=%i' % (search_query,
                                                             i,
                                                            results_per_iteration)

        r = urllib.request.urlopen(base_url+query).read()

        if (og is None):
            og = pd.read_xml(r, parser="etree").iloc[7:]
        else:
            og = og._append(pd.read_xml(r, parser="etree").iloc[7:])

        time.sleep(wait_time) #good practice according to api docs
    return og

def zbmath_get():

    def format_df(og):
        og["contributors"] = og["contributors"].apply(lambda x: [y["name"] for y in x["authors"]])
        og["title"] = og["title"].apply(lambda x: x["title"])
        og.drop("biographic_references", axis="columns", inplace = True)
        og.drop("id", axis="columns", inplace = True)
        og["editorial_contributions"] = og["editorial_contributions"].apply(lambda x: [y["text"] for y in x])
        og["document_type"] = og["document_type"].apply(lambda x: x["description"])
        og["languages"] = og["language"].apply(lambda x: x["languages"])
        og.drop("language", axis="columns", inplace = True)
        og.drop("license", axis="columns", inplace = True)
        og.msc = og.msc.apply(lambda x: [y["text"] for y in x]) #msc = Mathematical Subject Classification
        og.source = og.source.apply(lambda x: x["source"]) #source = where the paper is from
        og["links"] = og["links"].apply(lambda x: [y["url"] for y in x])
        og["references"] = og["references"].apply(lambda x: [y["doi"] for y in x])
        og["references"] = og["references"].apply(lambda x: list(filter(lambda y: y is not None, x)))
        return og

    page = 0
    results_per_page = 100
    print("Searching zbMath\n", flush=True)
    og = None
    allowed_retries = 5
    attempt = 0

    while (True):

        try:
            print(f"page: {page}, attempt: {attempt}\n", flush= True)
            base_url = f"https://api.zbmath.org/v1/document/_search?search_string=game%20theory&page={page}&results_per_page={results_per_page}"
            r = requests.get(base_url)
        except Exception as e:
            print(f"ENCOUNTERED ERROR: {e}\nRETRYING\n", flush = True)
            attempt += 1
            continue

        if (r.status_code != 200 or attempt > allowed_retries): 
            return og

        if og is None:
            og = pd.DataFrame(r.json()["result"])
            og = format_df(og)
        else:
            new_og = format_df(pd.DataFrame(r.json()["result"]))
            og = og._append(new_og)
        page += 1


from urllib.parse import urlparse
def extract_doi_abstract(url):
    parsed = urlparse(url)
    hostname = parsed.netloc
    path = parsed.path
    if ("doi" not in hostname):
        return None
    
    restful_api = f"https://api.crossref.org/works/{path}"
    try:
        r = requests.get(restful_api)
        print(json.dumps(r.json()))
        return r.json()["message"]["abstract"]
    except Exception:
        return None

def snowball_from_zbmath(df):
    print("starting to snowball form zbmath\n", flush=True)
    og = None
    formatted_urls = df["links"].str.replace("{", "").str.replace("}", "").str.split(",")
    num_rows = formatted_urls.shape[0]
    for row ,urls in enumerate(formatted_urls):
        print(urls)
        print(f"row: {row} out of: {num_rows}",flush=True)
        for url in urls:
            parsed = urlparse(url)
            hostname = parsed.netloc
            path = parsed.path

            restful_api = f"https://api.crossref.org/works/{path}"

            if ("doi" not in hostname):
                continue
            
            try:
                r = requests.get(restful_api)
                if(og is None):
                    og = pd.DataFrame(r.json()["message"]["reference"])
                else:
                    new_result = og._append(pd.DataFrame(r.json()["message"]["reference"]))
                    og = new_result

            except Exception:
                continue

    return og

def snowball_from_dblp(df):
    print("starting to snowball form dblp\n", flush=True)
    og = None
    nrows = df["ee"].dropna().shape[0]
    for row ,url in enumerate(df["ee"].dropna()):

        print(f"row: {row} out of: {nrows}", flush=True)
        parsed = urlparse(url)
        hostname = parsed.netloc
        path = parsed.path

        restful_api = f"https://api.crossref.org/works/{path}"

        if ("doi" not in hostname):
            continue
        
        try:
            r = requests.get(restful_api)
            if(og is None):
                og = pd.DataFrame(r.json()["message"]["reference"])
            else:
                new_result = og._append(pd.DataFrame(r.json()["message"]["reference"]))
                og = new_result

        except Exception:
            continue

    return og


if __name__ == "__main__":
    dburl = URL.create("postgresql",
                       username="postgres",
                       password="",
                       host="localhost",
                       database="thesis")

    engine = create_engine(dburl)

    #serp_get()
    #arxiv_get().to_sql("arxiv", engine, if_exists = "fail", index=False)
    #dblp_df = dblp_get()
    #dblp_df.to_sql("dblp", engine, if_exists= "fail", index=False)
    #snowball_from_dblp(dblp_df).to_sql("dblp_snowball_depth_1", engine, if_exists = "fail", index=False)
    #zbmath_get().to_sql("zbmath", engine, if_exists = "fail", index=False)
    df = pd.read_sql("zbmath", engine)
    print(df, flush=True)
    snowball_from_zbmath(df).to_sql("zbmath_snowball_depth_1", engine, if_exists = "fail", index=False)
