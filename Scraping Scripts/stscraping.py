import requests
import os
from tqdm import tqdm
from urllib.request import urlopen
from bs4 import BeautifulSoup
import pandas as pd
import time

def scrape_st():
  
    df = pd.DataFrame()

    save_url_only = False

    target_site = "https://www.straitstimes.com/"

    # get how much pages are there in straitstimes.com/sitemap.xml
    total_sitemap_pages = len([x for x in BeautifulSoup(urlopen(f"{target_site}sitemap.xml"),'lxml').get_text().split('\n') if "page=" in x])

    # for debugging only
    # total_sitemap_pages = 1

    # save file name
    full_raw_name = "debugging_data.csv"

    start_time = time.time()
    condition =False
    if not(os.path.isfile(full_raw_name)):
        for i in tqdm(range(total_sitemap_pages)):
            soup_text = BeautifulSoup(urlopen(f"{target_site}sitemap.xml?page={str(i+1)}"), 'lxml').get_text()
            soup_url = [x for x in soup_text.split("\n") if "https://www.straitstimes.com/singapore/" in x and len(x) >= len(target_site)+1] # list of urls
            
            if save_url_only:
                df = pd.concat([df, pd.DataFrame(
                    {"url": soup_url}
                )], ignore_index=True)
            else:
                updated_soup_url = []
                soup_title = []
                soup_date = []
                soup_text = []

                for url in tqdm(soup_url, leave=False):
                    try:
                        page = requests.get(url, headers={'user-agent': 'Mozilla/5.0 (X11; Linux i686; rv:10.0) Gecko/20100101 Firefox/10.0'})
                        doc = BeautifulSoup(page.text, 'html.parser')
                        title = list(doc.select_one(".headline.node-title").stripped_strings)[0]
                        date = doc.select_one(".story-changeddate").text.strip()
                        text = doc.select_one(".clearfix.text-formatted.field.field--name-field-paragraph-text.field--type-text-long.field--label-hidden.field__item").text
                        text = ''.join(text.splitlines())

                        updated_soup_url.append(url)
                        soup_title.append(title)
                        soup_date.append(date)
                        soup_text.append(text)
                    except:
                        pass
                    if time.time() - start_time >= 7200:
                        condition = True
                        break
                df = pd.concat([df, pd.DataFrame(
                    {"url": updated_soup_url, "title": soup_title, "date": soup_date, "text": soup_text}
                )], ignore_index=True)
                if condition:
                    break
            
        df.drop_duplicates(inplace=True)
        df.dropna(inplace=True)
        df.to_csv(full_raw_name)
        return df

    else:
        print("Already got the data, proceeding with it..")
