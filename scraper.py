import json

from httpx import AsyncClient
from numpy import tile
import pandas as pd
from selectolax.parser import HTMLParser
from dataclasses import dataclass
import asyncio
from urllib.parse import urljoin
import sqlite3
import os
from dotenv import load_dotenv
from converter import *

load_dotenv()
limit = asyncio.Semaphore(20)

@dataclass
class GasScootersScraper:
    base_url: str = 'https://www.gasscooters.net'
    source: pd.DataFrame = None
    user_agent: str = 'Chrome/125.0.0.0 Safari/537.36'
    max_retries: int = 3
    timeout: int = 30

    async def fetch(self, product_url):
        headers = {
            'user-agent': self.user_agent,
        }

        async with AsyncClient(headers=headers, timeout=self.timeout) as aclient:
            async with limit:
                try:
                    response = await aclient.get(product_url, follow_redirects=True)
                    print(f"Fetched {product_url} - Status: {response.status_code}")
                    response.raise_for_status()
                    return product_url, response.text
                except Exception as e:
                    print(f"Error fetching {product_url}: {e}")
                    raise

    def read_source(self):
        self.source = pd.read_csv('data/search.csv', encoding='latin1', usecols=['product-url'])

    async def fetch_with_retries(self, product_url):
        for attempt in range(self.max_retries):
            try:
                return await self.fetch(product_url)
            except Exception as e:
                print(f"Attempt {attempt + 1} failed for {product_url}: {e}")
                if attempt == self.max_retries - 1:
                    print(f"Max retries reached for {product_url}. Skipping...")
                    return product_url, None
                await asyncio.sleep(2 ** attempt)

    async def fetch_all(self):
        if self.source is None:
            raise ValueError("Source data is not loaded. Call read_source() first.")

        tasks = []
        item_ids = self.source['product-url'].to_list()
        for item_id in item_ids:
            task = asyncio.create_task(self.fetch_with_retries(item_id))
            tasks.append(task)

        results = await asyncio.gather(*tasks, return_exceptions=True)

        try:
            conn = sqlite3.connect("gasscooters.db")
            curr = conn.cursor()
            curr.execute(
                """
                CREATE TABLE IF NOT EXISTS htmls(
                product_url TEXT PRIMARY KEY,
                html TEXT
                )
                """
            )

            for result in results:
                if isinstance(result, Exception):
                    print(f"Skipping due to exception: {result}")
                    continue
                product_url, content = result
                if content is None:
                    print(f"No data for {product_url} after retries.")
                    continue
                curr.execute(
                    "INSERT OR REPLACE INTO htmls (product_url, html) VALUES(?,?)",
                    (product_url, content)
                )
            conn.commit()
        except Exception as e:
            print(f"Error saving to database: {e}")
        finally:
            conn.close()

    def get_image(self):
        conn = sqlite3.connect("gasscooters.db")
        curr = conn.cursor()
        curr.execute("SELECT product_url, html FROM htmls")
        datas = curr.fetchall()
        products = []
        for data in datas:
            images = []
            tree = HTMLParser(data[1])
            image_elements = []
            main_img_elem = tree.css_first('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(4) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > a')
            secondary_img_elem = tree.css_first('area')
            small_img_elem = tree.css_first('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(4) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(1) > img:nth-child(1)')
            if main_img_elem:
                image_elements.append(main_img_elem)
            if secondary_img_elem:
                image_elements.append(secondary_img_elem)
            if small_img_elem:
                image_elements.append(small_img_elem)
            if image_elements:
                for elem in image_elements:
                    images.append(elem.attributes.get('href'))
                result = ';'.join(images)
            else:
                result = ''
            products.append((data[0], result))
        image_df = pd.DataFrame(columns=['product_url', 'images'], data=products)
        image_df.to_csv('data/images.csv', index=False)

    def get_collection(self):
        conn = sqlite3.connect("gasscooters.db")
        curr = conn.cursor()
        curr.execute("SELECT product_url, html FROM htmls")
        datas = curr.fetchall()
        collections = []
        for data in datas:
            try:
                item_urls = []
                item_names = []
                page_type = ''
                sub_col_urls = []
                sub_col_names = []
                tree = HTMLParser(data[1])
                item_elems_ind = tree.css('font[size="2"] > font[color="#cc0000"]')
                secondary_item_elems_ind = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(5) > tbody:nth-child(1) > tr > td > font:nth-child(1) > b')
                sub_col_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(5) > tbody:nth-child(1) > tr > td > font > a')
                item_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(5) > tbody:nth-child(1) > tr > td > font > a')
                if not item_elems:
                    item_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(5) > tbody:nth-child(1) > tr > td > font > b > a')
                if not item_elems:
                    item_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(6) > tbody:nth-child(1) > tr > td > font > center > b > a')

                # item_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td > table > tbody > tr > td > center > a')
                # if not item_elems:
                #     item_elems = tree.css('body > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(2) > table:nth-child(1) > tbody:nth-child(1) > tr:nth-child(1) > td:nth-child(3) > table:nth-child(5) > tbody:nth-child(1) > tr:nth-child(2) > td > font:nth-child(1) > a')

                if item_elems_ind or secondary_item_elems_ind:
                    page_type = 'collection'
                    for item_elem in item_elems:
                        item_urls.append('http://www.gasscooters.net/' + item_elem.attributes.get('href'))
                        item_names.append(item_elem.text(strip=True))
                    item_urls = ';'.join(item_urls)
                    item_names = ';'.join(item_names)
                    sub_col_urls = ''
                    sub_col_names = ''
                elif not item_elems_ind and sub_col_elems:
                    page_type = 'parent collection'
                    for sub_col_elem in sub_col_elems:
                        sub_col_urls.append('http://www.gasscooters.net/' + sub_col_elem.attributes.get('href'))
                        sub_col_names.append(sub_col_elem.text(strip=True))
                    item_urls = ''
                    item_names = ''
                    sub_col_urls = ';'.join(sub_col_urls)
                    sub_col_names = ';'.join(sub_col_names)
                else:
                    item_urls = ''
                    item_names = ''
                    sub_col_urls = ''
                    sub_col_names = ''
                    page_type = 'item'
            except Exception:
                item_urls = ''
                item_names = ''
                sub_col_urls = ''
                sub_col_names = ''
                page_type = ''
            finally:
                collections.append((data[0], page_type, item_urls, item_names, sub_col_urls, sub_col_names))
        collection_df = pd.DataFrame(columns=['product_url', 'page_type', 'item_urls', 'item_names', 'sub_col_urls', 'sub_col_names'], data=collections)
        collection_df.to_csv('data/collections.csv', index=False)


if __name__ == '__main__':
    gs = GasScootersScraper()
    gs.read_source()
    # asyncio.run(gs.fetch_all())
    # gs.get_image()
    gs.get_collection()