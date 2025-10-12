#!/usr/bin/env python3
"""
mirror_download.py

递归下载目标目录并在本地重建目录结构，支持多线程并发下载。
依赖: requests, beautifulsoup4, tqdm
安装: pip install requests beautifulsoup4 tqdm
"""

import os
import sys
import time
import logging
from urllib.parse import urljoin, urlparse, unquote, urlsplit
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Set
import requests
from bs4 import BeautifulSoup
from tqdm import tqdm

# ---------- 配置 ----------
BASE_URL = "http://cicresearch.ca/IOTDataset/CICEVSE2024%20Dataset/Dataset/"  # 可以修改为其它目录
LOCAL_ROOT = r"C:\Users\ermao\Desktop\postgraduate_innovate_application_2025\CICI"  # 本地保存根目录
WORKERS = 8             # 并发数（线程数），可按需调整
REQUEST_TIMEOUT = 20
RETRY = 3
SLEEP_BETWEEN_REQUESTS = 0.0  # 若需放慢速度以礼貌访问，可设置为 >0，比如 0.5
# ---------- end 配置 ----------

HEADERS = {
    "User-Agent": "Mozilla/5.0 (compatible; mirror_download/1.0; +https://example.org/)",
}

session = requests.Session()
session.headers.update(HEADERS)
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")


def ensure_dir(path: str):
    os.makedirs(path, exist_ok=True)


def is_same_host_and_path(base: str, candidate: str) -> bool:
    """判断 candidate 是否位于 base 所在主机并且路径以 base 的路径为前缀（避免爬出站点）"""
    b = urlsplit(base)
    c = urlsplit(candidate)
    if b.scheme != c.scheme or b.netloc != c.netloc:
        return False
    # require candidate path to start with base path
    return c.path.startswith(b.path)


def normalize_local_path(base_url: str, file_url: str, local_root: str) -> str:
    """把 file_url 映射为本地路径（相对于 local_root），同时解码 URL 编码"""
    # compute relative path by removing base_url's path prefix
    b = urlsplit(base_url)
    f = urlsplit(file_url)
    # relative path portion after base path
    rel = f.path[len(b.path):] if f.path.startswith(b.path) else f.path.lstrip("/")
    rel = unquote(rel)  # decode %20 等
    local_path = os.path.join(local_root, rel.lstrip("/"))
    return local_path


def try_head(url: str):
    for attempt in range(RETRY):
        try:
            r = session.head(url, allow_redirects=True, timeout=REQUEST_TIMEOUT)
            return r
        except Exception as e:
            logging.debug(f"HEAD {url} failed (attempt {attempt+1}): {e}")
            time.sleep(1)
    return None


def download_file(url: str, local_path: str, pbar=None):
    """下载单个文件（支持断点续传检测通过 Content-Length），如果已存在且大小一致则跳过"""
    ensure_dir(os.path.dirname(local_path))
    # check if local file exists and size matches Content-Length
    head = try_head(url)
    content_length = None
    if head is not None and head.status_code == 200:
        content_length = head.headers.get("Content-Length")
    # If local exists and sizes match, skip
    if os.path.exists(local_path) and content_length is not None:
        try:
            local_size = os.path.getsize(local_path)
            if local_size == int(content_length):
                logging.debug(f"Skip (exists & size match): {local_path}")
                if pbar:
                    pbar.update(1)
                return "skipped"
        except Exception:
            pass

    # attempt download with retries
    for attempt in range(RETRY):
        try:
            with session.get(url, stream=True, timeout=REQUEST_TIMEOUT) as r:
                if r.status_code in (200, 206):
                    total = r.headers.get("Content-Length")
                    # write to temp file first
                    tmp_path = local_path + ".part"
                    with open(tmp_path, "wb") as f:
                        for chunk in r.iter_content(chunk_size=1024 * 64):
                            if chunk:
                                f.write(chunk)
                    os.replace(tmp_path, local_path)
                    logging.info(f"Downloaded: {local_path}")
                    if pbar:
                        pbar.update(1)
                    # optional sleep
                    if SLEEP_BETWEEN_REQUESTS:
                        time.sleep(SLEEP_BETWEEN_REQUESTS)
                    return "downloaded"
                else:
                    logging.warning(f"GET {url} returned status {r.status_code}")
        except Exception as e:
            logging.warning(f"Download error {url} (attempt {attempt+1}): {e}")
            time.sleep(1)
    logging.error(f"Failed to download after retries: {url}")
    if pbar:
        pbar.update(1)
    return "failed"


def parse_directory_listing(url: str, base_url: str) -> Set[str]:
    """解析目录页，返回其中所有文件或子目录链接（完整 URL），只保留站内链接"""
    results = set()
    try:
        r = session.get(url, timeout=REQUEST_TIMEOUT)
    except Exception as e:
        logging.error(f"Failed to GET directory {url}: {e}")
        return results

    if r.status_code != 200:
        logging.error(f"Directory GET {url} status {r.status_code}")
        return results

    # Try to parse as HTML
    soup = BeautifulSoup(r.text, "html.parser")
    # find anchor tags
    for a in soup.find_all("a"):
        href = a.get("href")
        if not href:
            continue
        # ignore parent directory links
        if href in ("../", "/"):
            continue
        full = urljoin(url, href)
        # only keep links in the same site and under base_url path
        if is_same_host_and_path(base_url, full):
            results.add(full)
    return results


def collect_all_links(start_url: str) -> Set[str]:
    """广度优先遍历目录，收集所有文件链接（不重复）。返回文件 URL 集合（不包含目录URL结尾/的项）"""
    to_visit = [start_url]
    visited = set()
    file_links = set()

    while to_visit:
        cur = to_visit.pop(0)
        if cur in visited:
            continue
        visited.add(cur)
        logging.info(f"Crawling: {cur}")
        links = parse_directory_listing(cur, start_url)
        for link in links:
            # skip same-page anchors
            if link.endswith("/"):
                # directory -> enqueue
                if link not in visited and link not in to_visit:
                    to_visit.append(link)
            else:
                # Heuristic: treat link as file (not ending with '/')
                file_links.add(link)
        # small polite pause
        if SLEEP_BETWEEN_REQUESTS:
            time.sleep(SLEEP_BETWEEN_REQUESTS)
    return file_links


def main(base_url=BASE_URL, local_root=LOCAL_ROOT, workers=WORKERS):
    ensure_dir(local_root)
    logging.info(f"Start crawling {base_url}")
    file_urls = collect_all_links(base_url)
    logging.info(f"Found {len(file_urls)} files to consider.")

    # prepare download tasks and progress bar
    pbar = tqdm(total=len(file_urls), desc="files", unit="file")
    futures = []
    results_summary = {"downloaded": 0, "skipped": 0, "failed": 0}

    with ThreadPoolExecutor(max_workers=workers) as ex:
        for fu in file_urls:
            lp = normalize_local_path(base_url, fu, local_root)
            futures.append(ex.submit(download_file, fu, lp, pbar))

        for f in as_completed(futures):
            res = f.result()
            if res in results_summary:
                results_summary[res] += 1

    pbar.close()
    logging.info(f"Done. Summary: {results_summary}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Mirror a web directory (basic) with multithreaded downloads.")
    parser.add_argument("--url", "-u", default=BASE_URL, help="Directory URL to mirror")
    parser.add_argument("--out", "-o", default=LOCAL_ROOT, help="Local output root directory")
    parser.add_argument("--workers", "-w", type=int, default=WORKERS, help="Number of concurrent downloads")
    args = parser.parse_args()

    main(base_url=args.url, local_root=args.out, workers=args.workers)
