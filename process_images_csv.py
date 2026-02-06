import csv
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from urllib.parse import urlparse

import requests


def build_session():
    s = requests.Session()
    s.headers.update(
        {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
            "Accept": "image/avif,image/webp,image/apng,image/*,*/*;q=0.8",
        }
    )
    try:
        from urllib3.util.retry import Retry
        from requests.adapters import HTTPAdapter
        retry = Retry(
            total=3,
            connect=3,
            read=3,
            backoff_factor=0.5,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET", "HEAD"],
        )
        adapter = HTTPAdapter(max_retries=retry)
        s.mount("http://", adapter)
        s.mount("https://", adapter)
    except Exception:
        pass
    return s


def filename_from_url(url):
    p = urlparse(url)
    name = os.path.basename(p.path)
    return name


def raw_link_for(filename):
    return f"https://raw.githubusercontent.com/saiful3278/Csv-orgenizer/main/images/{filename}"


def download_image(session, url, dest_dir):
    fname = filename_from_url(url)
    path = os.path.join(dest_dir, fname)
    if os.path.exists(path):
        return url, True, fname
    try:
        with session.get(url, stream=True, timeout=30) as r:
            r.raise_for_status()
            with open(path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8192):
                    if chunk:
                        f.write(chunk)
        return url, True, fname
    except Exception:
        return url, False, fname


def process_csv(input_path, output_path, images_dir, threads=8, delay=0.0):
    session = build_session()
    os.makedirs(images_dir, exist_ok=True)
    with open(input_path, "r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)
    url_set = set()
    per_row_urls = []
    for row in rows:
        cell = row.get("images", "") or ""
        parts = [p.strip() for p in cell.split("|") if p.strip()]
        per_row_urls.append(parts)
        for u in parts:
            url_set.add(u)
    url_list = list(url_set)
    results = {}
    total = len(url_list)
    success = 0
    fail = 0
    with ThreadPoolExecutor(max_workers=max(1, threads)) as ex:
        futures = {ex.submit(download_image, session, u, images_dir): u for u in url_list}
        for fut in as_completed(futures):
            u, ok, fname = fut.result()
            results[u] = (ok, fname)
            if ok:
                success += 1
            else:
                fail += 1
            if delay and delay > 0:
                time.sleep(delay)
    for i, row in enumerate(rows):
        urls = per_row_urls[i]
        new_urls = []
        for u in urls:
            ok, fname = results.get(u, (False, filename_from_url(u)))
            new_urls.append(raw_link_for(fname) if ok else u)
        row["images"] = "|".join(new_urls)
    with open(output_path, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)
    print(f"total_images={total}")
    print(f"successful={success}")
    print(f"failed={fail}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--input", type=str, default="cme_lcd.csv")
    parser.add_argument("--output", type=str, default="images_updated.csv")
    parser.add_argument("--images-dir", type=str, default="images")
    parser.add_argument("--threads", type=int, default=8)
    parser.add_argument("--delay", type=float, default=0.0)
    args = parser.parse_args()
    process_csv(args.input, args.output, args.images_dir, args.threads, args.delay)


if __name__ == "__main__":
    main()
