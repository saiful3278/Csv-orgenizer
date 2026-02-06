import csv
import random
import re
import time
import string
from urllib.parse import urljoin
from urllib.parse import urlparse, unquote, parse_qs

import requests
from bs4 import BeautifulSoup


BASE_URL = "https://www.cmedistribution.my/product-category/lcd-screen/"
DEFAULT_OUTPUT = "cmedistribution_lcd_products.csv"


def build_session():
    session = requests.Session()
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/121.0.0.0 Safari/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "en-US,en;q=0.9",
    }
    session.headers.update(headers)
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
        session.mount("http://", adapter)
        session.mount("https://", adapter)
    except Exception:
        pass
    return session


def get_soup(session, url):
    resp = session.get(url, timeout=30)
    resp.raise_for_status()
    return BeautifulSoup(resp.text, "html.parser")


def sleep(delay_min=1.0, delay_max=2.0):
    time.sleep(random.uniform(delay_min, delay_max))


def normalize_whitespace(text):
    if not text:
        return ""
    return re.sub(r"\s+", " ", text).strip()


def extract_product_links(soup, base_url):
    links = set()
    for a in soup.select("li.product a.woocommerce-LoopProduct__link, li.product a.woocommerce-LoopProduct-link, ul.products li.product a[href]"):
        href = a.get("href")
        if href and "/product/" in href:
            links.add(href)
    if not links:
        for a in soup.select("a[href]"):
            href = a.get("href")
            if href and "/product/" in href:
                links.add(href)
    return [urljoin(base_url, l) for l in sorted(links)]


def parse_price(soup):
    price_el = soup.select_one("p.price")
    if price_el:
        txt = normalize_whitespace(price_el.get_text(separator=" ", strip=True))
        if txt:
            return txt
    els = soup.select("span.woocommerce-Price-amount")
    if els:
        txt = " ".join(normalize_whitespace(e.get_text(strip=True)) for e in els)
        if txt:
            return txt
    return ""


def parse_title(soup):
    el = soup.select_one("h1.product_title, h1.entry-title, h1")
    return normalize_whitespace(el.get_text(strip=True)) if el else ""


def parse_description(soup):
    candidates = [
        "div.woocommerce-Tabs-panel--description",
        "div#tab-description",
        "div.product div.entry-content",
        "div.woocommerce-product-details__short-description",
        "div.summary.entry-summary",
    ]
    for sel in candidates:
        el = soup.select_one(sel)
        if el:
            txt = normalize_whitespace(el.get_text(separator=" ", strip=True))
            if txt:
                return txt
    return ""


def pick_from_srcset(srcset):
    if not srcset:
        return ""
    parts = [p.strip() for p in srcset.split(",") if p.strip()]
    candidates = []
    for p in parts:
        tokens = p.split()
        url = tokens[0] if tokens else ""
        width = 0
        if len(tokens) > 1 and tokens[1].endswith("w"):
            try:
                width = int(tokens[1][:-1])
            except Exception:
                width = 0
        candidates.append((width, url))
    candidates.sort()
    return candidates[-1][1] if candidates else ""


def unwrap_phastpress(url):
    try:
        if "/wp-content/plugins/phastpress/phast.php/" not in url:
            return None
        p = urlparse(url)
        remainder = p.path.split("/phast.php/", 1)[1]
        parts = remainder.split("/")
        # Drop trailing extension segment if present (e.g., q.jpeg)
        if parts and "." in parts[-1] and len(parts[-1]) <= 10:
            parts = parts[:-1]
        b64 = "".join(parts)
        # Add padding for base64 if missing
        padding = "=" * (-len(b64) % 4)
        import base64
        decoded = base64.urlsafe_b64decode(b64 + padding).decode("utf-8", errors="ignore")
        # decoded looks like: service=images&src=https%3A%2F%2F...&cacheMarker=...&token=...
        # Extract src
        qs = parse_qs(decoded, keep_blank_values=True, strict_parsing=False)
        src = None
        for k, v in qs.items():
            if k.lower() == "src" and v:
                src = v[0]
                break
        if src:
            return unquote(src)
    except Exception:
        return None
    return None


def find_upload_urls_in_html(soup):
    try:
        html = str(soup)
    except Exception:
        html = soup.decode() if hasattr(soup, "decode") else ""
    urls = re.findall(r"https?://[^\"'\s]+/wp-content/uploads/[^\"'\s]+", html, flags=re.IGNORECASE)
    # Preserve order, dedupe
    seen = set()
    out = []
    for u in urls:
        if u not in seen:
            seen.add(u)
            out.append(u)
    return out


def extract_images(soup, product_url):
    urls = set()
    for img in soup.select(".woocommerce-product-gallery img, .images img"):
        attrs = [
            img.get("data-src"),
            img.get("data-lazy-src"),
            img.get("data-original"),
            img.get("data-orig-src"),
            img.get("src"),
        ]
        src = next((a for a in attrs if a), None)
        if not src:
            srcset = img.get("data-srcset") or img.get("srcset")
            src = pick_from_srcset(srcset)
        if not src:
            src = img.get("data-large_image") or img.get("data-zoom-image") or img.get("data-full_image") or img.get("data-image")
        if src:
            urls.add(urljoin(product_url, src))
    for a in soup.select(".woocommerce-product-gallery__image a[href], .images a[href]"):
        href = a.get("href")
        if href:
            urls.add(urljoin(product_url, href))
    for meta in soup.select("meta[property='og:image'], meta[name='twitter:image']"):
        content = meta.get("content")
        if content:
            urls.add(urljoin(product_url, content))
    canonical = set()
    for u in urls:
        absu = urljoin(product_url, u)
        cleaned = unwrap_phastpress(absu)
        canonical.add(cleaned or absu)
    uploads = [u for u in canonical if "/wp-content/uploads/" in u]
    if uploads:
        return sorted(set(uploads))
    non_plugin = [u for u in canonical if "/wp-content/plugins/phastpress/" not in u]
    if non_plugin:
        return sorted(set(non_plugin))
    fallback = find_upload_urls_in_html(soup)
    if fallback:
        return sorted(set(fallback))
    return sorted(set(canonical))


def generate_random_sku():
    """Generates a random SKU like 'SKU-A1B2C3'."""
    suffix = "".join(random.choices(string.ascii_uppercase + string.digits, k=6))
    return f"SKU-{suffix}"


def generate_random_stock():
    """Generates a random stock count between 0 and 100."""
    return random.randint(0, 100)


def extract_product(session, url):
    soup = get_soup(session, url)
    title = parse_title(soup)
    price = parse_price(soup)
    description = parse_description(soup)
    images = extract_images(soup, url)
    return {
        "title": title,
        "price": price,
        "description": description,
        "images": images,
        "url": url,
        "sku": generate_random_sku(),
        "stock": generate_random_stock(),
    }


def fetch_category_page(session, base_url, page):
    if page == 1:
        return get_soup(session, base_url)
    url1 = urljoin(base_url, f"page/{page}/")
    soup = get_soup(session, url1)
    links = extract_product_links(soup, base_url)
    if links:
        return soup
    url2 = f"{base_url}?paged={page}"
    soup2 = get_soup(session, url2)
    return soup2


def scrape(max_pages=10, output_csv=DEFAULT_OUTPUT, delay_min=1.0, delay_max=2.0, max_products_per_page=None):
    session = build_session()
    rows = []
    total_count = 0
    for page in range(1, max_pages + 1):
        try:
            soup = fetch_category_page(session, BASE_URL, page)
        except Exception as e:
            print(f"[page {page}] failed to fetch category page: {e}")
            break
        links = extract_product_links(soup, BASE_URL)
        if not links:
            print(f"[page {page}] no products found, stopping")
            break
        print(f"[page {page}] found {len(links)} product links")
        if isinstance(max_products_per_page, int) and max_products_per_page > 0:
            links = links[:max_products_per_page]
        for idx, link in enumerate(links, 1):
            sleep(delay_min, delay_max)
            try:
                data = extract_product(session, link)
                rows.append([
                    data["title"],
                    data["price"],
                    data["description"],
                    "|".join(data["images"]),
                    data["sku"],
                    data["stock"],
                ])
                total_count += 1
                print(f"  scraped {idx}/{len(links)} on page {page}: {data['title'] or 'untitled'}")
            except Exception as e:
                print(f"  error scraping {link}: {e}")
    with open(output_csv, "w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["title", "price", "description", "images", "sku", "stock"])
        writer.writerows(rows)
    print(f"done. wrote {len(rows)} products to {output_csv}")


def main():
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-pages", type=int, default=10)
    parser.add_argument("--output", type=str, default=DEFAULT_OUTPUT)
    parser.add_argument("--delay-min", type=float, default=1.0)
    parser.add_argument("--delay-max", type=float, default=2.0)
    parser.add_argument("--max-products", type=int, default=None)
    args = parser.parse_args()
    scrape(
        max_pages=args.max_pages,
        output_csv=args.output,
        delay_min=args.delay_min,
        delay_max=args.delay_max,
        max_products_per_page=args.max_products,
    )


if __name__ == "__main__":
    main()
