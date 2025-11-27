import scrapy
import json
import pytz
from datetime import datetime
from scrapy_playwright.page import PageMethod
from urllib.parse import urljoin, urlparse
import logging

class JsonWriterPipeline:
    def open_spider(self, spider):
        self.file = open('data/products.json', 'w', encoding='utf-8')
        self.file.write('[\n')
        self.first_item = True

    def close_spider(self, spider):
        self.file.write('\n]')
        self.file.close()

    def process_item(self, item, spider):
        if not self.first_item:
            self.file.write(',\n')
        else:
            self.first_item = False
        
        line = json.dumps(dict(item), ensure_ascii=False, indent=2)
        self.file.write(line)
        return item

class PicknPaySpider(scrapy.Spider):
    name = 'picknpay'
    allowed_domains = ['pnp.co.za', 'cdn-prd-02.pnp.co.za']
    
    custom_settings = {
        'DOWNLOAD_DELAY': 10.0,
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': True,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utc_tz = pytz.utc
    
    def within_crawl_window(self):
        utc_now = datetime.now(self.utc_tz)
        hour, minute = utc_now.hour, utc_now.minute
        
        if hour < 4 or hour > 8:
            return False
        if hour == 8 and minute > 45:
            return False
        return True
    
    def start_requests(self):
        if not self.within_crawl_window():
            self.logger.warning("❌ Outside allowed crawling time (04:00-08:45 UTC)")
            return
        
        self.logger.info("✅ Within crawling window, starting scrape...")
        
        # Use the actual URLs from the sitemap you provided
        category_urls = [
            "https://www.pnp.co.za/c/butter-and-margarine44805192",
            "https://www.pnp.co.za/c/carbonated-fruit-juice1492983919",
            "https://www.pnp.co.za/c/tomatoes-and-tomato-puree-577008296",
            "https://www.pnp.co.za/c/baby-hangers-413980973",
            "https://www.pnp.co.za/c/lactose-free-yoghurts652760584",
            "https://www.pnp.co.za/c/plant-based-alternatives244026503",
            "https://www.pnp.co.za/c/tuna128672784",
            "https://www.pnp.co.za/c/frozen-pies-1677821099",
            "https://www.pnp.co.za/c/marvel-super-heroes698769880",
            "https://www.pnp.co.za/c/ladders146898925",
            "https://www.pnp.co.za/c/baby-nursery1526525998",
            "https://www.pnp.co.za/c/sweetcorn-845526167",
            "https://www.pnp.co.za/c/security146898925",
            "https://www.pnp.co.za/c/rotisserie-chicken-and-hot-foods504656053",
            "https://www.pnp.co.za/c/body-wash-and-scrubs-1205886282",
            "https://www.pnp.co.za/c/fruit-577008296",
            "https://www.pnp.co.za/c/avocados964132541",
            "https://www.pnp.co.za/c/refuse-and-bin-bags1641520077",
        ]
        
        for url in category_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_category,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=25000),
                        PageMethod('wait_for_timeout', 4000),  # Extra wait for content
                    ],
                    'playwright_include_page': True,
                    'download_delay': 10.0,
                    'category_name': self.extract_category_name(url),
                },
                errback=self.errback,
            )
    
    async def parse_category(self, response):
        if not self.within_crawl_window():
            self.logger.info("⏰ Crawling window closed, stopping further requests")
            return
        
        self.logger.info(f"📁 Parsing category: {response.meta.get('category_name', 'Unknown')}")
        self.logger.info(f"🔗 URL: {response.url}")
        
        # Wait a bit more for dynamic content
        import asyncio
        await asyncio.sleep(3)
        
        # Extract product elements
        products = response.css('div.product-grid-item')
        self.logger.info(f"🎯 Found {len(products)} product elements")
        
        if len(products) == 0:
            self.logger.warning("❌ No products found with selector 'div.product-grid-item'")
            # Let's try some alternative selectors
            alt_selectors = [
                '[data-cnstrc-item]',
                '.product-item',
                '.product-card',
                '.product-tile'
            ]
            for selector in alt_selectors:
                alt_products = response.css(selector)
                if alt_products:
                    self.logger.info(f"🔍 Found {len(alt_products)} with selector: {selector}")
                    products = alt_products
                    break
        
        product_count = 0
        for index, product in enumerate(products):
            item = self.extract_product_data(product, response, index)
            
            if item and item.get('name') and item.get('price'):
                product_count += 1
                self.logger.info(f"✅ Product {index + 1}: {item['name']} - {item['price']}")
                yield item
            else:
                self.logger.warning(f"⚠️ Skipping incomplete product {index + 1}")
        
        self.logger.info(f"📊 Successfully extracted {product_count} products from this page")
        
        # Handle pagination
        next_page = self.find_next_page(response)
        if next_page and self.within_crawl_window():
            self.logger.info(f"➡️ Following next page: {next_page}")
            yield response.follow(
                next_page,
                callback=self.parse_category,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=20000),
                    ],
                    'download_delay': 10.0,
                    'category_name': response.meta.get('category_name', ''),
                }
            )
        elif next_page:
            self.logger.info("⏰ Skipping pagination due to time constraints")
    
    def extract_product_data(self, product, response, index):
        """Extract product data from the HTML structure"""
        
        # Method 1: Direct from data attributes (most reliable based on your HTML)
        name = product.attrib.get('data-cnstrc-item-name', '')
        price = product.attrib.get('data-cnstrc-item-price', '')
        product_id = product.attrib.get('data-cnstrc-item-id', '')
        
        # Method 2: If data attributes are empty, try CSS selectors
        if not name:
            name = product.css('a.product-grid-item__info-container__name span::text').get()
        if not name:
            name = product.css('a.product-action span::text').get()
        
        if not price:
            price = product.css('.price::text').get()
        if not price:
            price = product.css('.product__price::text').get()
        if not price:
            price = product.css('.cms-price-display .price::text').get()
        
        # Clean the price
        if price:
            price = price.strip()
            if not price.startswith('R'):
                price = f"R{price}"
        
        # Get product URL
        product_url = product.css('a.product-action::attr(href)').get()
        if not product_url:
            product_url = product.css('a.product-grid-item__info-container__name::attr(href)').get()
        if not product_url:
            product_url = product.css('a[aria-label]::attr(href)').get()
        
        if product_url:
            product_url = response.urljoin(product_url)
        
        # Get image URL
        image_url = product.css('img::attr(src)').get()
        
        # Build the item
        item = {
            'name': name.strip() if name else f"Product_{index}",
            'price': price,
            'original_price': product.css('.old::text').get(),
            'product_url': product_url,
            'image_url': image_url,
            'product_id': product_id,
            'category': response.meta.get('category_name', ''),
            'category_url': response.url,
            'scraped_at': datetime.now(self.utc_tz).isoformat(),
            'data_attributes': {
                'item_id': product_id,
                'item_name': product.attrib.get('data-cnstrc-item-name', ''),
                'item_price': product.attrib.get('data-cnstrc-item-price', ''),
                'strategy_id': product.attrib.get('data-cnstrc-strategy-id', ''),
            }
        }
        
        return self.clean_item(item)
    
    def find_next_page(self, response):
        """Find next page URL"""
        next_selectors = [
            '.pagination__next a::attr(href)',
            'a[rel="next"]::attr(href)',
            '.pagination .next a::attr(href)',
            '.page-next a::attr(href)',
            'a:contains("Next")::attr(href)',
            'a:contains("next")::attr(href)',
            '[aria-label="Next"]::attr(href)',
        ]
        
        for selector in next_selectors:
            next_url = response.css(selector).get()
            if next_url:
                return next_url
        return None
    
    def extract_category_name(self, url):
        """Extract category name from URL"""
        # Example: https://www.pnp.co.za/c/butter-and-margarine44805192
        parsed = urlparse(url)
        path = parsed.path
        
        # Remove /c/ prefix and numbers at the end
        if path.startswith('/c/'):
            category_part = path[3:]  # Remove '/c/'
            # Remove trailing numbers
            category_name = ''.join([c for c in category_part if not c.isdigit()])
            # Clean up dashes
            category_name = category_name.rstrip('-')
            category_name = category_name.replace('-', ' ').title()
            return category_name
        return "Unknown Category"
    
    def clean_text(self, text):
        if text:
            cleaned = ' '.join(text.strip().split())
            return cleaned
        return None
    
    def clean_item(self, item):
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = self.clean_text(value)
            elif isinstance(value, dict):
                item[key] = {k: self.clean_text(v) if isinstance(v, str) else v 
                           for k, v in value.items()}
        return item
    
    async def errback(self, failure):
        self.logger.error(f"❌ Request failed: {failure.value}")