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
    
    # Custom settings for this spider
    custom_settings = {
        'DOWNLOAD_DELAY': 10.0,
        'CONCURRENT_REQUESTS': 1,
        'ROBOTSTXT_OBEY': True,
    }
    
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.utc_tz = pytz.utc
        self.category_sitemap = "https://cdn-prd-02.pnp.co.za/sys-master/root/hfe/hf0/29407478153246/CATEGORY-en-ZAR-14475868262960441156.xml"
    
    def within_crawl_window(self):
        """Check if current time is within allowed crawling window (04:00-08:45 UTC)"""
        utc_now = datetime.now(self.utc_tz)
        hour, minute = utc_now.hour, utc_now.minute
        
        # 04:00-08:45 UTC window
        if hour < 4 or hour > 8:
            return False
        if hour == 8 and minute > 45:
            return False
        return True
    
    def start_requests(self):
        if not self.within_crawl_window():
            self.logger.warning("❌ Outside allowed crawling time (04:00-08:45 UTC / 06:00-10:45 SAST)")
            self.logger.info("⏰ Current UTC time: %s", datetime.now(self.utc_tz).strftime('%H:%M'))
            return
        
        self.logger.info("✅ Within crawling window, starting scrape...")
        
        # Start with category sitemap or direct category URLs
        category_urls = [
            "https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products/Food-Beverages/c/food-beverages",
            "https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products/Fruit-Vegetables/c/fruit-vegetables", 
            "https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products/Bakery/c/bakery",
            "https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products/Dairy-Eggs-Fridge/c/dairy-eggs-fridge",
            "https://www.pnp.co.za/pnpstorefront/pnp/en/All-Products/Meat-Fish-Poultry/c/meat-fish-poultry",
        ]
        
        for url in category_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse_category,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=15000),
                        PageMethod('wait_for_timeout', 2000),  # Additional wait for dynamic content
                    ],
                    'playwright_include_page': True,
                    'download_delay': 10.0,
                    'category_name': self.extract_category_name(url),
                },
                errback=self.errback,
            )
    
    async def parse_category(self, response):
        """Parse category page for products"""
        if not self.within_crawl_window():
            self.logger.info("⏰ Crawling window closed, stopping further requests")
            return
        
        self.logger.info(f"📁 Parsing category: {response.meta.get('category_name', 'Unknown')}")
        self.logger.info(f"🔗 URL: {response.url}")
        
        # Extract product elements using the specific selector you provided
        products = response.css('div.product-grid-item')
        self.logger.info(f"🎯 Found {len(products)} products on page")
        
        for index, product in enumerate(products):
            item = {
                'name': self.clean_text(product.css('a.product-action span::text').get() or 
                                      product.css('a.product-grid-item__info-container__name span::text').get() or
                                      product.attrib.get('data-cnstrc-item-name', '')),
                
                'price': self.clean_text(product.css('.price::text').get() or 
                                       product.css('.product__price::text').get() or
                                       product.attrib.get('data-cnstrc-item-price', '')),
                
                'original_price': self.clean_text(product.css('.old::text').get()),
                
                'product_url': response.urljoin(product.css('a.product-action::attr(href)').get() or 
                                              product.css('a.product-grid-item__info-container__name::attr(href)').get()),
                
                'image_url': product.css('img::attr(src)').get(),
                
                'category': response.meta.get('category_name', ''),
                'category_url': response.url,
                
                'scraped_at': datetime.now(self.utc_tz).isoformat(),
                'product_id': product.attrib.get('data-cnstrc-item-id', ''),
                
                # Additional data attributes
                'data_attributes': {
                    'item_id': product.attrib.get('data-cnstrc-item-id'),
                    'item_name': product.attrib.get('data-cnstrc-item-name'),
                    'item_price': product.attrib.get('data-cnstrc-item-price'),
                    'strategy_id': product.attrib.get('data-cnstrc-strategy-id'),
                }
            }
            
            # Clean up the data
            item = self.clean_item(item)
            
            if item['name'] and item['price']:
                self.logger.info(f"✅ Product {index + 1}: {item['name']} - {item['price']}")
                yield item
            else:
                self.logger.warning(f"⚠️ Incomplete product data: {item}")
        
        # Handle pagination - be very careful with rate limiting
        next_page = response.css('.pagination__next a::attr(href)').get() or \
                   response.css('a[rel="next"]::attr(href)').get()
        
        if next_page and self.within_crawl_window():
            self.logger.info(f"➡️ Following next page: {next_page}")
            yield response.follow(
                next_page,
                callback=self.parse_category,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=15000),
                    ],
                    'download_delay': 10.0,
                    'category_name': response.meta.get('category_name', ''),
                }
            )
        elif next_page:
            self.logger.info("⏰ Skipping pagination due to time constraints")
    
    def extract_category_name(self, url):
        """Extract category name from URL"""
        parsed = urlparse(url)
        path_parts = parsed.path.split('/')
        if len(path_parts) >= 2:
            return path_parts[-2].replace('-', ' ').title()
        return "Unknown Category"
    
    def clean_text(self, text):
        """Clean extracted text"""
        if text:
            # Remove extra whitespace and specific currency symbols
            cleaned = ' '.join(text.strip().split())
            cleaned = cleaned.replace('R ', 'R').replace('R', 'R ')
            return cleaned
        return None
    
    def clean_item(self, item):
        """Clean the entire item dictionary"""
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = self.clean_text(value)
            elif isinstance(value, dict):
                item[key] = {k: self.clean_text(v) if isinstance(v, str) else v 
                           for k, v in value.items()}
        return item
    
    async def errback(self, failure):
        """Handle request errors"""
        self.logger.error(f"❌ Request failed: {failure.value}")
        
        # Check if it's a time window issue
        if not self.within_crawl_window():
            self.logger.info("⏰ Request failed due to time window closure")