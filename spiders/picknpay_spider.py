import scrapy
import json
import pytz
from datetime import datetime
from scrapy_playwright.page import PageMethod
from urllib.parse import urljoin
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
        self.products_per_category = 2
    
    def within_crawl_window(self):
        """Check if current time is within allowed crawling window"""
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
        self.logger.info(f"🎯 Scraping {self.products_per_category} products per category")
        
        # FINAL 6 categories (removed Baby and Kids, added Stationery)
        category_urls = [
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:food-cupboard-423144840',
                'main_category': 'Groceries',
                'sub_category': 'Food Cupboard'
            },
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:household-and-cleaning-423144840',
                'main_category': 'Cleaning and Household', 
                'sub_category': 'Household and Cleaning'
            },
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:personal-care-and-hygiene-423144840',
                'main_category': 'Personal Care',
                'sub_category': 'Personal Care and Hygiene'
            },
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:health-and-wellness-423144840',
                'main_category': 'Health and Wellness',
                'sub_category': 'Health and Wellness'
            },
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:electronics-and-office-423144840',
                'main_category': 'Electronics',
                'sub_category': 'Electronics and Office'
            },
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:stationery-423144840',
                'main_category': 'Stationery',
                'sub_category': 'Stationery'
            }
        ]
        
        self.logger.info(f"📂 Processing {len(category_urls)} main categories")
        
        for category_info in category_urls:
            self.logger.info(f"📦 Queueing: {category_info['main_category']}")
            yield scrapy.Request(
                url=category_info['url'],
                callback=self.parse_category,
                meta={
                    'playwright': True,
                    'playwright_page_methods': [
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=25000),
                        PageMethod('wait_for_timeout', 3000),
                    ],
                    'download_delay': 10.0,
                    'main_category': category_info['main_category'],
                    'sub_category': category_info['sub_category'],
                },
                errback=self.errback,
            )
    
    def parse_category(self, response):
        """Parse category page and extract products"""
        main_category = response.meta.get('main_category', 'Unknown')
        sub_category = response.meta.get('sub_category', 'Unknown')
        
        self.logger.info(f"📁 Processing: {main_category} > {sub_category}")
        self.logger.info(f"🔗 URL: {response.url}")
        
        # Extract product elements using the exact selector from your HTML
        products = response.css('div.product-grid-item')
        self.logger.info(f"🎯 Found {len(products)} total products")
        
        if len(products) == 0:
            self.logger.warning(f"❌ No products found in {main_category}")
            return
        
        # Limit to specified number of products per category
        limited_products = products[:self.products_per_category]
        self.logger.info(f"🔒 Limiting to first {len(limited_products)} products")
        
        product_count = 0
        for index, product in enumerate(limited_products):
            item = self.extract_product_data(product, response, main_category, sub_category)
            
            if item and item.get('name') and item.get('price'):
                product_count += 1
                self.logger.info(f"✅ Product {index + 1}: {item['name']} - {item['price']}")
                yield item
            else:
                self.logger.warning(f"⚠️ Skipping incomplete product {index + 1}")
        
        self.logger.info(f"📊 Successfully extracted {product_count} products from {main_category}")
        self.logger.info(f"⏹️ Finished with {main_category}, moving to next category")
    
    def extract_product_data(self, product, response, main_category, sub_category):
        """Extract product data using the data attributes from your HTML examples"""
        
        # Extract from data attributes (most reliable based on your HTML)
        name = product.attrib.get('data-cnstrc-item-name', '').strip()
        price_value = product.attrib.get('data-cnstrc-item-price', '').strip()
        product_id = product.attrib.get('data-cnstrc-item-id', '').strip()
        
        # Format price with R prefix
        price = f"R {price_value}" if price_value else ""
        
        # Get product URL
        product_url = product.css('a.product-action::attr(href)').get()
        if not product_url:
            product_url = product.css('a.product-grid-item__info-container__name::attr(href)').get()
        
        if product_url:
            product_url = response.urljoin(product_url)
        
        # Get image URL
        image_url = product.css('img::attr(src)').get()
        
        # Get original price if on sale
        original_price = product.css('.old::text').get()
        if original_price:
            original_price = original_price.strip()
        
        # Build the complete item
        item = {
            'name': name,
            'price': price,
            'original_price': original_price,
            'product_url': product_url,
            'image_url': image_url,
            'product_id': product_id,
            'main_category': main_category,
            'sub_category': sub_category,
            'category_url': response.url,
            'scraped_at': datetime.now(self.utc_tz).isoformat(),
            'data_attributes': {
                'item_id': product_id,
                'item_name': name,
                'item_price': price_value,
                'strategy_id': product.attrib.get('data-cnstrc-strategy-id', ''),
            }
        }
        
        return self.clean_item(item)
    
    def clean_item(self, item):
        """Clean and validate the item data"""
        for key, value in item.items():
            if isinstance(value, str):
                item[key] = self.clean_text(value)
            elif isinstance(value, dict):
                item[key] = {k: self.clean_text(v) if isinstance(v, str) else v 
                           for k, v in value.items()}
        return item
    
    def clean_text(self, text):
        """Clean text by removing extra whitespace"""
        if text:
            return ' '.join(text.strip().split())
        return None
    
    async def errback(self, failure):
        """Handle request errors"""
        self.logger.error(f"❌ Request failed: {failure.value}")