import scrapy
import json
import pytz
from datetime import datetime
from scrapy_playwright.page import PageMethod
from urllib.parse import urljoin
import logging
import re

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
        self.target_products = 5  # Aim for 5
        self.min_products = 2     # Minimum 2
    
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
        self.logger.info(f"🎯 Aiming for {self.target_products} products per category (minimum {self.min_products})")
        
        # Updated categories with latest URLs
        category_urls = [
            {
                'url': 'https://www.pnp.co.za/c/pnpbase?query=:relevance:allCategories:pnpbase:category:milk-dairy-and-eggs-423144840',
                'main_category': 'Groceries',
                'sub_category': 'Milk Dairy and Eggs'
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
                        PageMethod('wait_for_selector', 'div.product-grid-item', timeout=40000),
                        PageMethod('wait_for_timeout', 8000),  # Even longer wait
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
        
        # Extract product elements - use the exact selector from your HTML
        products = response.css('div.product-grid-item')
        
        self.logger.info(f"🎯 Found {len(products)} total products")
        
        if len(products) == 0:
            self.logger.error(f"💥 No products found in {main_category}")
            return
        
        scraped_count = 0
        all_valid_items = []
        
        # First pass: Try to extract as many valid products as possible
        for index, product in enumerate(products):
            item = self.extract_product_data(product, response, main_category, sub_category, index)
            
            if item and self.is_valid_product(item):
                all_valid_items.append(item)
                scraped_count += 1
                self.logger.info(f"✅ Product {scraped_count}: {item['name']} - {item['price']}")
        
        # If we don't have enough valid products, try with more aggressive extraction
        if len(all_valid_items) < self.min_products and len(products) > len(all_valid_items):
            self.logger.info(f"🔄 Only got {len(all_valid_items)} products, trying aggressive extraction...")
            for index, product in enumerate(products[len(all_valid_items):], len(all_valid_items)):
                if len(all_valid_items) >= self.target_products:
                    break
                    
                item = self.extract_product_data_aggressive(product, response, main_category, sub_category, index)
                if item and self.is_valid_product(item):
                    all_valid_items.append(item)
                    scraped_count += 1
                    self.logger.info(f"✅ Product {scraped_count}: {item['name']} - {item['price']}")
        
        # Yield all valid items (up to target limit)
        for item in all_valid_items[:self.target_products]:
            yield item
        
        final_count = min(len(all_valid_items), self.target_products)
        self.logger.info(f"📊 Successfully extracted {final_count} products from {main_category}")
        self.logger.info(f"⏹️ Finished with {main_category}, moving to next category")
    
    def extract_product_data(self, product, response, main_category, sub_category, index):
        """Extract product data using reliable methods from your HTML examples"""
        
        # METHOD 1: Data attributes (most reliable when available)
        name = product.attrib.get('data-cnstrc-item-name', '').strip()
        price_value = product.attrib.get('data-cnstrc-item-price', '').strip()
        product_id = product.attrib.get('data-cnstrc-item-id', '').strip()
        
        # METHOD 2: CSS selectors from your exact HTML structure
        if not name:
            # Exact selector from your HTML: span inside product name link
            name = product.css('a.product-grid-item__info-container__name span::text').get()
            if name:
                name = name.strip()
        
        # METHOD 3: Price extraction from visible elements
        if not price_value:
            # Try multiple price selectors from your HTML examples
            price_selectors = [
                '.price::text',
                '.cms-price-display .price::text',
                '.plp-price .price::text',
                '.product-grid-item__price-container .price::text'
            ]
            
            for selector in price_selectors:
                price_text = product.css(selector).get()
                if price_text:
                    price_text = price_text.strip()
                    # Extract numbers from price text (e.g., "R24.99" -> "24.99")
                    numbers = re.findall(r'\d+\.?\d*', price_text)
                    if numbers:
                        price_value = numbers[0]
                        break
        
        # Format price
        price = f"R {price_value}" if price_value and price_value != "0.00" else ""
        
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
        
        # Only create item if we have essential data
        if not name or not price_value:
            return None
        
        # Build the complete item
        item = {
            'name': name,
            'price': price,
            'price_value': price_value,
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
                'item_name': product.attrib.get('data-cnstrc-item-name', ''),
                'item_price': product.attrib.get('data-cnstrc-item-price', ''),
                'strategy_id': product.attrib.get('data-cnstrc-strategy-id', ''),
            }
        }
        
        return self.clean_item(item)
    
    def extract_product_data_aggressive(self, product, response, main_category, sub_category, index):
        """Aggressive extraction with more fallbacks"""
        
        # Try all possible name selectors
        name_selectors = [
            'a.product-grid-item__info-container__name span::text',
            'a[aria-label]::attr(aria-label)',
            '.product-name::text',
            '.product-title::text',
            'span::text'  # Last resort
        ]
        
        name = None
        for selector in name_selectors:
            name = product.css(selector).get()
            if name:
                name = name.strip()
                if name and len(name) > 2:  # Basic validation
                    break
                name = None
        
        # Try all possible price selectors
        price_selectors = [
            '.price::text',
            '.cms-price-display .price::text',
            '.plp-price .price::text',
            '[class*="price"]::text',
            'div::text'  # Last resort
        ]
        
        price_value = None
        for selector in price_selectors:
            elements = product.css(selector)
            for elem in elements:
                price_text = elem.get()
                if price_text:
                    price_text = price_text.strip()
                    if 'R' in price_text:
                        numbers = re.findall(r'\d+\.?\d*', price_text)
                        if numbers:
                            price_value = numbers[0]
                            break
            if price_value:
                break
        
        # If we still don't have name or price, skip
        if not name or not price_value:
            return None
        
        # Get other data
        product_url = product.css('a::attr(href)').get()
        if product_url:
            product_url = response.urljoin(product_url)
        
        image_url = product.css('img::attr(src)').get()
        product_id = product.attrib.get('data-cnstrc-item-id', f"item_{index}")
        original_price = product.css('.old::text').get()
        if original_price:
            original_price = original_price.strip()
        
        # Build item
        item = {
            'name': name,
            'price': f"R {price_value}",
            'price_value': price_value,
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
    
    def is_valid_product(self, item):
        """Validate that we have a real product"""
        if not item.get('name') or not item.get('price_value'):
            return False
        
        # Check if name is not a placeholder
        name = item['name'].lower()
        invalid_keywords = ['product', 'unknown', 'placeholder', 'item_']
        for keyword in invalid_keywords:
            if keyword in name:
                return False
        
        # Check if price is realistic
        try:
            price = float(item['price_value'])
            if price <= 0 or price > 10000:  # Reasonable price range
                return False
        except ValueError:
            return False
        
        return True
    
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