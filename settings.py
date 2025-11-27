import scrapy.utils.project

BOT_NAME = 'picknpay_scraper'

SPIDER_MODULES = ['spiders']
NEWSPIDER_MODULE = 'spiders'

# Playwright settings
DOWNLOAD_HANDLERS = {
    "http": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
    "https": "scrapy_playwright.handler.ScrapyPlaywrightDownloadHandler",
}

TWISTED_REACTOR = "twisted.internet.asyncioreactor.AsyncioSelectorReactor"

# Strict rate limiting as per robots.txt
DOWNLOAD_DELAY = 10.0  # 10 seconds between requests
CONCURRENT_REQUESTS = 1
CONCURRENT_REQUESTS_PER_DOMAIN = 1

# Auto-throttle to be extra careful
AUTOTHROTTLE_ENABLED = True
AUTOTHROTTLE_START_DELAY = 10
AUTOTHROTTLE_MAX_DELAY = 15
AUTOTHROTTLE_TARGET_CONCURRENCY = 0.5

# Respect robots.txt
ROBOTSTXT_OBEY = True

# Cache to avoid re-requests
HTTPCACHE_ENABLED = True
HTTPCACHE_EXPIRATION_SECS = 3600  # 1 hour

# Headers to mimic real browser
DEFAULT_REQUEST_HEADERS = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8',
    'Accept-Language': 'en',
    'Accept-Encoding': 'gzip, deflate, br',
    'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
}

# Playwright context settings
PLAYWRIGHT_CONTEXT_ARGS = {
    "ignore_https_errors": True,
    "viewport": {"width": 1920, "height": 1080},
}

# Enable or disable extensions
EXTENSIONS = {
    'scrapy.extensions.telnet.TelnetConsole': None,
}

# Item pipelines
ITEM_PIPELINES = {
    'spiders.picknpay_spider.JsonWriterPipeline': 300,
}

# Logging
LOG_LEVEL = 'INFO'

FEED_EXPORT_ENCODING = 'utf-8'