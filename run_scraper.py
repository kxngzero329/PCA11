#!/usr/bin/env python3
"""
Script to run the Pick n Pay scraper with proper settings
"""
import os
import sys
from scrapy.crawler import CrawlerProcess
from scrapy.utils.project import get_project_settings

# Add current directory to Python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from spiders.picknpay_spider import PicknPaySpider
from utils.time_checker import within_crawl_window

def main():
    """Main function to run the scraper"""
    
    # Check if within crawl window
    allowed, message = within_crawl_window()
    
    if not allowed:
        print(f"❌ {message}")
        print("⏰ Please run between 04:00-08:45 UTC (06:00-10:45 SAST)")
        sys.exit(1)
    
    print("✅ Within crawling window, starting scraper...")
    print("📝 Settings:")
    print("   - Rate limit: 10 seconds between requests")
    print("   - Concurrent requests: 1")
    print("   - Respecting robots.txt")
    print("   - Using Playwright for JavaScript rendering")
    print("   - Using actual category URLs from sitemap")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Configure and run Scrapy
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    
    print("🚀 Starting Pick n Pay spider...")
    process.crawl(PicknPaySpider)
    process.start()
    
    print("✅ Scraping completed!")

if __name__ == "__main__":
    main()