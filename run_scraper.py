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
    print("   - Looking for 12 specific products")
    print("   - Category-based searching")
    
    print("📋 Products to find:")
    print("   1. Clover UHT Full Cream Long Life Milk 6 x 1L")
    print("   2. PnP Large Eggs 30 Pack")
    print("   3. Grand-pa Headache Powder 38 Pack")
    print("   4. Calpol Strawberry 100ml")
    print("   5. Sunlight Dishwashing Liquid 750ml")
    print("   6. Surf Hand Washing Powder 2kg")
    print("   7. Energizer Max AAA 12 Pack")
    print("   8. Sandisk Cruizer Blade 32GB")
    print("   9. Staedtler Pencil Crayons 24 Pack")
    print("   10. PnP A4 Counter Book 96 Pages")
    print("   11. Colgate Triple Action Toothpaste 100ml")
    print("   12. Dettol Antiseptic Liquid 750ml")
    
    # Create data directory if it doesn't exist
    os.makedirs('data', exist_ok=True)
    
    # Configure and run Scrapy
    settings = get_project_settings()
    process = CrawlerProcess(settings)
    
    print("🚀 Starting Pick n Pay spider...")
    process.crawl(PicknPaySpider)
    process.start()
    
    print("✅ Scraping completed!")
    print("📊 Check data/products.json for your results")

if __name__ == "__main__":
    main()