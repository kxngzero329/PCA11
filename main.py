from fastapi import FastAPI, HTTPException, BackgroundTasks
from pydantic import BaseModel
from typing import List, Optional
import subprocess
import asyncio
import json
import os
from datetime import datetime
import pytz

# Import from local utils
from utils.time_checker import within_crawl_window, get_crawl_window_info

app = FastAPI(title="Pick n Pay Scraper API", version="1.0.0")

class ScrapeResponse(BaseModel):
    status: str
    message: str
    task_id: Optional[str] = None
    timestamp: str

class ScrapeStatus(BaseModel):
    task_id: str
    status: str
    products_scraped: int
    start_time: str
    end_time: Optional[str] = None

# In-memory storage for scrape status
scrape_jobs = {}

@app.get("/")
async def root():
    return {
        "message": "Pick n Pay Scraper API is running!",
        "status": "active",
        "endpoints": {
            "status": "/scrape/status",
            "start_scraping": "/scrape/start (POST)",
            "get_results": "/scrape/results",
            "docs": "/docs"
        }
    }

@app.get("/scrape/status")
async def scrape_status():
    """Check if scraping is currently allowed"""
    allowed, message = within_crawl_window()
    return {
        "scraping_allowed": allowed,
        "message": message,
        "window_utc": "04:00-08:45",
        "window_sast": "06:00-10:45", 
        "current_utc": datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC'),
        "current_sast": datetime.now(pytz.timezone('Africa/Johannesburg')).strftime('%Y-%m-%d %H:%M:%S SAST')
    }

@app.post("/scrape/start", response_model=ScrapeResponse)
async def start_scrape(background_tasks: BackgroundTasks):
    """Start the scraping process"""
    allowed, message = within_crawl_window()
    
    if not allowed:
        raise HTTPException(
            status_code=423,
            detail=f"Scraping not allowed: {message}"
        )
    
    task_id = datetime.now().strftime("%Y%m%d_%H%M%S")
    
    scrape_jobs[task_id] = {
        "status": "running",
        "start_time": datetime.now().isoformat(),
        "products_scraped": 0
    }
    
    background_tasks.add_task(run_scrapy_spider, task_id)
    
    return ScrapeResponse(
        status="started",
        message="Scraping initiated within allowed window",
        task_id=task_id,
        timestamp=datetime.now().isoformat()
    )

@app.get("/scrape/jobs/{task_id}")
async def get_job_status(task_id: str):
    """Get status of a specific scrape job"""
    if task_id not in scrape_jobs:
        raise HTTPException(status_code=404, detail="Job not found")
    return scrape_jobs[task_id]

@app.get("/scrape/results")
async def get_scrape_results():
    """Get the latest scrape results"""
    try:
        with open('data/products.json', 'r', encoding='utf-8') as f:
            products = json.load(f)
        return {
            "count": len(products),
            "products": products
        }
    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="No results found. Run scraper first.")

async def run_scrapy_spider(task_id: str):
    """Run the Scrapy spider in a subprocess"""
    try:
        process = await asyncio.create_subprocess_exec(
            'python', 'run_scraper.py',
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE
        )
        
        stdout, stderr = await process.communicate()
        
        if process.returncode == 0:
            scrape_jobs[task_id]["status"] = "completed"
            scrape_jobs[task_id]["end_time"] = datetime.now().isoformat()
            try:
                with open('data/products.json', 'r', encoding='utf-8') as f:
                    products = json.load(f)
                    scrape_jobs[task_id]["products_scraped"] = len(products)
            except:
                scrape_jobs[task_id]["products_scraped"] = 0
        else:
            scrape_jobs[task_id]["status"] = "failed"
            scrape_jobs[task_id]["error"] = stderr.decode()
            
    except Exception as e:
        scrape_jobs[task_id]["status"] = "failed"
        scrape_jobs[task_id]["error"] = str(e)

if __name__ == "__main__":
    import uvicorn
    uvicorn.run("main:app", host="0.0.0.0", port=8000, reload=True)