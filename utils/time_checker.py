from datetime import datetime
import pytz

def within_crawl_window():
    """Check if current time is within allowed crawling window"""
    utc_tz = pytz.utc
    utc_now = datetime.now(utc_tz)
    hour, minute = utc_now.hour, utc_now.minute
    
    # 04:00-08:45 UTC window
    if hour < 4 or hour > 8:
        return False, f"Outside crawling hours. Current UTC: {utc_now.strftime('%H:%M')}"
    if hour == 8 and minute > 45:
        return False, f"Outside crawling hours. Current UTC: {utc_now.strftime('%H:%M')}"
    return True, f"Within allowed window. Current UTC: {utc_now.strftime('%H:%M')}"

def get_crawl_window_info():
    """Get information about the crawl window"""
    allowed, message = within_crawl_window()
    return {
        "allowed": allowed,
        "message": message,
        "window_utc": "04:00-08:45",
        "window_sast": "06:00-10:45",
        "current_utc": datetime.now(pytz.utc).strftime('%Y-%m-%d %H:%M:%S UTC')
    }