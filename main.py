from fastapi import FastAPI, HTTPException, Request, Header
from fastapi.middleware.cors import CORSMiddleware
import uvicorn
from cron_job import generate_and_insert_data
from datetime import datetime
import os
from dotenv import load_dotenv
import logging
import httpx
import json
from sse_starlette.sse import EventSourceResponse
import asyncio
import threading

# ======================
# Configuration
# ======================
load_dotenv()

app = FastAPI(
    title="Energy Optimization Backend",
    description="Production-ready backend with SSE integration",
    version="10.0",
    docs_url="/docs",
    redoc_url=None
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

# ======================
# Constants
# ======================
API_KEY = os.getenv("API_KEY")
PRODUCTION_URL = "https://energy-optimisation-backend.onrender.com"
SSE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/house/1/subscribe"
TOGGLE_ENDPOINT = f"{PRODUCTION_URL}/api/device-status/{{deviceId}}/toggle"

# ======================
# Setup
# ======================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(),
        logging.FileHandler("energy_backend.log")
    ]
)
logger = logging.getLogger(__name__)

# ======================
# Active Jobs Tracking
# ======================
active_cron_jobs = {}  # Dictionary to keep track of active jobs and their stop events

def run_cron_job(device_id, stop_event):
    """Run the cron job in a separate thread and handle stopping"""
    try:
        device_ids = [str(device_id)]
        thread = threading.Thread(
            target=generate_and_insert_data,
            args=(device_ids, stop_event),
            daemon=True
        )
        thread.start()
        logger.info(f"[CRON] Thread started for device {device_id}")
        return thread
    except Exception as e:
        logger.error(f"[CRON] Error starting thread for device {device_id}: {e}")
        return None

# ======================
# Device Status Management
# ======================
def process_device_status(devices):
    """Process device status updates and manage cron jobs accordingly"""
    # Devices to monitor for cron job triggering
    CRON_DEVICE_IDS = {"1", "3", "16"}
    
    device_status_changes = []
    
    for device in devices:
        device_id = str(device.get("deviceId"))
        status = device.get("on")
        
        if device_id in CRON_DEVICE_IDS:
            if status and device_id not in active_cron_jobs:
                # Device turned ON and no active job - start new job
                logger.info(f"[CRON] Starting for device {device_id}")
                stop_event = threading.Event()
                thread = run_cron_job(device_id, stop_event)
                
                if thread:
                    active_cron_jobs[device_id] = {
                        "thread": thread,
                        "stop_event": stop_event,
                        "started_at": datetime.now().isoformat(),
                        "device_name": device.get("deviceName", "Unknown")
                    }
                    device_status_changes.append(f"Started job for {device.get('deviceName', 'Device')} (ID: {device_id})")
                    logger.info(f"[CRON] Started job for device {device_id}")
            
            elif not status and device_id in active_cron_jobs:
                # Device turned OFF and has active job - stop job
                logger.info(f"[CRON] Stopping job for device {device_id}")
                job_info = active_cron_jobs[device_id]
                job_info["stop_event"].set()  # Signal thread to stop
                device_status_changes.append(f"Stopped job for {job_info.get('device_name', 'Device')} (ID: {device_id})")
                del active_cron_jobs[device_id]
                logger.info(f"[CRON] Stopped job for device {device_id}")
    
    return device_status_changes

# ======================
# API Endpoints
# ======================
@app.get("/sse-subscribe")
async def sse_subscribe(request: Request):
    async def event_generator():
        try:
            async with httpx.AsyncClient() as client:
                async with client.stream(
                    "GET",
                    SSE_ENDPOINT,
                    headers={"x-api-key": API_KEY},
                    timeout=None
                ) as response:
                    logger.info(f"Connected to SSE: {SSE_ENDPOINT}")

                    async for line in response.aiter_lines():
                        if line.strip():
                            logger.info(f"[SSE RAW] {line}")
                            
                            # Handle both "event:" and "data:" lines
                            if line.startswith("event:"):
                                event_type = line[6:].strip()
                                logger.info(f"[SSE EVENT] {event_type}")
                                continue
                            
                            if line.startswith("data:"):
                                try:
                                    raw_data = line[5:].strip()
                                    devices = json.loads(raw_data)
                                    logger.info(f"[SSE DATA] Received data for {len(devices)} devices")
                                    
                                    # Process device status changes
                                    changes = process_device_status(devices)
                                    if changes:
                                        logger.info(f"[CRON CHANGES] {', '.join(changes)}")
                                        
                                except json.JSONDecodeError as e:
                                    logger.error(f"[SSE ERROR] JSON decode error: {e}, Raw data: {raw_data[:100]}")
                                except Exception as e:
                                    logger.error(f"[SSE ERROR] Processing error: {e}")
                        
                        # Return the raw line to the client
                        yield f"{line}\n"

        except Exception as e:
            logger.error(f"[ERROR] SSE Connection Error: {e}")
            yield "event: error\ndata: Connection failed\n\n"

    return EventSourceResponse(event_generator())


@app.post("/toggle-device/{device_id}")
async def toggle_device(device_id: str):
    """Proxy to toggle device in production"""
    try:
        logger.info(f"[TOGGLE] Toggling device {device_id}")
        response = await make_authenticated_request(
            TOGGLE_ENDPOINT.format(deviceId=device_id),
            method="POST"
        )
        result = response.json()
        logger.info(f"[TOGGLE] Response: {result}")
        return result
    except Exception as e:
        logger.error(f"[TOGGLE] Failed: {str(e)}")
        raise HTTPException(502, detail=str(e))

@app.get("/active-jobs")
async def get_active_jobs():
    """List active cron jobs with status"""
    result = {}
    for device_id, job_info in active_cron_jobs.items():
        result[device_id] = {
            "device_name": job_info.get("device_name", "Unknown"),
            "started_at": job_info["started_at"],
            "running": job_info["thread"].is_alive(),
            "elapsed_seconds": (datetime.now() - datetime.fromisoformat(job_info["started_at"])).total_seconds()
        }
    return result

@app.post("/force-start-job/{device_id}")
async def force_start_job(device_id: str):
    """Manually start a cron job for a device"""
    if device_id in active_cron_jobs:
        return {"status": "error", "message": f"Job for device {device_id} is already running"}
    
    stop_event = threading.Event()
    thread = run_cron_job(device_id, stop_event)
    
    if thread:
        active_cron_jobs[device_id] = {
            "thread": thread,
            "stop_event": stop_event,
            "started_at": datetime.now().isoformat(),
            "device_name": f"Device {device_id}"
        }
        return {"status": "success", "message": f"Started job for device {device_id}"}
    else:
        return {"status": "error", "message": f"Failed to start job for device {device_id}"}

@app.post("/force-stop-job/{device_id}")
async def force_stop_job(device_id: str):
    """Manually stop a cron job for a device"""
    if device_id not in active_cron_jobs:
        return {"status": "error", "message": f"No active job for device {device_id}"}
    
    job_info = active_cron_jobs[device_id]
    job_info["stop_event"].set()
    del active_cron_jobs[device_id]
    return {"status": "success", "message": f"Stopped job for device {device_id}"}

@app.get("/health")
async def health_check():
    """Comprehensive health check"""
    active_jobs_list = []
    for device_id, job_info in active_cron_jobs.items():
        active_jobs_list.append({
            "device_id": device_id,
            "device_name": job_info.get("device_name", "Unknown"),
            "running": job_info["thread"].is_alive(),
            "started_at": job_info["started_at"]
        })
    
    return {
        "status": "healthy",
        "timestamp": datetime.now().isoformat(),
        "api_key": API_KEY[:8] + "...",
        "endpoints": {
            "sse": SSE_ENDPOINT,
            "toggle": TOGGLE_ENDPOINT
        },
        "active_jobs_count": len(active_cron_jobs),
        "active_jobs": active_jobs_list
    }

# ======================
# Helpers
# ======================
async def make_authenticated_request(url: str, method="GET", **kwargs):
    max_retries = 3
    for attempt in range(max_retries):
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                headers = kwargs.pop("headers", {})
                headers["x-api-key"] = API_KEY

                response = await client.request(
                    method,
                    url,
                    headers=headers,
                    **kwargs
                )

                if response.status_code == 200:
                    return response

                logger.warning(f"Attempt {attempt + 1}: Status {response.status_code}")
                await asyncio.sleep(2 ** attempt)

        except Exception as e:
            logger.warning(f"Attempt {attempt + 1} failed: {str(e)}")
            if attempt == max_retries - 1:
                raise
            await asyncio.sleep(2 ** attempt)

    raise HTTPException(502, "All retry attempts failed")

# ======================
# Cleanup on shutdown
# ======================
@app.on_event("shutdown")
async def shutdown_event():
    """Clean up all running cron jobs when shutting down"""
    logger.info("Shutting down and stopping all cron jobs")
    for device_id, job_info in list(active_cron_jobs.items()):
        logger.info(f"Stopping job for device {device_id}")
        job_info["stop_event"].set()
    
    # Give some time for threads to terminate
    await asyncio.sleep(2)
    logger.info("All jobs stopped")

# ======================
# Startup
# ======================
if __name__ == "__main__":
    logger.info("====================================")
    logger.info(" Energy Optimization Backend Starting")
    logger.info(f" API Key: {API_KEY[:8]}...")
    logger.info(f" Production URL: {PRODUCTION_URL}")
    logger.info("====================================")

    uvicorn.run(
        app,
        host="0.0.0.0",
        port=5000,
        log_level="info",
        access_log=False,
        timeout_keep_alive=60
    )