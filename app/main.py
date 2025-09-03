from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
import logging
import traceback

from app.routers import files, nodes, visualizations
from app.config import settings

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)

app = FastAPI(
    title="Orange Mining Backend",
    description="FastAPI backend for visual data mining with Redis caching",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:3001"],  # Add your frontend URLs
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(files.router)
app.include_router(nodes.router)
app.include_router(visualizations.router)

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """Global exception handler"""
    logging.error(f"Global exception: {str(exc)}")
    logging.error(traceback.format_exc())
    
    return JSONResponse(
        status_code=500,
        content={
            "message": "Internal server error",
            "detail": str(exc) if app.debug else "An error occurred"
        }
    )

@app.get("/")
async def root():
    return {"message": "Orange Mining Backend API", "status": "running"}

@app.get("/health")
async def health_check():
    """Health check endpoint"""
    try:
        from app.services.redis_service import redis_service
        # Test Redis connection
        await redis_service.redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}
@app.get("/hi")
async def health_check():
    """Health check endpoint"""
    try:
        from app.services.redis_service import redis_service
        # Test Redis connection
        await redis_service.redis_client.ping()
        return {"status": "healthy", "redis": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)