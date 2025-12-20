from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.api.routes_upload import router as upload_router
from app.api.routes_analysis import router as analysis_router
from app.api.routes_execute import router as execute_router
from app.api.routes_versions import router as versions_router
from app.api.routes_rollback import router as rollback_router
from app.api.routes_rescore import router as rescore_router
from app.api.routes_reports import router as reports_router
from app.api.routes_download import router as download_router

app = FastAPI(
    title="Automated Dataset Quality & Preprocessing Pipeline",
    description="Backend service for dataset quality scoring, risk detection, and preprocessing execution",
    version="1.0.0"
)

app.include_router(upload_router)
app.include_router(analysis_router)
app.include_router(execute_router)
app.include_router(versions_router)
app.include_router(rollback_router)
app.include_router(rescore_router)
app.include_router(reports_router)
app.include_router(download_router)

# CORS configuration (needed for React later)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # restrict later
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
def root():
    return {
        "status": "running",
        "service": "Dataset Quality Pipeline Backend"
    }


@app.get("/health")
def health_check():
    return {
        "status": "healthy"
    }
