# api/dbt_server.py
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel, Field

from datetime import date

import asyncio

#----------------------------------
# App initialization
#----------------------------------
app = FastAPI(
    title="DBT Gold Transformations API", 
    description="API to access DBT service for applying gold transforms on silver bucket data for datasets", 
    version="1.0.0", 
    )

#-----------------------------------
# API Response Models
#-----------------------------------

#--------Input Validation Models------------------

class TaxiTransformRequest(BaseModel):
    year  : int = Field(..., ge=2009, le=int(date.today().year), description = "Year in which the month to transform is")
    month : int = Field(..., ge=1, le=12, description= "Month from silver bucket which we wish to transform")

#--------Response Models---------------------------

class TaxiTransformResponse(BaseModel):
    success: bool
    stdout: str
    stderr: str

class HealthResponse(BaseModel):
    status: str

#-----------------------------------
# API Endpoints
#-----------------------------------

@app.get("/")
def root():
    return {
        "message": "DBT Gold Transformation Service",
        "description": "Use the /taxi, /weather",
        "endpoints": {
            "/taxi": "POST: Run gold transformations for taxi silver bucket",
            "/weather": "POST: Run gold transformations for weather silver bucekt",
        },
    }


@app.get("/health")
def health_check():
    return HealthResponse(
        status = "healthy"
    )


@app.post("/taxi")
async def run_taxi_transform(request: TaxiTransformRequest):
    process = await asyncio.create_subprocess_exec(
        "dbt", "build",
        "--target", "prod",
        "--select", "+tag:taxi",
        "--vars", f'{{"year": {request.year}, "month": {request.month}}}',
        cwd    = "/usr/app/dbt",
        stdout = asyncio.subprocess.PIPE,
        stderr = asyncio.subprocess.PIPE
    )

    stdout, stderr = await process.communicate()

    response = TaxiTransformResponse(
        success = process.returncode == 0,
        stdout  = stdout.decode(),
        stderr  = stderr.decode()
    )

    if not response.success:
        raise HTTPException(status_code=500, detail=response.dict())

    return response
