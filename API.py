from fastapi import FastAPI, HTTPException
import pandas as pd
import os

app = FastAPI()

DATA_PATH = os.getenv("DATA_PATH", "./data/gold")

def load_data(path: str): 
    if not os.path.exists(path):
        raise HTTPException(status_code=404, detail="File not found")
    if not os.path.exists(DATA_PATH):
        raise HTTPException(status_code=500, detail="Data directory missing")
    try:
        return pd.read_parquet(path)
    except Exception:
        raise HTTPException(status_code=500, detail="Error reading data")

@app.get("/")
def root():
    return {"message": "API Works"}

@app.get("/rides-per-day")
def rides_per_day():
    df = load_data(f"{DATA_PATH}/rides_per_day.parquet")
    return df.to_dict(orient="records")

@app.get("/top-routes")
def top_routes():
    df = load_data(f"{DATA_PATH}/top_routes.parquet")
    return df.to_dict(orient="records")

@app.get("/provider-summary")
def provider_summary():
    df = load_data(f"{DATA_PATH}/provider_summary.parquet")
    return df.to_dict(orient="records")

@app.get("/health")
def health():
    return {"status": "ok"}




# Testi
"""from fastapi import FastAPI

app = FastAPI()

@app.get("/")
def root():
    return {"message": "API Works"}

@app.get("/rides-per-day")
def rides_per_day():
    return [
        {"date": "2024-01-01", "rides": 1000},
        {"date": "2024-01-02",  "rides": 1200},
    ]

@app.get("/top-routes")
def top_routes():
    return {"data": "placeholder"}

@app.get("/provider-summary")
def provider_summary():
    return {"data": "placeholder"}"""