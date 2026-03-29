from fastapi import FastAPI, HTTPException
import pandas as pd
import boto3
import io
import s3fs

app = FastAPI()

def load_parquet_from_s3(path: str):
    try:
        fs = s3fs.S3FileSystem(
            key="user",
            secret="user12345",
            client_kwargs={"endpoint_url": "http://127.0.0.1:9000"}
        
        )

        df = pd.read_parquet(
            f"nyc-tlc-taxi-data/{path}",
            filesystem=fs
        )
        return df

    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/")
def root():
    return {"message": "API Works"}

@app.get("/rides-per-day")
def rides_per_day():
    df = load_parquet_from_s3("gold/rides_per_day.parquet")
    return df.to_dict(orient="records")

@app.get("/top-routes")
def top_routes(limit: int = 50):
    df = load_parquet_from_s3("gold/top_routes.parquet")
    return df.head(limit).to_dict(orient="records")

"""@app.get("/provider-summary")
def provider_summary():
    df = load_parquet_from_s3("gold/provider_summary.parquet")
    return df.to_dict(orient="records")"""

@app.get("/health")
def health():
    return {"status": "ok"}

