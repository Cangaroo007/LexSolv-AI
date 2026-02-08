from fastapi import FastAPI
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
import os

app = FastAPI()
simulated_db = {"is_analyzing": False}

@app.get("/api/analysis")
async def get_analysis():
    if not simulated_db["is_analyzing"]:
        return {"status": "pending"}
    return {
        "status": "complete",
        "company_name": "Construction Corp Pty Ltd",
        "health_score": 42,
        "alerts": [
            {"type": "Critical", "msg": "Unpaid Superannuation: $12,400 (Safe Harbor Void)"},
            {"type": "Warning", "msg": "Preference Payment detected: $50k to 'Related Party'"},
            {"type": "Action", "msg": "ATO Debt: $185k - Payment plan recommended"}
        ],
        "forensic_matches": "94% (1,240/1,300 transactions reconciled)"
    }

@app.post("/api/trigger-analysis")
async def trigger():
    simulated_db["is_analyzing"] = True
    return {"status": "started"}

@app.post("/api/reset")
async def reset():
    simulated_db["is_analyzing"] = False
    return {"status": "reset"}

app.mount("/static", StaticFiles(directory="static"), name="static")

@app.get("/")
async def read_index():
    return FileResponse('static/index.html')

if __name__ == "__main__":
    import uvicorn
    port = int(os.environ.get("PORT", 8000))
    uvicorn.run(app, host="0.0.0.0", port=port)
