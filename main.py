from fastapi import FastAPI, HTTPException
import logging
import joblib
import pandas as pd
from pydantic import BaseModel

app = FastAPI()

class Item(BaseModel):
    q_per_invoice: int
    Shop: str 
    ProductCode: str
    OriginalSaleAmountInclVAT: float 
    CustomerID: str
    RevenueInclVAT: float 
    CostPriceExclVAT: float 
    BrandName: str 
    ModelGroup: str
    ProductGroup: str 

@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.post("/predict")
async def create_item(item: Item):
    try:
        # Convert input data to DataFrame
        data = pd.DataFrame([item.dict()]) 

        artifacts = joblib.load("model/artifacts.joblib")
        model = artifacts["model"]

        for col in ['Shop', 'ProductCode', 'CustomerID', 'BrandName', 'ModelGroup', 'ProductGroup']:
            label_encoder = joblib.load(f"model/{col}_label_encoder.joblib")
            data[col] = label_encoder.transform(data[col])

        # Make predictions
        prediction = model.predict_proba(data)

        # Convert prediction to a list
        prediction_list = prediction[0].tolist()

        # Return the prediction
        return {"prediction": prediction_list}

    except Exception as e:
        # Log the error for debugging
        logging.error(f"Prediction error: {e}")
        # Return a 500 error response
        raise HTTPException(status_code=500, detail="An error occurred during prediction.") from e


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, port=8000, host="0.0.0.0")
