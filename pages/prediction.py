import streamlit as st
import requests
from pydantic import BaseModel

FASTAPI_URL = 'http://0.0.0.0:8000/predict'

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

invoice = st.text_input("q_per_invoice", value=1)
shop = st.text_input("Shop", value='35')
product_code = st.text_input("Product Code", value='-6695580755828751834')
original_sale_amount = st.number_input("Original Sale Amount Incl VAT", value=99.95)
customer_id = st.text_input("Customer ID", value='-2190786785520839526')
revenue_incl_vat = st.number_input("Revenue Incl VAT", value=74.96)
cost_price_excl_vat = st.number_input("Cost Price Excl VAT", value=36.53)
brand_name = st.text_input("Brand Name", value='3694837121284491212')
model_group = st.text_input("Model Group", value='3162564956579801398')
product_group = st.text_input("Product Group", value='-453682476182549203')


# Initialize input_data dictionary
if st.button("Predict"):
    item = Item(
        q_per_invoice = invoice,
        Shop = shop,
        ProductCode=product_code,
        OriginalSaleAmountInclVAT=original_sale_amount,
        CustomerID=customer_id,
        RevenueInclVAT=revenue_incl_vat,
        CostPriceExclVAT=cost_price_excl_vat,
        BrandName=brand_name,
        ModelGroup=model_group,
        ProductGroup=product_group)

    response = requests.post(FASTAPI_URL,json=item.model_dump())

    # Make prediction and display result
    if response.status_code == 200:
        prediction = response.json()["prediction"]
        increased_odds = prediction[1] * 1.5
        formatted_odds = "{:.2f}".format(increased_odds)  # Format to display only relevant numbers
        st.write(f"The predicted odds that the item will be returned: {formatted_odds}%")
    else:
        st.error("Failed to get prediction from the server.")