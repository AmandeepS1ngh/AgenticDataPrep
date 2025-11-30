import streamlit as st
import requests
import pandas as pd
import json
from io import StringIO

FASTAPI_URL = "http://127.0.0.1:8000"

st.set_page_config(page_title="AI-Powered Data Cleaning", layout="wide")
st.title("AI-Powered Data Cleaning Application")

st.sidebar.header("Data Source Selection")
data_source = st.sidebar.radio(
    "Select Data Source:",
    ["Upload CSV/Excel", "Database Query", "API Data"],
    index=0
)

st.markdown(""" **AI-POWERED DATA CLEANING APPLICATION** 
            *Clean your data effortlessly using AI.*""")

# ================= CSV / Excel Upload =================
if data_source == "Upload CSV/Excel":
    st.subheader("Upload your CSV or Excel file")
    uploaded_file = st.file_uploader("Choose a file", type=["csv", "xlsx", "xls"])
    if uploaded_file is not None:
        file_extension = uploaded_file.name.split(".")[-1].lower()
        if file_extension == "csv":
            df = pd.read_csv(uploaded_file)
        else:
            df = pd.read_excel(uploaded_file)

        st.write("RAW DATA PREVIEW:")
        st.dataframe(df.head(10))

        if st.button("Clean Data"):
            files = {"file": (uploaded_file.name, uploaded_file.getvalue())}
            response = requests.post(f"{FASTAPI_URL}/cleandata/", files=files)

            if response.status_code == 200:
                st.subheader("Raw API Response (Debugging)")
                st.json(response.json())

                try:
                    cleaned_data_raw = response.json().get("cleaned_data", [])
                    cleaned_data = pd.DataFrame(cleaned_data_raw)

                    st.subheader("Cleaned Data")
                    st.dataframe(cleaned_data)

                except Exception as e:
                    st.error(f"Error converting response to DataFrame: {e}")

            else:
                st.error("Failed to clean data")

# ================= Database Query =================
elif data_source == "Database Query":
    st.subheader("Enter Database Query")
    db_url = st.text_input("Database Connection URL", value="postgresql://user:password@localhost:5432/db")
    query = st.text_area("Enter SQL Query", value="SELECT * FROM my_table;")

    if st.button("Fetch and Clean Data"):
        response = requests.post(f"{FASTAPI_URL}/clean-db/", json={"db_url": db_url, "query": query})

        if response.status_code == 200:
            st.subheader("Raw API Response (Debugging)")
            st.json(response.json())

            try:
                cleaned_data_raw = response.json().get("cleaned_data", [])
                cleaned_data = pd.DataFrame(cleaned_data_raw)

                st.subheader("Cleaned Data")
                st.dataframe(cleaned_data)

            except Exception as e:
                st.error(f"Error converting response to DataFrame: {e}")
        else:
            st.error("Failed to fetch/clean data from database")

# ================= API Data =================
elif data_source == "API Data":
    st.subheader("Enter API URL")
    api_url = st.text_input("Enter API Endpoint", "https://jsonplaceholder.typicode.com/posts")

    if st.button("Fetch and Clean Data"):
        response = requests.post(f"{FASTAPI_URL}/clean-api/", json={"api_url": api_url})

        if response.status_code == 200:
            st.subheader("Raw API Response (Debugging)")
            st.json(response.json())

            try:
                cleaned_data_raw = response.json().get("cleaned_data", [])
                cleaned_data = pd.DataFrame(cleaned_data_raw)

                st.subheader("Cleaned Data")
                st.dataframe(cleaned_data)

            except Exception as e:
                st.error(f"Error converting response to DataFrame: {e}")
        else:
            st.error("Failed to fetch/clean data from API")

st.markdown("""
Built with **Streamlit + FastAPI + AI** for automated data cleaning.
""")
