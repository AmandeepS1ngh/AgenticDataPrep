# 🤖 AI Agent for Data Cleaning and Preprocessing

<p align="center">
  <img src="https://img.shields.io/badge/Python-3.10-blue?style=for-the-badge&logo=python&logoColor=white" alt="Python 3.10">
  <img src="https://img.shields.io/badge/FastAPI-009688?style=for-the-badge&logo=fastapi&logoColor=white" alt="FastAPI">
  <img src="https://img.shields.io/badge/Streamlit-FF4B4B?style=for-the-badge&logo=streamlit&logoColor=white" alt="Streamlit">
  <img src="https://img.shields.io/badge/Google%20Gemini-4285F4?style=for-the-badge&logo=google&logoColor=white" alt="Gemini AI">
  <img src="https://img.shields.io/badge/LangGraph-121212?style=for-the-badge" alt="LangGraph">
</p>

<p align="center">
  <strong>An intelligent AI-powered system designed to automate data cleaning and preprocessing tasks for machine learning workflows.</strong>
</p>

---

## 📖 Overview

This project leverages **Python, FastAPI, Streamlit, and Google Gemini AI** with **LangGraph** to streamline data preparation, ensuring clean, consistent, and model-ready datasets. It combines traditional data cleaning techniques with AI-powered intelligent processing for a fully automated data preprocessing pipeline.

---

## ✨ Features

| Feature | Description |
|---------|-------------|
| 🤖 **AI-Powered Data Cleaning** | Automatically detects and handles missing values, outliers, and inconsistencies using Google Gemini AI |
| 🧩 **Automated Preprocessing** | Performs encoding, normalization, and scaling operations efficiently |
| 🗄️ **Database Integration** | Ingests raw data directly from **PostgreSQL** and **MySQL** databases |
| 🌐 **API Data Fetching** | Fetch and clean data from external REST APIs |
| 📁 **File Upload Support** | Upload and process CSV and Excel files |
| ⚙️ **RESTful API Backend** | Real-time endpoints via **FastAPI** for seamless integration with ML pipelines |
| 🧠 **AI Agent with LangGraph** | Uses **LangGraph StateGraph** for intelligent workflow orchestration |
| 🖥️ **Modern Web UI** | Beautiful **Streamlit** interface for easy interaction |

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        STREAMLIT UI                              │
│   (Upload CSV/Excel │ Database Query │ External API)            │
└────────────────────────────┬────────────────────────────────────┘
                             │
                             ▼
┌─────────────────────────────────────────────────────────────────┐
│                     FASTAPI BACKEND                              │
│     Endpoints: /cleandata/ │ /clean-db/ │ /clean-api/           │
└────────────────────────────┬────────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              ▼                              ▼
┌─────────────────────────┐    ┌─────────────────────────────────┐
│    DATA CLEANING        │    │         AI AGENT                 │
│  (Traditional Methods)  │ →  │  (Gemini AI + LangGraph)         │
│  • Handle missing vals  │    │  • Intelligent imputation        │
│  • Remove duplicates    │    │  • Anomaly detection             │
│  • Normalize data       │    │  • Context-aware formatting      │
│  • Fix data types       │    │  • Returns cleaned JSON          │
└─────────────────────────┘    └─────────────────────────────────┘
                                             │
                                             ▼
                              ┌─────────────────────────────────┐
                              │      CLEANED DATA (JSON)        │
                              │   Displayed in Streamlit UI     │
                              └─────────────────────────────────┘
```

---

## 📁 Project Structure

```
Ai_Agent_for_Data_Cleaning_and_Preprocessing/
│
├── 📄 .env                      # Environment variables (API keys, DB config)
├── 📄 README.md                 # Project documentation
├── 📄 requirement.txt           # Python dependencies
│
├── 📂 app/
│   └── 📄 app.py                # Streamlit frontend application
│
├── 📂 data/
│   ├── 📄 sample_data.csv       # Sample CSV dataset
│   └── 📄 sample_data.xlsx      # Sample Excel dataset
│
└── 📂 Scripts/
    ├── 📄 ai_agent.py           # AI Agent with Gemini & LangGraph
    ├── 📄 backend.py            # FastAPI backend server
    ├── 📄 data_cleaning.py      # Traditional data cleaning methods
    ├── 📄 data_ingestion.py     # Data loading utilities
    ├── 📄 main.py               # Main entry point
    ├── 📄 test_mysql_connection.py    # MySQL connection test
    └── 📄 test_postgre_connection.py  # PostgreSQL connection test
```

---

## 🛠️ Tech Stack

| Category | Technology |
|----------|------------|
| **Frontend** | Streamlit |
| **Backend** | FastAPI, Uvicorn |
| **AI/ML** | Google Gemini 2.0 Flash, LangGraph |
| **Data Processing** | Pandas, NumPy, Scikit-learn |
| **Database** | PostgreSQL, MySQL (via SQLAlchemy) |
| **HTTP Client** | Requests, AIOHTTP |

---

## 🚀 Getting Started

### Prerequisites

#### Python Version Requirements

**✅ Recommended: Python 3.10**

This project is fully tested and optimized for **Python 3.10**. Here's the compatibility breakdown:

| Python Version | Status | Notes |
|----------------|--------|-------|
| **3.10.x** | ✅ **Fully Supported** | Recommended version - all packages compatible |
| **3.11.x** | ✅ Supported | Most features work, some packages may need updates |
| **3.12.x** | ⚠️ Limited Support | Some dependencies may not be fully compatible |
| **3.13.x** | ❌ Not Supported | Several packages incompatible (scipy, ipython) |
| **3.9.x** | ⚠️ Limited Support | Some newer features may not work |

**Why Python 3.10?**
- All AI/ML libraries (LangGraph, Google Gemini, scipy, scikit-learn) are fully compatible
- Best balance between modern features and package compatibility
- Stable and widely used in production environments

#### Other Requirements

- **Google Gemini API Key** (Required for AI features)
- **PostgreSQL or MySQL** (Optional, only needed for database features)
- **Git** (for cloning the repository)

---

### Installation

#### Step 1: Clone the Repository
```bash
git clone https://github.com/yourusername/Agentic-Data-Prep.git
cd "Agentic Data Prep"
```

#### Step 2: Create Virtual Environment with Python 3.10

**On macOS/Linux:**
```bash
# Using python3.10 specifically
python3.10 -m venv .venv
source .venv/bin/activate
```

**On Windows:**
```cmd
# Using python3.10 specifically
python3.10 -m venv .venv
.venv\Scripts\activate
```

**Alternative: Using conda**
```bash
conda create -n dataprep python=3.10
conda activate dataprep
```

#### Step 3: Install Dependencies
```bash
pip install --upgrade pip
pip install -r requirement.txt
```

**Note:** If you encounter any issues during installation, try:
```bash
# Install packages one category at a time
pip install fastapi uvicorn starlette python-multipart
pip install pandas numpy openpyxl
pip install google-generativeai langgraph
pip install streamlit
pip install python-dotenv
```

#### Step 4: Configure Environment Variables

Create a `.env` file in the root directory:

```env
# Required: Google Gemini API Key
GEMINI_API_KEY=your_gemini_api_key_here

# Optional: MySQL Configuration (only if using database features)
MYSQL_HOST=127.0.0.1
MYSQL_PORT=3306
MYSQL_USER=root
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database

# Optional: PostgreSQL Configuration
POSTGRES_HOST=127.0.0.1
POSTGRES_PORT=5432
POSTGRES_USER=postgres
POSTGRES_PASSWORD=your_password
POSTGRES_DATABASE=your_database
```

**Get your Gemini API Key:**
1. Visit [Google AI Studio](https://makersuite.google.com/app/apikey)
2. Sign in with your Google account
3. Click "Get API Key" or "Create API Key"
4. Copy the key and paste it in your `.env` file

---

### Running the Application

#### Option 1: Using Two Terminals (Recommended)

**Terminal 1 - Start FastAPI Backend:**
```bash
# Make sure you're in the project root directory
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
uvicorn Scripts.backend:app --host 127.0.0.1 --port 8000 --reload
```

**Terminal 2 - Start Streamlit Frontend:**
```bash
# In a new terminal window
source .venv/bin/activate  # On Windows: .venv\Scripts\activate
streamlit run app/app.py
```

#### Option 2: Using Background Process

```bash
# Start backend in background
source .venv/bin/activate
uvicorn Scripts.backend:app --host 127.0.0.1 --port 8000 --reload &

# Start frontend
streamlit run app/app.py
```

#### Option 3: Quick Start Script

Create a `start.sh` file (macOS/Linux):
```bash
#!/bin/bash
source .venv/bin/activate
uvicorn Scripts.backend:app --host 127.0.0.1 --port 8000 --reload &
streamlit run app/app.py
```

Make it executable and run:
```bash
chmod +x start.sh
./start.sh
```

#### Access the Application

Once both servers are running:
- **Streamlit Frontend:** [http://localhost:8501](http://localhost:8501)
- **FastAPI Backend:** [http://127.0.0.1:8000](http://127.0.0.1:8000)
- **API Documentation:** [http://127.0.0.1:8000/docs](http://127.0.0.1:8000/docs)
- **Alternative API Docs:** [http://127.0.0.1:8000/redoc](http://127.0.0.1:8000/redoc)

---

### Troubleshooting Installation

<details>
<summary><strong>❌ "ModuleNotFoundError: No module named 'dotenv'"</strong></summary>

**Solution:**
```bash
pip install python-dotenv
```
</details>

<details>
<summary><strong>❌ "No module named 'langgraph'"</strong></summary>

**Solution:**
```bash
pip install langgraph google-generativeai
```
</details>

<details>
<summary><strong>❌ "Could not find a version that satisfies the requirement scipy==1.16.2"</strong></summary>

**Solution:** This means you're using Python 3.13. Use Python 3.10 instead:
```bash
# Recreate virtual environment with Python 3.10
rm -rf .venv
python3.10 -m venv .venv
source .venv/bin/activate
pip install -r requirement.txt
```
</details>

<details>
<summary><strong>❌ "Connection refused" when uploading files in Streamlit</strong></summary>

**Solution:** Make sure the FastAPI backend is running:
```bash
# In a separate terminal
uvicorn Scripts.backend:app --host 127.0.0.1 --port 8000 --reload
```
</details>

<details>
<summary><strong>❌ "GEMINI_API_KEY is missing"</strong></summary>

**Solution:** Create or update your `.env` file with a valid API key:
```env
GEMINI_API_KEY=your_actual_api_key_here
```
</details>

---

## 📡 API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| `POST` | `/cleandata/` | Upload and clean CSV/Excel files |
| `POST` | `/clean-db/` | Fetch and clean data from database |
| `POST` | `/clean-api/` | Fetch and clean data from external API |

### Example: Clean a CSV file

```bash
curl -X POST "http://localhost:8000/cleandata/" \
  -H "Content-Type: multipart/form-data" \
  -F "file=@data/sample_data.csv"
```

### Example: Clean data from database

```bash
curl -X POST "http://localhost:8000/clean-db/" \
  -H "Content-Type: application/json" \
  -d '{
    "db_url": "postgresql://user:password@localhost:5432/mydb",
    "query": "SELECT * FROM my_table"
  }'
```

---

## 🧹 Data Cleaning Operations

### Traditional Cleaning (`data_cleaning.py`)
- ✅ Handle missing values (mean, median, mode, drop)
- ✅ Remove duplicate rows
- ✅ Fix and convert data types
- ✅ Min-Max normalization

### AI-Powered Cleaning (`ai_agent.py`)
- 🤖 Intelligent missing value imputation
- 🤖 Context-aware data formatting
- 🤖 Anomaly detection and handling
- 🤖 Text standardization

---

## 🖼️ Screenshots

### Data Source Selection
The application supports three data input methods:
- 📁 Upload CSV/Excel files
- 🗄️ Query databases directly
- 🌐 Fetch from external APIs

### Cleaned Data Output
View the cleaned data in a beautiful, interactive table format with the ability to download the results.

---

## 🔧 Configuration

### Environment Variables

| Variable | Description | Required |
|----------|-------------|----------|
| `GEMINI_API_KEY` | Google Gemini API key | ✅ Yes |
| `MYSQL_HOST` | MySQL database host | ❌ Optional |
| `MYSQL_PORT` | MySQL database port | ❌ Optional |
| `MYSQL_USER` | MySQL username | ❌ Optional |
| `MYSQL_PASSWORD` | MySQL password | ❌ Optional |
| `MYSQL_DATABASE` | MySQL database name | ❌ Optional |

---

## 🧪 Testing

### Test Database Connections

```bash
# Test MySQL connection
python Scripts/test_mysql_connection.py

# Test PostgreSQL connection
python Scripts/test_postgre_connection.py
```

---

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

1. Fork the repository
2. Create your feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

---

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

---

## 🙏 Acknowledgments

- [Google Gemini AI](https://ai.google.dev/) for the powerful language model
- [LangGraph](https://github.com/langchain-ai/langgraph) for AI workflow orchestration
- [FastAPI](https://fastapi.tiangolo.com/) for the amazing web framework
- [Streamlit](https://streamlit.io/) for the beautiful UI components

---

<p align="center">
  Made with ❤️ for the Data Science Community
</p>  
