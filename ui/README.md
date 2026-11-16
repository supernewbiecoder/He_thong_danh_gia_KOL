# KOL Streamlit dashboard

The dashboard is read-only and consumes the FastAPI service.

```bash
python -m venv .venv-ui
source .venv-ui/Scripts/activate
pip install -r ui/requirements.txt
export KOL_API_URL='http://127.0.0.1:8000'
streamlit run ui/app.py
```

Open `http://localhost:8501`.
