# KOL Dashboard API

Read-only FastAPI service for the existing PostgreSQL schema. It does not write to Kafka or PostgreSQL.

```bash
python -m venv .venv-backend
source .venv-backend/Scripts/activate
pip install -r backend/requirements.txt
export DATABASE_URL='postgresql://kol_readonly:PASSWORD@127.0.0.1:5432/kol'
uvicorn backend.app.main:app --reload --port 8000
```

Open `http://127.0.0.1:8000/docs` for the interactive API documentation. Dashboard endpoints are under `/api/v1/dashboard`.
