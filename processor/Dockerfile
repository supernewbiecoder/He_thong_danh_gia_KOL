FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . /app/processor

RUN useradd --system --uid 10002 processor
USER processor

EXPOSE 8000
ENTRYPOINT ["python", "-m", "processor.processor"]
