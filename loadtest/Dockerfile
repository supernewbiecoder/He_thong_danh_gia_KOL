FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY loadgen.py .

RUN useradd --system --uid 10001 loadgen \
    && mkdir -p /app/results \
    && chown loadgen /app/results
USER loadgen

EXPOSE 8000
ENTRYPOINT ["python", "loadgen.py"]
CMD ["produce"]
