FROM python:3.12-slim

ARG VERSION=dev
LABEL version=${VERSION}

WORKDIR /app

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    VERSION=${VERSION}

COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

COPY VERSION ./
COPY lib ./lib
COPY tracker ./tracker

EXPOSE 8000
CMD ["uvicorn", "tracker.app:app", "--host", "0.0.0.0", "--port", "8000"]
