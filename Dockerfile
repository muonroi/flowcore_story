FROM python:3.11-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY src/ ./src/

# Project này phụ thuộc vào flowcore (sẽ được mount qua volume khi dev hoặc cài qua pip)
ENV PYTHONPATH="/app/src:/app/libs/flowcore/src"

CMD ["python", "-m", "flowcore_story.apps.main"]
