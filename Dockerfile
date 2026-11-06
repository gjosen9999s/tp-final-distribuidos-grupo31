FROM python:3.12-slim

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
RUN python -m nltk.downloader -d /usr/local/share/nltk_data stopwords

COPY src/ /app/src/
COPY tools/analyzer.py /app/tools/analyzer.py
ENV PYTHONPATH=/app/src
ENV NLTK_DATA=/usr/local/share/nltk_data
WORKDIR /app/src

CMD ["python", "-m", "client.main"]
