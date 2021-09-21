FROM python:3.9.7

COPY ./app /app

RUN pip install -r /app/requirement.txt

CMD ["python", "/app/main.py"]