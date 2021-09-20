FROM python:3.9.7

RUN pip install paho-mqtt requests

COPY ./app /app

CMD ["python", "/app/main.py"]