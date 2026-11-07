FROM python:3.12.12-slim
WORKDIR /app
RUN pip install --no-cache-dir httpx==0.28.1 pymysql==1.1.1 redis==5.2.1 kafka-python==2.2.10 cryptography
COPY bridge.py ./
CMD ["python","-u","bridge.py"]
