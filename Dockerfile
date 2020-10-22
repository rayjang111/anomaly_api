FROM python:3.8
WORKDIR /code
COPY requirements.txt . 
RUN pip install -r requirments.txt
COPY src/ .
CMD ["python","./anomaly_api.py"]
