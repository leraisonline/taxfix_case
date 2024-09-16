FROM python:3.12-slim

WORKDIR /app

COPY . /app

RUN pip3 install --no-cache-dir -r requirements.txt

RUN mkdir -p /app/reports

RUN pytest .

CMD ["sh", "-c", "python src/data_processor.py && python src/report_generator.py"]
