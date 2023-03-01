FROM python:3.9

WORKDIR /app

COPY requirements.txt .
COPY invoices.json .
COPY contracts.json .

RUN pip install --no-cache-dir -r requirements.txt

COPY . .

CMD [ "python", "app.py" ]