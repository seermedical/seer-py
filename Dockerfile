FROM python:3.6

WORKDIR /app

COPY requirements.txt /app
COPY setup.py /app
COPY README.md /app
RUN pip install --no-cache-dir -r requirements.txt

COPY requirements_test.txt /app
RUN pip install --no-cache-dir -r requirements_test.txt

COPY . /app

CMD ["pytest"]
