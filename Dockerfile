FROM python:3.10

COPY requirements.txt .

RUN pip install -r requirements.txt

EXPOSE 5001

COPY . .

CMD ["hypercorn", "main:app", "--bind", "0.0.0.0:5001"]

