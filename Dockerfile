FROM python3


FROM python:3

COPY . /app/

RUN pip install -r /app/requirements.txt

WORKDIR /app

EXPOSE 5000

CMD ["gunicorn", "app:app", "-c", "./gunicorn.conf.py"]