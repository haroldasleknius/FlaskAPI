FROM python

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1

WORKDIR /app

COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt

COPY miniproject2.py db.py generators.py /app

EXPOSE 5454
CMD ["waitress-serve", "--listen=0.0.0.0:5454", "miniproject2:app"]