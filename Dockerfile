FROM python
WORKDIR /app
COPY requirements.txt /app
RUN pip install --no-cache-dir -r requirements.txt
COPY miniproject2.py /app
EXPOSE 5454
CMD ["waitress-serve", "--listen=0.0.0.0:5454", "miniproject2:app"]