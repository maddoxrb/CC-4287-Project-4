FROM python-base

WORKDIR /app

COPY requirements.txt ./
RUN pip install flask
RUN pip install --no-cache-dir -r requirements.txt -f https://download.pytorch.org/whl/torch_stable.html
RUN pip install --no-cache-dir -r requirements.txt
COPY . .

EXPOSE 5000

# Runs ML Flask Server Indefinitely
CMD ["python", "ml_server.py"]