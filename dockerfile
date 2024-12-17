# Usa un'immagine di base Python ufficiale
FROM python:3.11-slim

# Imposta la directory di lavoro
WORKDIR /app

# Copia il file requirements.txt (se disponibile)
COPY requirements.txt . 

# Copia il file utils.py nella directory /app
COPY utils.py .

# Copia il file databases.py nella directory /app
COPY databases.py .

# Copia il file config.py nella directory /app
COPY config.py .

# Copia il file Geobap.py nella directory /app
COPY Geobap.py .

# Installa le dipendenze specificate in requirements.txt
RUN python -m pip install --no-cache-dir -r requirements.txt

# Espone la porta 5000
EXPOSE 5000

# Comando di default: avvia una shell Python interattiva
CMD ["python"]