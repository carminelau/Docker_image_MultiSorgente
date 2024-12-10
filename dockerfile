# Usa un'immagine di base Python ufficiale
FROM python:3.11-slim

# Imposta la directory di lavoro
WORKDIR /app

# Copia solo il file requirements.txt, se disponibile
COPY requirements.txt .

# Installa le dipendenze specificate in requirements.txt (se presente)
RUN python -m pip install -r requirements.txt

EXPOSE 5000

# Comando di default: avvia una shell Python interattiva
CMD ["python"]