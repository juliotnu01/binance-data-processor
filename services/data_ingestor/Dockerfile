FROM python:3.9-slim

WORKDIR /app

# Instalar dependencias del sistema
RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copiar requirements e instalar dependencias de Python
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt --timeout 100 --retries 5

# Copiar código de la aplicación
COPY . .

# Crear estructura de paquetes
RUN python -c "import app"

CMD ["python", "app.py"]