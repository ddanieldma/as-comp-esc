FROM python:3.12-slim

WORKDIR /app

# Creating a non-root user
RUN useradd -ms /bin/bash appuser

# Adding user's binary dir to system PATH
# Adiciona o diretório de binários do usuário ao PATH do sistema
ENV PATH="/home/appuser/.local/bin:${PATH}"

COPY requirements.txt .

# Create virtual environment and install dependencies
RUN pip install uv
RUN uv pip install --system --no-cache-dir -r requirements.txt

COPY . .
RUN chown -R appuser:appuser /app

USER appuser