FROM continuumio/miniconda3

WORKDIR /app

# Install system deps for cfgrib / eccodes
RUN apt-get update && apt-get install -y \
    build-essential \
    gcc \
    g++ \
    libeccodes-dev \
    && rm -rf /var/lib/apt/lists/*

# Create conda environment
RUN conda create -n radar python=3.11 -y

SHELL ["conda", "run", "-n", "radar", "/bin/bash", "-c"]

# Install Python deps inside conda env
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy app
COPY . .

EXPOSE 8080

CMD ["conda", "run", "--no-capture-output", "-n", "radar", "gunicorn", "-w", "2", "-b", "0.0.0.0:8080", "app:app"]