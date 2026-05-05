# Radar-Api

Radar-Api is a Python-based Flask API for radar and weather data processing. The project uses scientific Python packages such as `numpy`, `pyproj`, `metpy`, `cfgrib`, and `xarray`, and it is intended to run inside a Miniconda-managed environment.

## Requirements

Before getting started, install the following:

- [Miniconda](https://www.anaconda.com/docs/getting-started/miniconda/main)
- Python 3.11 support through Conda
- Git

Optional for containerized setup:

- Docker

## Clone the repository

```bash
git clone https://github.com/ryderm-wx/Radar-Api.git
cd Radar-Api
```

## Local setup with Miniconda

### 1. Create the Conda environment

This project’s Dockerfile creates an environment named `radar` with Python 3.11, so the same setup is recommended locally:

```bash
conda create -n radar python=3.11 -y
```

### 2. Activate the environment

```bash
conda activate radar
```

### 3. Install Python dependencies

```bash
pip install --no-cache-dir -r requirements.txt
```

The current Python dependencies are:

- flask
- flask-cors
- flask_compress
- gunicorn
- numpy
- pyproj
- metpy
- cfgrib
- xarray

## Notes about `cfgrib` / eccodes

This project depends on `cfgrib`, which usually requires ECMWF ecCodes system libraries.

The provided Docker setup installs:

- `libeccodes-dev`
- `build-essential`
- `gcc`
- `g++`

If you are running locally and `cfgrib` fails to install or import, install ecCodes on your machine first.

### Ubuntu/Debian example

```bash
sudo apt-get update
sudo apt-get install -y build-essential gcc g++ libeccodes-dev
```

If you're on macOS or Windows, install ecCodes using the package manager appropriate for your platform before reinstalling Python dependencies.

## Run the API locally

This repository exposes the Flask app from `app.py` as `app`.

### Option 1: Run with Gunicorn

```bash
gunicorn -w 2 -b 0.0.0.0:8080 app:app
```

### Option 2: Run with Flask development tooling

If you want to try a development-style run:

```bash
export FLASK_APP=app.py
flask run --host=0.0.0.0 --port=8080
```

On Windows PowerShell:

```powershell
$env:FLASK_APP="app.py"
flask run --host=0.0.0.0 --port=8080
```

## Run with Docker

A Dockerfile is included and already uses Miniconda.

### Build the image

```bash
docker build -t radar-api .
```

### Run the container

```bash
docker run --rm -p 8080:8080 radar-api
```

## Project structure

```text
.
├── app.py
├── nexrad/
├── nexrad_level2.py
├── requirements.txt
└── Dockerfile
```

## Troubleshooting

### Conda command not found

Make sure Miniconda is installed and available in your shell PATH. Restart your terminal after installation if needed.

### `cfgrib` import errors

This usually means ecCodes is missing from your system. Install the required native libraries, then reinstall dependencies:

```bash
pip install --no-cache-dir -r requirements.txt
```

### Port 8080 already in use

Run the server on a different port, for example:

```bash
gunicorn -w 2 -b 0.0.0.0:5000 app:app
```

## Development notes

- The Dockerfile uses a Conda environment named `radar`
- The application is served with Gunicorn on port `8080`
- The Flask app entry point is:

```python
app:app
```
