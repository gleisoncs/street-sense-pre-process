FROM python:3.11-slim

# gdal-bin provides ogr2ogr / ogrinfo for loading into PostGIS.
RUN apt-get update && apt-get install -y --no-install-recommends \
    gdal-bin \
    && rm -rf /var/lib/apt/lists/*

RUN pip install --no-cache-dir osmnx

WORKDIR /app
COPY import_region.py .

ENTRYPOINT ["python", "import_region.py"]
