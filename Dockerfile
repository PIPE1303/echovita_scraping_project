# Astronomer-style Dockerfile for Echovita pipeline
# Astro Runtime includes Airflow; we add Scrapy, DuckDB, and project code.
FROM astrocrpublic.azurecr.io/astronomer/astro-runtime:13.5.1

# Install Python dependencies (Airflow is already in the base image)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy project (dags, scraper, scripts, data, config)
COPY . .
