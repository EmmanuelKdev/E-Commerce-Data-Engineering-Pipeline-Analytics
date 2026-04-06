FROM astrocrpublic.azurecr.io/runtime:3.1-14


# Copy data files into the image 
COPY include/data/ /usr/local/airflow/data/