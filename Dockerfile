# Use the official Python image from the Docker Hub
FROM python:3.9-slim

# Set the working directory in the container
WORKDIR /app

# Add the application files into the container
ADD dimbenchmarks.py .
ADD dimclients.py .
ADD dimcurrecny.py .
ADD dimDates.py .
ADD dimDestination.py .
ADD dimoffers.py .
ADD dimregions.py .
ADD dimRequestType.py .
ADD lancement_dimcars.py .
ADD alimentation_fact.py .
ADD run_all.py .

# Install the dependencies
RUN pip install pandas psycopg2-binary pymongo requests openpyxl

# Command to run your master script
CMD ["python", "./run_all.py"]
