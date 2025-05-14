# Use an official Python runtime as a parent image
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements.txt file into the container at /app
COPY ./model/requirements.txt /app/

# Copy everything from the current directory into /app
COPY ./model /app/

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

# Run classification.py when the container launches
CMD ["python", "/app/classification.py"]