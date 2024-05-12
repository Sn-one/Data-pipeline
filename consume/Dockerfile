# Dockerfile in consume directory
FROM python:3.8-slim

# Set the working directory in the container
WORKDIR /usr/src/app

# Copy the current directory contents into the container at /usr/src/app
COPY . .

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Define the command to run your app using CMD which makes your app start when the container is run
CMD ["python", "kafka_consumer.py"]
