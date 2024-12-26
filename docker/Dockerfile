# Use an official Python image as the base
FROM python:3.11-slim

# Set the working directory in the container
WORKDIR /app

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the app's code into the container
COPY . .

# Expose the port for the application if needed (optional for testing)
# EXPOSE 8000

# Set the entry point for the container (optional)
CMD ["pytest", "tests"]