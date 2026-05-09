# Use an official lightweight Python image
FROM python:3.10-slim

# Set the working directory in the container
WORKDIR /app

# System dependencies (required for some math/C++ libraries in Pandas/SciPy)
RUN apt-get update && apt-get install -y \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy the requirements file into the container
COPY requirements.txt .

# Install Python dependencies
RUN pip install --no-cache-dir -r requirements.txt

# Copy the rest of the application code
COPY . .

# Expose the port Streamlit runs on (we use 8505 based on your previous commands)
EXPOSE 8505

# Command to run the Streamlit dashboard
CMD ["streamlit", "run", "src/dashboard.py", "--server.port=8505", "--server.address=0.0.0.0"]
