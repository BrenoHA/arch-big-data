# Set the base image 
FROM python:3.11-alpine

# dont write pyc files
ENV PYTHONDONTWRITEBYTECODE 1
# dont buffer to stdout/stderr
ENV PYTHONUNBUFFERED 1

# Sets the user name to use when running the image.
USER root
RUN apk update
RUN apk add --no-cache bash

# Make a directory for our app
WORKDIR /publisher

# Install dependencies
COPY requirements.txt .
RUN pip install -r requirements.txt

# Copy source code
COPY ./app ./app

# Run the application
CMD ["bash"]