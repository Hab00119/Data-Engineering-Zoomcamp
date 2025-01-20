FROM python:3.9

RUN pip install pandas

WORKDIR /app
COPY dataloader.py /app/dataloader.py

# Copy the requirements.txt file into the container
COPY requirements.txt /app/requirements.txt

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

ENTRYPOINT [ "python", "dataloader.py" ]