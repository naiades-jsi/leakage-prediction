# BUILDING: docker build -t <container_name> .
# RUNNING: docker run-d --network="host" <container_name>
FROM ubuntu:20.04
RUN apt-get update -y && \
    apt-get install -y python3-pip python3-dev
COPY ./requirements.txt /requirements.txt
WORKDIR /
RUN pip3 install -r requirements.txt
COPY . /

# e3ailab/braila_leakage_accurate_ircai
CMD ["python3", "main.py"]