FROM ghcr.io/opensafely-core/base-docker

RUN apt-get update --fix-missing

RUN apt-get install -y python3.8 python3-pip git docker.io sqlite3
RUN update-alternatives --install /usr/bin/python python /usr/bin/python3.8 1

# Install pip requirements
COPY requirements.txt /tmp/
RUN python -m pip install --requirement /tmp/requirements.txt

RUN mkdir /app
COPY . /app
# Build *.pyc files
RUN python -m compileall /app
WORKDIR /app

CMD python
