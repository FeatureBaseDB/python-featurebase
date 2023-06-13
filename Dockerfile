FROM python:3.8

WORKDIR /python-featurebase

COPY . .

RUN pip install -r requirements.txt

RUN python3 -m build --wheel

