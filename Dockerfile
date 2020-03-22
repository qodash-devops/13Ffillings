FROM python:3.7-buster
COPY requirements.txt /crawler/requirements.txt
RUN pip install -r /crawler/requirements.txt
ADD . /crawler
ENV PYTHONPATH=/crawler
WORKDIR /crawler/edgar
CMD tail -f /dev/null