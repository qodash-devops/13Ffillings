FROM python:3.7-buster
COPY requirements.txt /crawler/requirements.txt
RUN pip install -r /crawler/requirements.txt
ADD . /crawler
WORKDIR /crawler
CMD /crawler/run.sh