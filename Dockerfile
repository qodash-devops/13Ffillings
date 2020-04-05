FROM python:3.7-buster
COPY requirements.txt /crawler/requirements.txt
RUN pip install -r /crawler/requirements.txt
ADD . /crawler
RUN mv /crawler/edgar/edgar/proxy_list_docker.txt /crawler/edgar/edgar/proxy_list.txt
ENV PYTHONPATH=/crawler
WORKDIR /crawler/edgar
CMD tail -f /dev/null