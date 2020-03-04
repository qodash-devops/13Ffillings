FROM python:3.7-buster
COPY requirements.txt /crawler/requirements.txt
RUN pip install -r /crawler/requirements.txt
COPY . /crawler
WORKDIR /crawler/edgar
CMD scrapy crawl edgar