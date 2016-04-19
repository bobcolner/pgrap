FROM python:3.5
MAINTAINER Bob Colner <bcolner@gmail.com>
WORKDIR /dbrap
# ADD . /dbrap
RUN git clone https://github.com/bobcolner/dbrap.git
RUN pip install -r requirements.txt
