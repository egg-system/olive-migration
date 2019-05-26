FROM python:3
USER root

RUN apt-get update
RUN apt-get install -y vim less
RUN apt-get install -y zsh less
RUN pip install --upgrade pip
RUN pip install --upgrade setuptools
RUN pip install mysqlclient
RUN pip install pandas
RUN pip install phpserialize
RUN pip install mojimoji
RUN pip install jaconv
RUN pip install numpy

ENV LANG ja_JP.UTF-8
ENV LANGUAGE ja_JP:ja
ENV LC_ALL ja_JP.UTF-8
EXPOSE 8000
CMD ["bash"]
