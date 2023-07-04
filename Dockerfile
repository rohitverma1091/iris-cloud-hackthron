FROM ubuntu:18.04

RUN apt-get update && apt-get install -y unzip python3-pip unixodbc-dev libaio1 g++ wget libc6 python3-setuptools curl apt-transport-https
# RUN curl https://packages.microsoft.com/keys/microsoft.asc | apt-key add -
# RUN curl https://packages.microsoft.com/config/ubuntu/18.04/prod.list | tee /etc/apt/sources.list.d/msprod.list
# RUN apt-get update
# ENV ACCEPT_EULA=y DEBIAN_FRONTEND=noninteractive
# RUN apt-get install mssql-tools unixodbc-dev -y

RUN mkdir /app && adduser --home /app app
WORKDIR /app
COPY requirements.txt .
RUN pip3 install --upgrade pip && pip3 install --no-cache-dir -r requirements.txt
COPY app.py /app/
COPY testdata /app/testdata/
COPY output /app/output/
RUN ls -lrth
ENTRYPOINT [ "python3","/app/app.py" ]