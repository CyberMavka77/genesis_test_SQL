FROM python:3.10

RUN pip install pandas 
RUN pip install psycopg2

COPY ./subscription1.csv /opt/app/subscription1.csv
COPY ./read_file.py /opt/app/read_file.py
COPY ./DDL.sql /opt/app/DDL.sql

ENTRYPOINT ["python", "/opt/app/read_file.py"]
