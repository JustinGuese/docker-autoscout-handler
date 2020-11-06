FROM python:3.8
RUN mkdir -p /data/input /data/output
COPY elasticToPandas.py /
COPY meanValCreator.py /
COPY execute.sh /
RUN chmod +x execute.sh
COPY ./requirements.txt /
RUN pip install -r requirements.txt
CMD ["./execute.sh"]

