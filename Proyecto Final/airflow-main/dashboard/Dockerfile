FROM python:3.8.2

WORKDIR .

RUN apt-get install libgdal-dev

RUN apt-get update && apt-get install -y binutils libproj-dev gdal-bin

COPY ./requirements.txt ./requirements.txt

RUN pip install -r requirements.txt

EXPOSE 8501

COPY ./images/ ./images/
COPY ./map_sources/ ./map_sources/
COPY ./main.py .
COPY ./funciones.py .
ENTRYPOINT ["streamlit", "run"]

CMD ["main.py"]