FROM osgeo/gdal

RUN apt-get update

RUN apt install -y python3-pip

WORKDIR .

COPY requirements.txt ./

RUN pip install -r requirements.txt

EXPOSE 8501

COPY ./images/ ./images/
COPY ./map_sources/ ./map_sources/
COPY ./main.py .
COPY ./funciones.py .
ENTRYPOINT ["streamlit", "run"]

CMD ["main.py"]
