FROM osgeo/gdal

RUN apt-get update

RUN apt install -y python3-pip

WORKDIR .

COPY requirements.txt ./

RUN pip install -r requirements.txt

EXPOSE 8501

COPY ./images/covid-19-updates-banner-a.png .
COPY ./images/galileo.png .
COPY ./map_sources/world-countries.json .
COPY ./main.py .
COPY ./funciones.py .
ENTRYPOINT ["streamlit", "run"]

CMD ["main.py"]
