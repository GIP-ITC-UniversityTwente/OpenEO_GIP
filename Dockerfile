FROM python:3.10
WORKDIR /app

RUN pip install openeo
RUN pip install Flask
RUN pip install flask-restful
RUN pip install flask_cors
RUN pip install eoreader
RUN pip install pystac>=1.0.0
RUN pip install Flask-HTTPAuth
RUN pip install pynacl

COPY /ilwis /app/packages/

RUN apt-get update && apt-get install -y libqt5gui5
RUN apt-get update && apt-get install -y python3-pyqt5.qtwebengine
RUN apt-get update && apt-get install libqt5concurrent5
RUN pip install pyqt5
RUN apt-get update && apt-get install -y binutils libproj-dev gdal-bin
RUN apt-get install libgdal-dev -y
COPY . /app

EXPOSE 5000



CMD ["flask", "run", "--host=0.0.0.0", "--port", "5000"]
