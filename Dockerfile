############################################################
# Dockerfile para configurar aplicación en flask
############################################################

# Establece la imagen base
FROM python:3.6

# Información de Metadata
LABEL version="3.0"

#Se añade y seleciona el directorio de trabajo
ADD . /api_v1
WORKDIR /api_v1

# Instala los paquetes necesarios Flask
RUN pip3 install flask
RUN pip3 install pymongo
RUN pip3 install statsmodels 
RUN pip3 install pmdarima
RUN pip3 install requests

# Expone la aplicación en el puerto 3001
EXPOSE 3001
