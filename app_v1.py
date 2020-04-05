# -*- coding: utf-8 -*-

from flask import Flask, jsonify, Response, render_template
from prediccion import prediccion_arima
# import logging


app = Flask(__name__)

@app.route('/servicio/v1/prediccion/24horas/')
def prediccion_24():

	result = prediccion_arima(24)

	return jsonify(result)


@app.route('/servicio/v1/prediccion/48horas/')
def prediccion_48():
	
	result = prediccion_arima(48)

	return jsonify(result)

@app.route('/servicio/v1/prediccion/72horas/')
def prediccion_72():
	
	result = prediccion_arima(72)

	return jsonify(result)

@app.route("/")
def index():
	return render_template('index.html')
    # return Response("Microservicio para predecir la temperatura y la humedad con ARIMA", status=200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3001, debug=True)
