# -*- coding: utf-8 -*-

from flask import Flask, jsonify, Response, render_template
from prediccion import prediccion_weatherstack
# import logging


app = Flask(__name__)

@app.route('/servicio/v2/prediccion/24horas/')
def prediccion_24():

	result = prediccion_weatherstack(24)

	return jsonify(result)


@app.route('/servicio/v2/prediccion/48horas/')
def prediccion_48():
	
	result = prediccion_weatherstack(48)

	return jsonify(result)

@app.route('/servicio/v2/prediccion/72horas/')
def prediccion_72():
	
	result = prediccion_weatherstack(72)

	return jsonify(result)

@app.route("/")
def index():
	return render_template('index_v2.html')
    # return Response("Segundo Microservicio utilizando la api weatherstack, status=200)


if __name__ == '__main__':
    app.run(host='0.0.0.0', port=3010, debug=True)
