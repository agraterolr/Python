#!/usr/bin/python
# coding: utf-8

import MySQLdb
from time import time
import sys

fec1 = sys.argv[1]
fec2 = sys.argv[2]

if fec1 == '' or fec2 == '':
	print "Faltan parámetros---> Formato: yyyy-mm-dd"
else:

	start_time = time()

	conexion = MySQLdb.connect(host="10.0.68.8", user="opt", passwd="Optbroadb2015", db="OPTPERFORMANCE")

	micursor = conexion.cursor()
	micursor3 = conexion.cursor()

	query = "SELECT test_id, client_ip_id, test_date FROM Ookla_Stnet WHERE test_date between '" + fec1 +" 00:00:00' and '" + fec2 +" 23:59:59' ORDER BY test_id"

	micursor.execute(query)

	count = micursor.rowcount

	print "Conexion abierta en la BBDD"
	print "Procesando " + str(count) + " Registros en OPTPERFORMANCE.Ookla_Stnet_IP_Encryp"
	
	count1 = 0
	registro = micursor.fetchone()
	j = 0
	campo = ''
	while j < count:
		n1 = ''
		n2 = ''
		n3 = ''
		campo = registro[1]
		avance = 1
		ippos = 1
		i = 0
		Largo = len(campo)
		while i < Largo:
			caracter = campo[i]
			if avance == 1 and caracter != '.':
				n1 = caracter
			elif avance == 2 and caracter != '.':	        	
				n2 = caracter
			elif avance == 3 and caracter != '.':	        	
				n3 = caracter
		
			if caracter == '.':
				if ippos == 1:
					Ip1 = n1 + n2 + n3
				elif ippos == 2:
					Ip2 = n1 + n2 + n3
				elif ippos == 3:
					Ip3 = n1 + n2 + n3

				n1 = ''
				n2 = ''
				n3 = ''
				avance = 0
				ippos += 1
		
			elif i == Largo and ippos == 4:
				Ip4 = n1 + n2 + n3
				n1 = ''
				n2 = ''
				n3 = ''
				break

			avance += 1
			i += 1


		vtest_id = registro [0]
		vtest_date = str(registro [2])
		query = "INSERT INTO Ookla_Stnet_IP_Encryp ( test_id, client_ip_id_encryp, client_ip_id, test_date ) VALUES ('" + vtest_id + "','" + Ip1 + "." + Ip2 + "." + Ip3 + ".xxx'" + ",'" + campo + "','" + vtest_date + "')"
		micursor3.execute(query)
		conexion.commit()
		count1 += 1
		registro = micursor.fetchone()	
		j += 1

	micursor.close
	micursor3.close

	elapsed_time = time() - start_time
	print(" Tiempo Estimado: %0.10f seconds." % elapsed_time)
	print "***Ingresados " + str(count1) + " Registros entre las fechas del " + fec1 + " al " + fec2
	print "Cerrada la bbdd"  
