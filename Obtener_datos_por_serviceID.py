#/usr/bin/python
# coding: utf-8

import csv
import datetime
from datetime import datetime
import sys
import os
import MySQLdb

host_connect = "10.0.68.8"
user_connect = "opt"
password = "Optbroadb2015"
instance = "OPTPERFORMANCE"
conexion = MySQLdb.connect(host = host_connect, user = user_connect, passwd = password, db = instance)
micursor = conexion.cursor()

file_1 = str(sys.argv[1])

file_audit = '/home/scripts/Data/auditoria/' + file_1 #archivo obligatorio
file_out = '/home/scripts/Data/auditoria/serviceid_complementos.csv'


#escribo los encabezados en serviceid_complementos.csv
myfile = open(file_out, 'w') 
with myfile:
	row1 = ['ServiceID','Provincia','Region','NodoRed','Throughput','ServiceType','Template','Sharing','CPE','Sw_Version']
	writer = csv.writer(myfile)
	writer.writerow(row1)


#leemos el archivo de auditoria Auditoria_W49_audit_result_2018-12-11_12:15:30.csv
# comparamos cada registro con la bbdd para obtener datos adicionales en serviceid_complementos.csv
with open(file_audit,'r') as file_audit_rw: 

	row_count_audit = 1

	for row_audit in file_audit_rw:

		if row_count_audit > 1: #no leer la primera linea de encabezados

			array_audit = row_audit.split(',')
			ip_audit = array_audit[3]
			fec_audit = array_audit[1]
			sharing_audit = array_audit[27]
			serv_id_audit = array_audit[11]

			if len(serv_id_audit)!=0 and serv_id_audit !='NV': 

				try:
					array_date = fec_audit.split('T') #divido la cadena para obtener fecha y hora por separado
					fecha = array_date[0]
					hora = array_date[1]

					fecha_array = fecha.split('-') #divido el cadena fecha para obtener dia. mes, año
					ano = str(fecha_array[0])
					mes = str(fecha_array[1]).zfill(2)
					dia = str(fecha_array[2]).zfill(2)

					hora_part_temp = hora.split('.') #divido la cadena hora para obtener hora, minuto, segundo
					hora_part = hora_part_temp[0]
					hora_array = hora_part.split(':')
					hora = str(hora_array[0]).zfill(2)  
					minuto = str(hora_array[1]).zfill(2) 
					seg = str(hora_array[2]).zfill(2)

					fec_audit_ajustado = ano + '-' + mes + '-' + dia  # ejemplo: 2018-09-03
					#fec_audit_format = datetime.strptime(fec_audit_ajustado, '%Y-%m-%')
					
				except:
					pass
						
				#busco datos de router por serviceid
				query = "SELECT sid, lastcontacttime, tipo_router, sw_version from HDM.HDM_Fotored where sid in ('" + str(serv_id_audit) + "') order by lastcontacttime DESC;"
				micursor.execute(query)
				registro = micursor.fetchone()
				if registro is not None:

					cpe_sid = registro[2]
					swversion_sid = registro[3]
				
				else:

					cpe_sid = 'No encontrado'
					swversion_sid = 'No encontrado'

				#busco demás datos por serviceid
				query = "SELECT ServiceID, Provincia, NodoRed, Throughput, ServiceType, Template FROM OPTPERFORMANCE.Inventory_Incremental where ServiceID = '" + str(serv_id_audit) + "' order by Date_To DESC;"
				micursor.execute(query)
				registro = micursor.fetchone()
				if registro is not None:

					provincia_sid = registro[1]
					throughput_sid = registro[3]
					servtype_sid = registro[4]
					template_sid = registro[5]
					nodo_sid = registro[2]
					sharing = sharing_audit

					buscar = nodo_sid.find("OLTZ") 
					if buscar <> -1:
						sharing = 'JAZZTEL'
					else:
						buscar = nodo_sid.find("OLT")
						if buscar <> -1:
							if nodo_sid.startswith("V"):
								sharing = 'VODAFONE'
							elif nodo_sid.startswith("O") and sharing_audit <> "jazztel":
								sharing = 'ORANGE'

					#busco datos de region por provincia
					query = "SELECT PROVINCIA, REGION from MAPEO_RED where PROVINCIA in ( '" + str(provincia_sid) + "');"
					micursor.execute(query)
					registro = micursor.fetchone()
					if registro is not None:
						
						region_sid = registro[1]

					else:

						region_sid = "No encontrado"
				
				else:

					provincia_sid = "No encontrado"
					throughput_sid = "No encontrado"
					servtype_sid = "No encontrado"
					template_sid = "No encontrado"
					nodo_sid = "No encontrado"
					sharing = "No encontrado"

					region_sid = "No encontrado"



				# construimos el registro de complementos
				row_write = [serv_id_audit, provincia_sid, region_sid, nodo_sid, throughput_sid, servtype_sid, template_sid, sharing, cpe_sid, swversion_sid]

				# lo escribimos en serviceid_complementos.csv
				myFile = open(file_out, 'a')
				with myFile:
					print (row_write)
					writer = csv.writer(myFile)
					writer.writerow(row_write)

		row_count_audit += 1

		