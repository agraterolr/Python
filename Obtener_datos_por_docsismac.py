#/usr/bin/python
# coding: utf-8

import csv
import datetime
from datetime import datetime
import sys
import os
import MySQLdb

#toma resultado del script meduxAuditoria.py  --- > ejemplo: Auditoria_W50_audit_result_2018-12-17_18:08:47.csv
file_1 = str(sys.argv[1])

file_audit = '/home/scripts/Data/auditoria/' + file_1  #archivo obligatorio
file_mac = '/home/scripts/Data/auditoria/medux_HFC.csv' #archivo obligatorio


file_out = '/home/scripts/Data/auditoria/docsismac_auditoria.csv'
file_out2 = '/home/scripts/Data/auditoria/docsismac_complementos.csv'


myfile = open(file_out,'w') #escribo la cabecera de columna de docsismac_auditoria.csv
with myfile:
	lineaUno = ['ip','timestamp(gmt)','docsis-mac','start-time','finish_time','sharing']
	writer = csv.writer(myfile)
	writer.writerow(lineaUno)


#leemos cada registro de Auditoria_W49_audit_result_2018-12-11_16:20:50.csv
#comparamos con medux_HFC.csv por fecha - hora para obtener las docsismac
#guardamos en docsismac_auditoria.csv
with open(file_audit,'r') as file_audit_rw:

	row_count_audit = 1

	for row_audit in file_audit_rw:

		if row_count_audit > 1: #no leer la primera fila de cabecera

			array_audit = row_audit.split(',')
			ip_audit = array_audit[3]
			fec_audit = array_audit[1]

			if(len(array_audit[27])>0): #valido que sharing tenga valor, sino le asigno NULL
				sharing_audit = array_audit[27]
			else:
				sharing_audit = 'NULL'
			if fec_audit is not None: #si el registro tiene valor en la fecha del timestamp continuo
			
				try: 
					array_date = fec_audit.split('T') #se separa el timestamp en dos
					fecha = array_date[0]
					hora = array_date[1]

					#se separa fecha
					fecha_array = fecha.split('-')
					ano = str(fecha_array[0])
					mes = str(fecha_array[1]).zfill(2)
					dia = str(fecha_array[2]).zfill(2)

					#se separa hora
					hora_part_temp = hora.split('.')
					hora_part = hora_part_temp[0]
					hora_array = hora_part.split(':')
					hora = str(hora_array[0])
					minuto = str(hora_array[1]).zfill(2)
					seg = str(hora_array[2]).zfill(2)

					#fecha-hora en formato a leer
					fec_audit_ajustado = ano + '-' + mes + '-' + dia + ' ' + hora + ':' + minuto + ':' + seg
					fec_audit_format = datetime.strptime(fec_audit_ajustado, '%Y-%m-%d %H:%M:%S')
					
				except:
					pass
						
				
				with open(file_mac,'r') as file_mac_r: # leemos medux_HFC.csv para comparar los registros

					row_count_mac = 1

					for row_mac in file_mac_r:

						if row_count_mac > 1: #para no leer la primera fila de cabecera
							try:
								array_mac = row_mac.split(',')
								ip_mac = array_mac[0]
								docsis_mac = array_mac[2]
								start_fecha = datetime.strptime(array_mac[3], '%Y-%m-%d %H:%M:%S')
								finish_fecha = datetime.strptime(array_mac[5], '%Y-%m-%d %H:%M:%S')

								if ip_audit == ip_mac and fec_audit_format >= start_fecha and fec_audit_format <= finish_fecha:
									
									print 'Ip: ' + str(ip_audit) + ' con docsismac: ' + str(docsis_mac) + '  ' + str(fec_audit_format)
									
									row = [ip_audit, fec_audit_format, docsis_mac, start_fecha, finish_fecha, sharing_audit] 
									
									myFile = open(file_out, 'a')
									with myFile:
										writer = csv.writer(myFile)
										writer.writerow(row)

							except:
								raise
							
						row_count_mac += 1
		row_count_audit += 1


#hacemos conexion con bbdd
host_connect = "10.0.68.8"
user_connect = "opt"
password = "Optbroadb2015"
instance = "OPTPERFORMANCE"
conexion = MySQLdb.connect(host = host_connect, user = user_connect, passwd = password, db = instance)
micursor = conexion.cursor()

# escribo encabezados de docsismac_complementos.csv
myfile = open(file_out2,'w')
with myfile:
	lineaUno = ['mac','ip','timestamp','crm_id','Date_From','Date_To','node_info','service','model','provincia','region','tecnologia']
	writer = csv.writer(myfile)
	writer.writerow(lineaUno)

# leemos docsismac_auditoria.csv para seguir buscando datos complementarios en bbdd...
myfile = open(file_out, 'r')
with myfile:
	row_count = 1
	for row_mac in myfile:
		if row_count > 1:
			try:
				#separo cada elemento del registro
				array_mac = row_mac.split(',')
				ip_mac = array_mac[0]
				timestamp_mac = array_mac[1]
				docsis_mac = array_mac[2]
				start_fecha = array_mac[3]
				finish_fecha = array_mac[4]
				sharing_mac = array_mac[5]

				timestamp_mac_file = datetime.strptime(timestamp_mac, '%Y-%m-%d %H:%M:%S')
				timestamp_mac_format= timestamp_mac_file.strftime("%Y-%m-%d")

				#busco por mac en OPTPERFORMANCE.Inventory_DOCSIS
				query = "SELECT mac, crm_id, Date_From, Date_To, node_info, service, model FROM OPTPERFORMANCE.Inventory_DOCSIS where mac = '" + str(docsis_mac) + "' and '" + str(timestamp_mac_format) + "' >= Date_From and '" + str(timestamp_mac_format) + "' <= Date_To;"
				micursor.execute(query)
				registro = micursor.fetchone()
				if registro is not None:

					crmid_mac = registro[1]
					datef_mac = registro[2]
					datet_mac = registro[3]
					nodo_mac = registro[4]
					service_mac = registro[5]
					model_mac = registro[6]

					sharing = sharing_mac
					buscar = nodo_mac.find("OLTZ") 
					if buscar <> -1:
						sharing = 'JAZZTEL'
					else:
						buscar = nodo_mac.find("OLT")
						if buscar <> -1:
							if nodo_mac.startswith("V"):
								sharing = 'VODAFONE'
							elif nodo_mac.startswith("O") and sharing_mac <> "jazztel":
								sharing = 'ORANGE'
					
					#busco por nodo en OPTPERFORMANCE.MAPEO_RED
					query = "SELECT DSLAM_OLT_SEC, PROVINCIA, REGION from MAPEO_RED where DSLAM_OLT_SEC in ('" + nodo_mac + "');"
					micursor.execute(query)
					registro = micursor.fetchone()
					if registro is not None:

						provincia_mac = registro[1]
						region_mac = registro[2]
					
					else:

						provincia_mac = 'No encontrado'
						region_mac = 'No encontrado'


				else:

					crmid_mac = 'No encontrado'
					datef_mac = 'No encontrado'
					datet_mac = 'No encontrado'
					nodo_mac = 'No encontrado'
					service_mac = 'No encontrado'
					model_mac = 'No encontrado'
					sharing = 'No encontrado'

					provincia_mac = 'No encontrado'
					region_mac = 'No encontrado'

				

				# todos con CRMid son tecnologia HFC
				if crmid_mac != 'No encontrado':
					tecnologia = 'HFC' 
				else:
					tecnologia = 'No encontrado'

				# construimos el registro de complementos
				row_write = [docsis_mac, ip_mac, timestamp_mac_format, crmid_mac, datef_mac, datet_mac, nodo_mac, service_mac, model_mac, provincia_mac, region_mac, tecnologia]
				
				# lo escribimos en docsismac_complementos.csv
				myFile = open(file_out2, 'a')
				with myFile:
					print row_write
					writer = csv.writer(myFile)
					writer.writerow(row_write)

			except:
				raise

		row_count += 1