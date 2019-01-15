#!/usr/bin/python
# coding: utf-8

import MySQLdb
import datetime
from datetime import timedelta
from datetime import datetime
from time import time
import sys
import os
import warnings
import zipfile
import csv
from email.mime.text import MIMEText
from smtplib import SMTP
from subprocess import call


def open_bd(host_connect, user_connect, password, instance):
	conexion = MySQLdb.connect(host = host_connect, user = user_connect, passwd = password, db = instance)
	print ('\n Conexion abierta a BBDD: ' + instance)
	return conexion

def close_bd(conexion):
	conexion.close()
	print ('\n Cerrada conexión a BBDD')


# Método Mail: Para construir el mail de acuerdo a los resultados obtenidos del proceso
def mail(msg_yt, msg_db, msg_ga, msg_wb, msg_hu, msg_hd, msg_pi, msg_st, msg_ost, msg_huf, msg_hdf):

	try:

		from_address = "zhilabs_vf_fixed@vodafone.es"

		to_address = "Angel G. (Vodafone) <agrater@corp.vodafone.es>"
		cc_address = "Angel G. (NAE) <agrar@nae.es>"
		#to_address = "Alicia F. <alicia.fernandez1@vodafone.com>, Marta T. <marta.torres@vodafone.com>, Justo B. <justo.banos@vodafone.com>, Angel G. (Vodafone) <agrater@corp.vodafone.es>"
		#message = '	-  ' + str(msg_yt) + '\n' + '	-  ' + str(msg_db) + '\n' + '	-  ' + str(msg_ga)
		message =  'Hola buen día, \n \n' +\
						'Un resumen del procesamiento de ficheros de hoy: \n \n' +\
						'	-  ' + str(msg_yt) + '\n' + '	-  ' + str(msg_db) + '\n' +\
						'	-  ' + str(msg_ga) + '\n' +  '	-  ' + str(msg_hd)  + '\n' +\
						'	-  ' + str(msg_hu) + '\n' + '	-  ' + str(msg_pi) + '\n' +\
						'	-  ' + str(msg_st) + '\n' + '	-  ' + str(msg_ost) + '\n' +\
						'	-  ' + str(msg_wb) + '\n' + '	-  ' + str(msg_huf) + '\n' +\
						'	-  ' + str(msg_hdf) + '\n \n' +\
						'El envío del fichero matriz a Zhilabs fue: <<<proceso en construcción>> '
				
		#message = '	-  ' + str(msg_yt) + '\n' + '\n' + '	-  ' + str(msg_db) + '\n' + '\n' + '	-  ' + str(msg_ga) + '\n' + '\n' + '	-  ' + str(msg_hd)  + '\n' + '\n' + '	-  ' + str(msg_hu) + '\n' + '\n' + '	-  ' + str(msg_pi) + '\n' + '\n' +  '	-  ' + str(msg_st) + '\n' + '\n' + '	-  ' + str(msg_ost) + '\n' + '\n' + '	-  ' + str(msg_wb) + '\n' + '\n' + '	-  ' + str(msg_huf) + '\n' + '\n' + '	-  ' + str(msg_hdf)

		mime_message = MIMEText(message, "plain", _charset="utf-8")

		mime_message["From"] = from_address

		mime_message["To"] = to_address

		mime_message["Cc"] = cc_address

		mime_message["Subject"] = "[Zhilabs - Bench] Resumen de carga.-"
		
		smtp = SMTP("fixlegsftpserver")
		#smtp = SMTP("217.130.24.55: 25")
		smtp.login("zhilabs_vf_fixed", "VF_2017FixedZhilabs")
		
		smtp.sendmail(from_address, to_address, mime_message.as_string())

		smtp.quit()

		print ("Mensaje Enviado")

	except: 

		print ("Atención: no se ha podido enviar el mensaje")




# Método num_days: Para obtener el número de días desde la última actualización, según la traza.
def num_days(conexion, test_type, fecha_update):

	micursor = conexion.cursor()

	query = "SELECT * FROM file_trace WHERE file_name = '" + test_type + "'"
	micursor.execute(query)

	registro = micursor.fetchone()

	fecha_registro = registro[1]
	fecha_server = fecha_update 

	fecha_min = str(fecha_registro.day) +'/'+ str(fecha_registro.month) +'/'+ str(fecha_registro.year)
	fecha_max = str(fecha_server.day) +'/'+ str(fecha_server.month) +'/'+ str(fecha_server.year)

	day_count = (datetime.strptime(fecha_max, '%d/%m/%Y') - datetime.strptime(fecha_min, '%d/%m/%Y'))- timedelta(days=1)

	day_str = str(day_count)
	
	if day_str == '0:00:00':
		day_str = '00'
	day_int = int(day_str[:2])

	return day_int



# Método process_file: procesar el fichero, cargar en bbdd y exportar a .csv con datos complementarios
def process_file(path_file_csv, path_data_zl, table, test_type, fecha_format, fecha, fields_table, wk, conexion, fecha_out_file, program):

	warnings.filterwarnings("ignore")

	micursor = conexion.cursor()
	cod_process_file = 0

	try:

		path_file_csv_zl = path_data_zl + "/" + test_type + "_" + fecha_out_file + ".csv"

		
		if program != 'N/A':

			query = "SELECT count(*) FROM " + table + " WHERE " + table + ".day = '" + str(fecha.day).zfill(2) + "' and " + table + ".month = '" + str(fecha.month).zfill(2) + "' and " + table + ".year = '" + str(fecha.year) + "' and week = " + str(wk) + " and program = '" + str(program) + "'"

		else:

			query = "SELECT count(*) FROM " + table + " WHERE " + table + ".day = '" + str(fecha.day).zfill(2) + "' and " + table + ".month = '" + str(fecha.month).zfill(2) + "' and " + table + ".year = '" + str(fecha.year) + "' and week = " + str(wk)

		micursor.execute(query)
		cont = micursor.fetchone()

		quotechar = '"'

		if cont[0] == 0:
			
			query = "LOAD DATA LOCAL INFILE '" + path_file_csv + "' INTO TABLE " + table + " FIELDS TERMINATED BY ',' ENCLOSED BY '" + quotechar + "' LINES TERMINATED BY '\n' IGNORE 1 LINES (" + fields_table + ") SET week = " + str(wk)
			num_inserts = micursor.execute(query)
			conexion.commit()

			query = "UPDATE file_trace SET update_last = '" + str(fecha) + "' where file_name = '" + test_type + "';"
			micursor.execute(query)
			conexion.commit()

			print (" " + str(num_inserts) + " registros insertados en " + table + ", OK, semana " + str(wk) + ", de fecha " + str(fecha_format))
			cod_process_file = 1

		else:

			if program != 'N/A':

				query = "DELETE FROM " + table + " WHERE " + table + ".day = '" + str(fecha.day).zfill(2) + "' and " + table + ".month = '" + str(fecha.month).zfill(2) + "' and " + table + ".year = '" + str(fecha.year) + "' and week = " + str(wk) + " and program = '" + str(program) + "'"

			else:

				query = "DELETE FROM " + table + " WHERE " + table + ".day = '" + str(fecha.day).zfill(2) + "' and " + table + ".month = '" + str(fecha.month).zfill(2) + "' and " + table + ".year = '" + str(fecha.year) + "' and week = " + str(wk)
				
			micursor.execute(query)
			conexion.commit()


			query = "LOAD DATA LOCAL INFILE '" + path_file_csv + "' INTO TABLE " + table + " FIELDS TERMINATED BY ',' ENCLOSED BY '" + quotechar + "' LINES TERMINATED BY '\n' IGNORE 1 LINES (" + fields_table + ") SET week = " + str(wk)
			num_inserts = micursor.execute(query)
			conexion.commit()

			query = "UPDATE file_trace SET update_last = '" + str(fecha) + "' where file_name = '" + test_type + "';"
			micursor.execute(query)
			conexion.commit()

			print (" " + str(num_inserts) + " registros re-insertados en " + table + ", OK, semana " + str(wk) + ", de fecha " + str(fecha_format))

			cod_process_file = 2

		
		if program != 'N/A':
			
			query = "SELECT T1.*, T2.serviceid, T2.universo, T2.modelo, T2.operador, T2.region, T2.provincia FROM " + table + " T1 LEFT JOIN complemento_test T2 ON T1.id_sonda = T2.id_sonda WHERE T1.day = '" + str(fecha.day).zfill(2) + "' and T1.month = '" + str(fecha.month).zfill(2) + "' and T1.year = '" + str(fecha.year) + "'" + " and T1.program = '" + str(program) + "'"

		else:

			query = "SELECT T1.*, T2.serviceid, T2.universo, T2.modelo, T2.operador, T2.region, T2.provincia FROM " + table + " T1 LEFT JOIN complemento_test T2 ON T1.id_sonda = T2.id_sonda WHERE T1.day = '" + str(fecha.day).zfill(2) + "' and T1.month = '" + str(fecha.month).zfill(2) + "' and T1.year = '" + str(fecha.year) + "'"


		micursor.execute(query)

		registros = micursor.fetchall()

		num_registros = len(registros)

		header = [i[0] for i in micursor.description]

		if os.path.exists(path_file_csv_zl):

			outputcsv = open(path_file_csv_zl, 'a')
			writercsv = csv.writer(outputcsv, delimiter=',', lineterminator='\n', quoting = csv.QUOTE_MINIMAL)
			#writercsv.writerow(header) # no incluyo las cabeceras de columna
			writercsv.writerows(registros)
			outputcsv.close()

			query = "UPDATE file_trace SET export_last = '" + str(fecha) + "' where file_name = '" + test_type + "';"
			micursor.execute(query)
			conexion.commit()

			print (" Exportacion de: " + str(num_registros) + " registros, en " + test_type + ", OK"+ ", fecha " + str(fecha_format))

			cod_process_file = 3

		else:

			outputcsv = open(path_file_csv_zl, 'a')
			writercsv = csv.writer(outputcsv, delimiter=',', lineterminator='\n', quoting = csv.QUOTE_MINIMAL)
			writercsv.writerow(header)
			writercsv.writerows(registros)
			outputcsv.close()

			query = "UPDATE file_trace SET export_last = '" + str(fecha) + "' where file_name = '" + test_type + "';"
			micursor.execute(query)
			conexion.commit()

			print (" Exportacion de: " + str(num_registros) + " registros, en " + test_type + ", OK"+ ", fecha " + str(fecha_format))

			cod_process_file = 4
	except:

		print (" Procesamiento de: " + test_type + " no fue completado")
		raise

	micursor.close()

	return cod_process_file



# Método main_process : para verificar si el proceso ya fue ejecutado, si los .zip a procesar existen, si los .csv existen y tienen información.
# también es usado para invocar el método: process_file
def main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program):

	list_mensaje_proccess_file = []

	micursor = conexion.cursor()

	if day_int <= 0:

		print (' Archivo: ' + test_type + ' ya fue procesado')
		list_mensaje_proccess_file.append(' Archivo: ' + test_type + ' ya fue procesado')

	else:

		while day_int > 0:

			cod_proccess_file = 0
			status = 0
			fecha = fecha_update - timedelta(days=day_int)
			fecha_out = fecha_update - timedelta(days=1)

			wk = fecha.isocalendar()[1]

			fecha_format = str(fecha.year) + str(fecha.month).zfill(2) + str(fecha.day).zfill(2)
			fecha_out_file = str(fecha_out.year) + str(fecha_out.month).zfill(2) + str(fecha_out.day).zfill(2)

			path_data = path_origin + str(fecha_format)
			path_zipfile = path_data + ".zip"
			path_file_csv = path_data + "/" + test_type + "_" + fecha_format + ".csv" 

			if os.path.exists(path_file_csv):

				if os.stat(path_file_csv).st_size > 0:

					#procesamiento de ficheros
					cod_proccess_file = process_file(path_file_csv,path_data_zl,table,test_type,fecha_format,fecha,fields_table,wk,conexion,fecha_out_file,program)
					
					if cod_proccess_file == 0:

						mensaje_process_file = test_type + "_" + fecha_format + "-NOK, proceso fallido (proccess_file function)"
				
				else:

					status = 1
			else:

				status = 1 

			if status == 1:
				#Buscar  y traer zipfile remote del dia':
				#cmd_findcopy = 'scp vdf_es_local@vdf-sftp.caseonit.com:/vdf_es_local/Exports_VDF_ES_20190101.zip /home/scripts/Data/medux/'
				#cmd_findcopy = 'scp vdf_es_local@195.201.132.3:/vdf_es_local/Exports_VDF_ES_20190101.zip /home/scripts/Data/medux/'
				#call(cmd_findcopy)

								
				if os.path.exists(path_zipfile):   

					zf = zipfile.ZipFile(path_zipfile, "r")

					for i in zf.namelist():

						zf.extract(i, path = path_extraction)

					print ("Finish unzip - " + fecha_format)

					if os.path.exists(path_file_csv):

						if os.stat(path_file_csv).st_size > 0:

							#procesamiento de ficheros
							cod_proccess_file = process_file(path_file_csv,path_data_zl,table,test_type,fecha_format,fecha,fields_table,wk,conexion,fecha_out_file,program)
							
							if cod_proccess_file == 0:

								mensaje_process_file = test_type + "_" + fecha_format + "-NOK,proceso fallido (proccess_file function)"
						else:

							print ("Archivo:  " + test_type + "_" + fecha_format + ".csv vacio. Verificar")
							mensaje_process_file = test_type + "_" + fecha_format + ".csv vacio. Verificar"

					else:

						print ("Archivo: " + test_type + "_" + fecha_format + ".csv no existe en el servidor origen. Verificar: nombre del fichero y carpeta del día")
						mensaje_process_file = test_type + "_" + fecha_format + ".csv no existe en el servidor origen. Verificar: nombre del fichero y carpeta del día"

				else:

					print ("Fichero .zip del " + fecha_format + " no encontrado, Import NOK")

					mensaje_process_file = "Fichero .zip del " + fecha_format + " no encontrado, Import NOK"


			if cod_proccess_file >= 3:

				mensaje_process_file = test_type + "_" + fecha_format + "-OK"

			elif cod_proccess_file == 2 or cod_proccess_file == 1:

				mensaje_process_file = test_type + "_" + fecha_format + "-NOK, no se exporto al .csv (proccess_file function " + str(cod_process_file) + ")"
		

			list_mensaje_proccess_file.append(mensaje_process_file)

			day_int -= 1

	return list_mensaje_proccess_file

'''

def master_file(path_file_csv_zl, path_file_csv_master, test_type, registros):# <<<<<<en construccion>>>>>>

	file_out = open(path_file_csv_master,'w') #cabecera de columna
	with file_out:
		lineaUno = ['_id','campana','day','devModem','program','hours','id_sonda','minutes','month','status','technology','year','week','configuracion_prueba_tamano','configuracion_prueba_url','configuracion_prueba_ping','configuracion_prueba_web','configuracion_prueba','testspeed_test_resultados_finales_avg_download','testspeed_test_resultados_finales_avg_upload','testspeed_test_resultados_globales_destino','testspeed_test_resultados_globales_destino_ciudad','testspeed_test_resultados_globales_download_finalizado','testspeed_test_resultados_globales_ping','testspeed_test_resultados_globales_upload_finalizado','testspeed_test_resultados_accomplishmentDownload','testspeed_test_resultados_accomplishmentUpload','testspeed_test_resultados_globales_distancia','http_upload_test_resultados_finales_avg_upload','http_upload_test_resultados_globales_upload_finalizado','http_upload_test_HTTP_server','http_down_test_resultados_finales_avg_download','http_down_test_resultados_finales_tiempo_descarga_ms','http_down_test_resultados_globales_download_finalizado','http_down_test_resultados_globales_porcentaje_completado','ping_test_limpito_finales_porcentaje_paquetes_perdidos','ping_test_resultados_finales_porcentaje_completo','ping_test_resultados_finales_tiempo_avg','ping_test_resultados_finales_tiempo_mdev','ping_test_resultados_finales_tiempo_ms','ping_test_resultados_globales_prueba_finalizada','confess_chrome_limpito_finales_dataLength','confess_chrome_limpito_finales_load_time','confess_chrome_limpito_finales_navegacion_completada','confess_chrome_limpito_finales_numResources','confess_chrome_limpito_finales_totalResourcesTime','confess_chrome_limpito_finales_totalByType_min_loadTime','gaming_test_limpito_finales_porcentaje_paquetes_perdidos','gaming_test_resultados_finales_tiempo_avg','gaming_test_resultados_finales_tiempo_mdev','gaming_test_resultados_finales_tiempo_ms','gaming_test_resultados_globales_prueba_finalizada','gaming_test_target_ping','dropbox_test_get_resultados_finales_tiempo_ms','dropbox_test_get_resultados_finales_vel_avg','dropbox_test_get_resultados_globales_prueba_finalizada','youtube_test_resultados_finales_avg_download','youtube_test_resultados_finales_buffer_tiempo_video','youtube_test_resultados_finales_carga_player','youtube_test_resultados_finales_inicio_play','youtube_test_resultados_finales_stalls','youtube_test_resultados_globales_prueba_finalizada','youtube_test_resultados_finales_max_stall_duration','youtube_test_resultados_finales_avergae_video_resolution','serviceid','universo','operador','modelo','region','provincia']
		writer = csv.writer(myfile)
		writer.writerow(lineaUno)

	file_read = open(path_file_csv_zl, 'r') 

	with file__read:

		row_count = 1

		for row_read in file_read:

			if row_count > 1:

				registro = row_read.split(',')
				_id = registro[0]
				campana = registro[1]
				day = registro[2]
				devModem = 


			row_count += 1

	#cmd_sendcopy = 'scp /home/scripts/Data/medux/20190101_Medux.csv.gz sbroadband@10.18.130.18:/var/sbroadband/files/Benchmark/Medux/'
	#call(cmd_sendcopy)

'''
	