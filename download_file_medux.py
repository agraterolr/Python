#!/usr/bin/python
# coding: utf-8


# Programa principal que permite orquestar módulo << utils_file >> 

import MySQLdb
import datetime
from time import time
import utils_file
from datetime import datetime

start_time = time()

#asignación de fecha del procesado
fecha_update = datetime.now()


#asignación de variables de conexion
host_connect = "10.0.68.8"
user_connect = "opt"
password = "Optbroadb2015"
instance = "medux"
#instance = "medux_prueba"

#asignación de variables de directorios
path_origin = "/home/scripts/Data/medux/Exports_VDF_ES_"
path_data_zl = "/home/scripts/Data/medux/zhilabs"
path_extraction = "/home/scripts/Data/medux/"

#abrir conexion a bbdd
conexion = utils_file.open_bd(host_connect, user_connect, password, instance)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "youtube"
table = "youtube"
program = "N/A"
fields_table = "_id,campana,configuracion_prueba,program,day,devModem,hours,youtube.id_sonda,minutes,month,status,technology,youtube.year,youtube_test_resultados_finales_avg_download,youtube_test_resultados_finales_buffer_tiempo_video,youtube_test_resultados_finales_carga_player,youtube_test_resultados_finales_inicio_play,youtube_test_resultados_finales_stalls,youtube_test_resultados_globales_prueba_finalizada,youtube_test_resultados_finales_max_stall_duration,youtube_test_resultados_finales_avergae_video_resolution"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_yt = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "dropbox"
table = "dropbox"
program = "N/A"
fields_table = "_id,campana,day,program,devModem,dropbox_test_get_resultados_finales_tiempo_ms,dropbox_test_get_resultados_finales_vel_avg,dropbox_test_get_resultados_globales_prueba_finalizada,hours,dropbox.id_sonda,minutes,month,status,technology,dropbox.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_db = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "gaming"
table = "gaming"
program = "N/A"
fields_table = "_id,campana,program,day,configuracion_prueba_ping,devModem,gaming_test_limpito_finales_porcentaje_paquetes_perdidos,gaming_test_resultados_finales_tiempo_avg,gaming_test_resultados_finales_tiempo_mdev,gaming_test_resultados_finales_tiempo_ms,gaming_test_resultados_globales_prueba_finalizada,gaming_test_target_ping,hours,gaming.id_sonda,minutes,month,status,technology,gaming.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_ga = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "wbt"
table = "wbt"
program = "N/A"
fields_table = "_id,campana,confess_chrome_limpito_finales_dataLength,confess_chrome_limpito_finales_load_time,confess_chrome_limpito_finales_navegacion_completada,confess_chrome_limpito_finales_numResources,confess_chrome_limpito_finales_totalResourcesTime,confess_chrome_limpito_finales_totalByType_min_loadTime,configuracion_prueba_web,program,day,devModem,hours,wbt.id_sonda,minutes,month,status,technology,wbt.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_wb = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "ping"
table = "ping"
program = "N/A"
fields_table = "_id,campana,configuracion_prueba_ping,day,devModem,hours,ping.id_sonda,program,minutes,month,ping_test_limpito_finales_porcentaje_paquetes_perdidos,ping_test_resultados_finales_porcentaje_completo,ping_test_resultados_finales_tiempo_avg,ping_test_resultados_finales_tiempo_mdev,ping_test_resultados_finales_tiempo_ms,ping_test_resultados_globales_prueba_finalizada,status,technology,ping.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_pi = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "http_down"
table = "http_down"
program = "http-down-test"
fields_table = "_id,campana,configuracion_prueba_tamano,configuracion_prueba_url,program,day,devModem,hours,http_down_test_resultados_finales_avg_download,http_down_test_resultados_finales_tiempo_descarga_ms,http_down_test_resultados_globales_download_finalizado,http_down_test_resultados_globales_porcentaje_completado,http_down.id_sonda,minutes,month,status,technology,http_down.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_hd = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "http_down_ftw"
table = "http_down"
program = "http-down-burst-test"
fields_table = "_id,campana,configuracion_prueba_tamano,configuracion_prueba_url,program,day,devModem,hours,http_down_test_resultados_finales_avg_download,http_down_test_resultados_finales_tiempo_descarga_ms,http_down_test_resultados_globales_download_finalizado,http_down_test_resultados_globales_porcentaje_completado,http_down.id_sonda,minutes,month,status,technology,http_down.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_hdf = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "http_up"
table = "http_up"
program = "http-upload-test"
fields_table = "_id,campana,configuracion_prueba_tamano,day,devModem,program,hours,http_upload_test_resultados_finales_avg_upload,http_upload_test_resultados_globales_upload_finalizado,http_upload_test_HTTP_server,http_up.id_sonda,minutes,month,status,technology,http_up.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_hu = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "http_up_ftw"
table = "http_up"
program = "http-upload-burst-test"
fields_table = "_id,campana,configuracion_prueba_tamano,day,devModem,program,hours,http_upload_test_resultados_finales_avg_upload,http_upload_test_resultados_globales_upload_finalizado,http_upload_test_HTTP_server,http_up.id_sonda,minutes,month,status,technology,http_up.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_huf = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "speedtest"
table = "speedtest"
program = "testspeed-test"
fields_table = "_id,campana,day,devModem,program,hours,speedtest.id_sonda,minutes,month,status,technology,testspeed_test_resultados_finales_avg_download,testspeed_test_resultados_finales_avg_upload,testspeed_test_resultados_globales_destino,testspeed_test_resultados_globales_destino_ciudad,testspeed_test_resultados_globales_distancia,testspeed_test_resultados_globales_download_finalizado,testspeed_test_resultados_globales_ping,testspeed_test_resultados_globales_upload_finalizado,testspeed_test_resultados_accomplishmentDownload,testspeed_test_resultados_accomplishmentUpload,speedtest.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_st = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)


#asignación nombre del test, tabla y campos de la tabla en el orden del fichero .csv
test_type = "ooklaspeedtest"
table = "speedtest"
program = "testspeed-ookla"
fields_table = "_id,campana,day,devModem,program,hours,speedtest.id_sonda,minutes,month,status,technology,testspeed_test_resultados_finales_avg_download,testspeed_test_resultados_finales_avg_upload,testspeed_test_resultados_globales_destino,testspeed_test_resultados_globales_destino_ciudad,testspeed_test_resultados_globales_download_finalizado,testspeed_test_resultados_globales_ping,testspeed_test_resultados_globales_upload_finalizado,testspeed_test_resultados_accomplishmentDownload,testspeed_test_resultados_accomplishmentUpload,speedtest.year"
print ('\n ------Procesando -----> ' + test_type)
day_int = utils_file.num_days(conexion,test_type,fecha_update)
msg_ost = utils_file.main_process(fecha_update, conexion, path_origin, path_data_zl, path_extraction, test_type, table, fields_table, day_int, program)



#cerrar conexion a bbdd
utils_file.close_bd(conexion)


#construcción y envío de email
print ('\n ------Procesando envío de correo -----')
utils_file.mail(msg_yt, msg_db, msg_ga, msg_wb, msg_hu, msg_hd, msg_pi, msg_st, msg_ost, msg_huf, msg_hdf)


elapsed_time = time() - start_time
print("\n ------Tiempo Estimado del proceso: %0.2f seconds." % elapsed_time)









