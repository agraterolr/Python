#/usr/bin/python
# coding: utf-8

import csv
import datetime
from datetime import datetime
import sys
import os

file_audit = '/home/scripts/Data/auditoria/Auditoria_W48_audit_result_2018-12-04_10:46:55.csv'
file_nomenc = '/home/scripts/Data/auditoria/nomenclatura_despliegue.csv'
file_out = '/home/scripts/Data/auditoria/despliegue_Wxx.csv'

with open(file_out,'w') as file_out:
	lineaUno = "id_sonda,operador,acceso,sharing,modelo,region,universo,univ_sharing,serviceid,velocidad_contratada,global,provincia"
	writer = csv.writer(file_out)
	writer.writerow(lineaUno)

with open(file_audit,'r') as file_audit_rw:
	row_count_audit = 1
	for row_audit in file_audit_rw:
		if row_count_audit > 1:
			array_audit = row_audit.split(',')

            idsonda_audit = array_audit[0]
			operador_audit = array_audit[4]
            acceso_audit = array_audit[5]
            sharing_audit = array_audit[6]
            modelo_audit = array_audit[7]
            region_audit = array_audit[8]
            universo_audit = array_audit[9]
            univsharing_audit = array_audit[10]
            serviceid_audit = array_audit[11]
            velocidad_audit = array_audit[12]
            global_audit = array_audit[13]
            provincia=audit = array_audit[14]


			
			if operador_audit == 'vodafone':

                serviceid_audit = array_audit[20]
                crmid_audit = array_audit[22]

                if len(serviceid_audit) == 0 or serviceid_audit == 'NV':
                    
                    serviceid_audit = crmid_audit


                modelo_audit = array_audit[23]
                tecnologia_audit = array_audit[26]
                provincia_audit = array_audit[27]
                region_audit = array_audit[29]
                sharing_audit = array_audit[28]
                velocidad_audit = array_audit[26]



						

				with open(file_nomenc,'r') as file_nomenc_r:
					row_count = 1
					for row_nomenc in file_nomenc_r:
						if row_count > 1:
							try:
								array_nomenc = row_nomenc.split(',')

								modelo_nomenc = array_nomenc[1]
								tech_nomenc = array_nomenc[2]
                                universo_nomenc = array_nomenc[4]
                                acceso_nomenc = array_nomenc[6]

                                if len(sharing_audit) > 0:

                                    univsharing_nomenc = universo_nomenc + '-' + sharing_audit.lower()
                                else:
                                    univsharing_nomenc = ''

								if modelo_audit == modelo_nomenc and tecnologia_audit == tech_nomenc:

									row = [idsonda_audit, operador_audit, acceso_nomenc, sharing_audit, modelo_audit, region_audit, universo_nomenc, univsharing_nomenc, serviceid_audit, velocidad_audit, global_audit, provincia_audit] 
									myFile = open(file_out, 'a')
									with myFile:
										writer = csv.writer(myFile)
										writer.writerow(row)

                                else: 

                                    row = [idsonda_audit, 'revisar']
                                    myFile = open(file_out, 'a')
									with myFile:
										writer = csv.writer(myFile)
										writer.writerow(row)

							except:
								raise
                        
                        row_count += 1

            else:

                row = [idsonda_audit, operador_audit, acceso_audit, sharing_audit, modelo_audit, region_audit, universo_audit, univsharing_audit, serviceid_audit, velocidad_audit, global_audit, provincia_audit] 

				myFile = open(file_out, 'a')
				with myFile:
					writer = csv.writer(myFile)
					writer.writerow(row)
							
						
		row_count_audit += 1

		