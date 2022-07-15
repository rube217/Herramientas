from Functions import GetChangeset_Errors, GetConfiguration, GetDifferencesRTDB, GetElementID, GetSourceFile, GetSummaryReport
import datetime, os, sqlalchemy

def main():
    conn_prod = sqlalchemy.create_engine('mssql+pyodbc://Epsareportes:cmXoasys2@10.241.114.12\OASYSHDB:20010/ADMS_QueryEngine?driver=SQL Server')
    conn_dev = sqlalchemy.create_engine('mssql+pyodbc://epsareportes:Epsa.2020!@10.241.109.41\OASYSHDB:20010/ADMS_QueryEngine?driver=SQL Server')

    option:str = input("""
    Que deseas hacer:

    1. Errores Import/Export 
    2. Alinear RTDB
    3. Comparar DMS_RT Dev-PROD (new coming feature!!)
    """)

    if option == '1':   
        option_import_export = input(""""Procesar:
        1. Changesets rechazados
        2. Extractos Invalidos\n""")

        if option_import_export == '1':
            print("Por favor ingresar archivo .csv, resultado de Summary Report")
            df,source = GetChangeset_Errors()
                
            with open('ProcessedErrors_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M')),'w+') as file:
                previous_circuit = ''
                for i,x in df.iterrows():
                    
                    if x.Circuit != previous_circuit:
                        source_file = GetSourceFile(str(x.Circuit).strip())
                        previous_circuit = x.Circuit
                    
                    y = GetElementID(source_file,x.FileContent)       
                    if y != None:
                        file.write(str(y)+'\n')       
            file.close()
            print('Se ha finalizado la ejecución, el archivo se encuentra en {}'.format(os.getcwd()))

        elif option_import_export == '2':
            print("Por favor ingresar carpeta que contenga las carpetas resultado de Summary Report")
            df,source = GetSummaryReport()
            with open('ProcessedErrors_{}.csv'.format(datetime.datetime.now().strftime('%Y%m%d_%H%M')),'w+') as file:
                previous_circuit = ''
                for i,x in df.iterrows():

                    if x.Feeder != previous_circuit:
                        source_file = GetSourceFile(str(x.Feeder))
                        previous_circuit = x.Feeder

                    y = GetElementID(source_file,x.Error)       
                    if y != None:
                        file.write(str(y)+'\n')
                           
            file.close()
            print('Se ha finalizado la ejecución, el archivo se encuentra en {}'.format(os.getcwd()))

    elif option == '2':
        menu_rtdb = GetConfiguration()
        option_rtdb:str = '¿Que tabla deseas importar?\n'
        for i in menu_rtdb['Scada_realtime']:
            option_rtdb = option_rtdb+'{}. {}\n'.format(i,menu_rtdb['Scada_realtime'][i][1])
        GetDifferencesRTDB(conn_prod, conn_dev,input(option_rtdb))

# %%
if __name__ == '__main__':
    main()