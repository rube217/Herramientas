# %%
from importlib.resources import path
import os, pandas as pd, xml.etree.ElementTree as ET, re, datetime, xmltodict, warnings
from tkinter.filedialog import askdirectory, askopenfilename
warnings.simplefilter(action='ignore', category=FutureWarning)
# %%
def GetConfiguration(df=pd.DataFrame()):
    try:
        if df.empty:
            query_file = "./Config/Queries.xml"
            with open(query_file,"r") as xmlFile:
                xmldict = xmltodict.parse(xmlFile.read())
            dict_table = {}
            for i in xmldict['tables']['table']:
                if i['Schema'] in dict_table:
                    dict_table[i['Schema']].update({i['id']:[i['Query'],i['Name']]})
                else:
                    dict_table.update({i['Schema']:{i['id']:[i['Query'],i['Name']]}})
            return dict_table
            
        else:
            x = list(df.loc[:,df.columns.str.contains('_x')].columns)
            y = list(df.loc[:,df.columns.str.contains('_y')].columns)
            normal = [i[:-2] for i in x]
            dictx = dict(zip(x,normal))
            dicty = dict(zip(y,normal))

            return dictx,dicty
    except ValueError:
        print(ValueError)


def GetDifferencesRTDB(conn_prod, conn_dev, table):
    
    dict_table = GetConfiguration()

    try:
        table_dev = pd.read_sql_query(dict_table['Scada_realtime'][table][0], conn_dev, coerce_float = False)
        table_prod = pd.read_sql_query(dict_table['Scada_realtime'][table][0], conn_prod, coerce_float = False)

        if (table_dev.columns == 'description').any():
            table_dev['description']=table_dev['description'].str.strip()
        if (table_prod.columns == 'description').any():
            table_prod['description']=table_prod['description'].str.strip()

        table_diff = pd.merge(table_dev,table_prod, on='Name' ,how='outer', indicator='Exist')
        dictx, dicty = GetConfiguration(table_diff)
        
        table_delete = table_diff.loc[table_diff['Exist']=='left_only',(table_diff.columns == 'Name') | (table_diff.columns.str.contains('_x'))].rename(columns = dictx)
        table_create = table_diff.loc[table_diff['Exist']=='right_only',(table_diff.columns == 'Name') | (table_diff.columns.str.contains('_y'))].rename(columns = dicty)
        
        table_update = table_diff.loc[table_diff['Exist']=='both',:].reset_index()
        x = table_update.loc[:,(table_update.columns == 'Name')|(table_update.columns.str.contains('_x'))].rename(columns=dictx) ## x es dev
        y = table_update.loc[:,(table_update.columns == 'Name')|(table_update.columns.str.contains('_y'))].rename(columns=dicty) ## y es prod
        if 'DMS Alias' in table_dev.columns:
            y['DMS Alias'] = x['DMS Alias']
        table_update = y.loc[~(x==y).all(1)]
               
        if not(table_update.empty & table_create.empty & table_delete.empty):
            destination_folder = askdirectory()
            with pd.ExcelWriter('{}\{}_{}.xls'.format(destination_folder,dict_table['Scada_realtime'][table][1],datetime.datetime.now().strftime('%Y%m%d_%H%M')),engine="xlwt") as writer:
                if not(table_update.empty & table_create.empty):
                    pd.concat([table_update,table_create]).to_excel(writer,sheet_name=dict_table['Scada_realtime'][table][1], index=False)
                if not(table_delete.empty):
                    table_delete.to_excel(writer,sheet_name='{}_delete'.format(dict_table['Scada_realtime'][table][1]), index=False)
                writer.save()
            print("Se ha generado con exito el archivo excel:{}/{}_{}.xls, por favor subirlo al ADE en Dev".format(destination_folder,dict_table['Scada_realtime'][table][1],datetime.datetime.now().strftime('%Y%m%d_%H%M')))
        else:
            print("No hay diferencias entre Dev y Prod")
        
    except KeyError:
        print("El valor {} no es permitido, los valores permitidos son: status, analog, rate, multistate, station, remote, connection".format(table))

# %%
def GetChangeset_Errors():
    try:
        file_root = askopenfilename()
        df = pd.read_csv(file_root,sep=',|;',error_bad_lines= False,skiprows=1,engine='python')
        df.Circuit = df.Circuit.str.replace('( [[A-Z]\w+])','')

        source = 'ChangeSet'
        return df,source
    except ValueError:
        print("Error", ValueError)

def GetSummaryReport():
    try:
        root = askdirectory()
        df = pd.DataFrame(columns=['Feeder','Error'])
        
        for i in os.listdir(root):
            if os.path.exists(root+'/'+i+'/SumariodeInformes.txt'):
                filename = 'SumariodeInformes.txt'
            else:
                filename = 'SummaryReport.txt'
            with open(root+'/'+i+'/'+filename,'r') as F:
                Lines = F.readlines()
            for Line in Lines:
                if 'ERROR' in Line:
                    #print('Iteracion ',str(n),i.split('_')[3],Line)
                    
                    df = df.append({'Feeder': i.split('_')[3], 'Error': Line.strip()},ignore_index= True)
        df.drop_duplicates().reset_index().drop(columns='index')
        source = 'Extract'
        return df,source
    except ValueError:
        print("ERROR",ValueError)

def GetSourceFile(FeederList):
    #for FeederCID in FeederList:
    if ' ' in FeederList:
        FeederList = FeederList.split(' ')[0]
    FileChoosen = ['',0]

    while os.path.isdir(r'\\10.241.115.13\Extract') == False: ## autenticacion para la carpeta compartida
        user = input("Por favor ingresar su usuario de desarrollo (sin el dominio, ejemplo: rdjaramillo)")
        password = input("Por favor ingresar su contraseña")
        os.system(r'net use \\10.241.115.13\Extract /user:"dev\{}" "{}"'.format(user,password))

    for root,dir,files in os.walk('//10.241.115.13/Extract'):
        for file in files:
            if FeederList in file:
                FileChoosen[1] = max(FileChoosen[1], os.path.getctime(root+'/'+file))
                FileChoosen[0] = root+'/'+file

    return(FileChoosen[0])
    
def GetElementID(path = '',Error_Mess = ''):
    try:
        root = ET.parse(path)
        connectivity_node = re.search('(-[\d]\w+.[\d]\w+.[\d]:CN)',Error_Mess)

        if connectivity_node != None:
            print(connectivity_node.group())
            for connection in root.findall("{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal"):
                if connectivity_node.group() in str(connection.find('{http://iec.ch/TC57/2010/CIM-schema-cim15#}Terminal.ConnectivityNode').attrib):
                    return('Revisar elemento:\t' + re.search('\d{15}',str(connection.attrib)).group() + '\tpara solucionar Error:\t' + Error_Mess)
    except ValueError:
        print(ValueError)