###############################################################################
###                             CARGAR LIBRERIAS                            ###
###############################################################################
from configparser import ConfigParser
import pandas as pd
import pyodbc
import os
import os.path
import numpy as np
import logging
import pathlib
import datetime

###############################################################################
###                      CARGAR ARCHIVO INICIALIZACIÓN                      ###
###############################################################################
config = ConfigParser()
config.read('Config.ini')
STRSQLGP = config.get('Config', 'StringBDCEDI_GP')
STRSQLRESERVA = config.get('Config', 'StringBDRESERVA')
STRDB2 = config.get('Config', 'StringBDPK')
CantidadAire = int(config.get('Config', 'DiasDemanda'))
DiasDemanda = config.get('Config', 'DiasDemanda')
CurrentPath = pathlib.Path().absolute()

###############################################################################
###                                  VARIABLES                              ###
###############################################################################
Lista_de_categorias = ['BBFCO',  
'SF',        
'PESCO',     
'GEFCD',     
'GEFCO',    
'GEFSP',     
'PESCD',     
'PBCCO',     
'PBCCD',     
'GEFC1',     
'PBCC1',     
'BBFC1',     
'BBFCD',     
'GLXCO',     
'GLXC1',     
'GEFC2',     
'PBCC2',     
'BBFC2',     
'GLXC2',     
'BBFO1',     
'GEFO1',     
'GLXO1',     
'PBCO1',     
'BBFO2',     
'GEFO2',     
'GLXO2',     
'PBCO2',     
'PBCC3',     
'BBFC3',     
'GEFC3',     
'GLXC3']

###############################################################################
###                                  LOG ERROR                              ###
###############################################################################
MyFileError = 'Error/LogError.log'
MyFolderError = 'Error'
PathFolderError = os.path.join(CurrentPath, MyFolderError)
PathfileError = os.path.join(CurrentPath, MyFileError)
if not os.path.exists(PathFolderError):
    os.makedirs(PathFolderError)
    
logging.basicConfig(
    filename=PathfileError,
    filemode='a', #append
    # format='%(asctime)s,%(msecs)d %(name)s %(levelname)s %(message)s,%(lineno)d',
    datefmt='%m-%d-%Y %H:%M:%S', 
    level=logging.DEBUG)

# # Remove all handlers associated with the root logger object. Está rutina se ejecuta cuando no alamcena en logging
# for handler in logging.root.handlers[:]:
#     logging.root.removeHandler(handler)

###############################################################################
###                              BASE DE DATOS                              ###
###############################################################################
class DataBase:

    def __init__(self, strConection):
        self.strConection = strConection          
           
    def EjecutarQuery(self, query, consulta):
        try:
            self.consulta = consulta
            self.query = query
            self.cnxn = pyodbc.connect(self.strConection)
            self.cursor = self.cnxn.cursor()
            if self.consulta is False:
                self.cursor.execute(self.query)
                self.cnxn.commit()
                self.cnxn.close()
            else:
                self.DataFrame = pd.read_sql_query(self.query, self.cnxn)
                self.cnxn.close()
                return self.DataFrame
        except Exception as error:
            self.cnxn.close()
            logging.debug(error)
            logging.info(self.query)

### BASE DE DATOS PKMS ###
bd_pkms = DataBase(STRDB2)
### BASE DE DATOS WCS GP ###
bd_wcsgp = DataBase(STRSQLGP)
### BASE DE DATOS WCS GP ###
bd_wcsreserva = DataBase(STRSQLRESERVA)

###############################################################################
###                               QUERIES                                   ###
###############################################################################
q_tareas_reserva = ''' 
SELECT PSBRCD AS SKU,
PSMIS2 AS CATEGORIA,
CAST(SUM(PSQPUL/PSBXQT) AS INT) AS PAQUETESDEMANDA
FROM                 WM310BASD.PSPULL00
WHERE               PSPATY='' AND PSCASN<>'' AND PSDCR > CAST(TO_CHAR(CURRENT_DATE - #DiasDemanda# DAYS,'YYYYMMDD') AS INT)
GROUP BY          PSBRCD,PSMIS2
ORDER BY SUM(PSQPUL/PSBXQT) DESC
 '''
q_tareas_reserva = q_tareas_reserva.replace('#DiasDemanda#',DiasDemanda)

q_precios = '''
SELECT [SKU]
      ,[CategoriaFinal] AS CATEGORIA
      ,[Precio] AS PRECIOWCS
  FROM [GP].[dbo].[GPPreImpresion]
'''

q_inventario = '''
SELECT TRIM(IDCASN) Caja,
TRIM(IDZONE) ZONA,
TRIM(IDAISL) PASILLO,
TRIM(IDBAY) MODULO,
TRIM(IDLEVL) NIVEL,
TRIM(IDPOSN) POSICION,
TRIM(STBRCD) SKU, 
Cast(IDQTY/STBXQT as int) PAQUETES,
TRIM(IDMANI) CATEGORIA,
TRIM(IDCPDT) FECHAING,
IDNUM1 PRECIO
FROM
WM310BASD.idcase00,
WM310BASD.ststyl00
WHERE IDWHSE='01'
AND IDSTAT='30'
AND IDLKCD=' '
AND IDLKC2=' '
AND IDLKC3=' '
AND IDLKC4=' '
AND IDLKC5=' '
AND IDSTYL=STSTYL
AND IDCOLR=STCOLR
AND IDCSFX=STCSFX
AND IDSDIM=STSDIM
'''

q_deleteinventario = ''' DELETE FROM [Reserva].[dbo].[InventarioDemanda]  '''

q_bulkinventario = ''' BULK INSERT [Reserva].[dbo].[InventarioDemanda]
   FROM 'G:\Archivos Bulk\inventarioreubicacion.csv'
   WITH 
      (
         FIELDTERMINATOR ='\t',
         ROWTERMINATOR ='\n'
      ); '''

###############################################################################
###                              FUNCIONES                                  ###
###############################################################################

#NUEVA COLUMNA QUE CLASIFICA LOS NIVELES DE ALTA ROTACION
def f_nivel(row):
    if (row['NIVEL'] == '2A') or (row['NIVEL'] == '2B') or (row['NIVEL'] == '3A') or (row['NIVEL'] == '3B') :
        val = 'NIVEL_DOSYTRES'
    else:
        val = 'NIVEL_OTROS'
    return val

def f_filtro(row):
    if ((row['PRECIOWCS'] == np.nan) or (row['PRECIO'] == row['PRECIOWCS'])) and (row['TIPONIVEL'] == 'NIVEL_OTROS') and (row['PAQUETES'] > CantidadAire) and ((row['NIVEL_OTROS'] - row['NIVEL_DOSYTRES']) > 0) :
        val = 1
    else:
        val = 0
    return val

def borrar_archivos(pathfolder):
    import shutil
    shutil.rmtree(pathfolder)
        
###############################################################################
###                           WHILE PRINCIPAL                               ###
###############################################################################
     
try:
    
    df_demanda = pd.read_csv ('D:/Users/practti5/.spyder-py3/Trabajo/archivo_demanda.csv', sep='\t', lineterminator='\r', dtype=str) 
    
    df_precios = pd.read_csv ('D:/Users/practti5/.spyder-py3/Trabajo/archivo_precios.csv', sep='\t', lineterminator='\r', dtype=str) 
    
    df_inventario = pd.read_csv ('D:/Users/practti5/.spyder-py3/Trabajo/archivo_inventario.csv', sep='\t', dtype=str) 
    
    df_dda = pd.read_excel('D:/Users/practti5/.spyder-py3/Trabajo/DDA.xlsx')
    
    print(df_demanda.columns)
    print(df_precios.columns)
    print(df_inventario.columns)
    print(df_dda.columns)
    
    
    ###CONSULTA TAREAS DE PKMS###
    # df_demanda = bd_pkms.EjecutarQuery(q_tareas_reserva, True)
    df_demanda['CATEGORIA'] = df_demanda['CATEGORIA'].str.replace(' ','',)
    # #FILTRA CATEGORIAS#
    df_demanda = df_demanda[df_demanda['CATEGORIA'].isin(Lista_de_categorias)]    
    # ###CONSULTA PRECIOS DE WCS###
    # df_precios = bd_wcsgp.EjecutarQuery(q_precios, True)
    df_precios['CATEGORIA'] = df_precios['CATEGORIA'].str.replace(' ','',)
    df_precios['PRECIOWCS'] = df_precios['PRECIOWCS'].str.replace('%','').astype(np.float64)
    df_demanda = pd.merge(df_demanda, df_precios, on=['SKU','CATEGORIA'], how="left")
    
    # df_final2 = pd.merge(df_inventario, df_dda, on=['Material','Colores','Tallas'])
    # ###INVENTRIO NIVEL 2 Y 3 Y OTROS###
    # df_inventario = bd_pkms.EjecutarQuery(q_inventario, True)
    df_inventario['TIPONIVEL'] = df_inventario.apply(f_nivel, axis=1)
    df_inventario['CATEGORIA'] = df_inventario['CATEGORIA'].str.replace(' ','',)
    df_inventario2 = df_inventario.groupby(['SKU', 'CATEGORIA', 'TIPONIVEL'])['PAQUETES'].agg('sum')
    df_inventario2 = df_inventario2.to_frame()
    df_inventario2.reset_index(level=0, inplace=True)
    df_inventario2.reset_index(level=0, inplace=True)
    df_inventario2.reset_index(level=0, inplace=True)
    df_inventario3 = pd.pivot_table(df_inventario2, 
                                    values='PAQUETES', 
                                    index=['CATEGORIA','SKU'], 
                                    columns=['TIPONIVEL'], 
                                    aggfunc=np.sum)
    df_inventario3.reset_index(level=0, inplace=True)
    df_inventario3.reset_index(level=0, inplace=True)
    #INSERTA INVENTARIO POR CAT,SKU,NIVEL AL INVENTARIO DE CAJAS#
    df_inventario4 = pd.merge(df_inventario, df_inventario3, on=['SKU','CATEGORIA'], how="left")
    df_inventario4['Cod Material'] = df_inventario4['Cod Material'].str.replace('%','').astype(np.float64)
    df_inventario4['Cod Colores'] = df_inventario4['Cod Colores'].str.replace('%','').astype(np.float64)
    df_inventario4['Cod Tallas'] = df_inventario4['Cod Tallas'].str.replace('%','').astype(np.float64)

    # df_inventario4[['Cod Material','Cod Colores','Cod Tallas']].astype(float)
    df_final = pd.merge(df_inventario4, df_demanda, on=['SKU','CATEGORIA'], how="left")
    df_final['FILTRO'] = df_final.apply(f_filtro, axis=1)
    df_final['FECHAACT'] = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    df_final = pd.merge(df_inventario4, df_dda, on=['Cod Material','Cod Colores', 'Cod Tallas'])
    df_final.drop(['Caja', 'ZONA', 'PASILLO', 'MODULO'	, 'NIVEL', 'POSICION','SKU','PAQUETES','CATEGORIA'], axis = 'columns', inplace=True)
    df_final.drop(['Grupo','FECHAING','PRECIO','TIPONIVEL','Concatenado','Ean','Lead Time','Ciclo Color','Uso'], axis = 'columns', inplace=True)
    df_final.drop(['AAAA','Sector','Cód. Marca','Marca','Cód. Grupo','Cód. Subgrupo/Estilo','Subgrupo/Estilo'], axis = 'columns', inplace=True)
    df_final.drop(['Extensión Marca','Cód. Extensión Marca','Cód. Tipo Prenda','Tipo Prenda','Material','Tallas','Colores','Cod Tema','Tema','CONCA'], axis = 'columns', inplace=True)

    # df_final.drop(df_final.columns[[0,1,2,3,4,5,6,7,8,9]], axis='columns')
    ##INSERTA INFORMACIÓN EN BASE DE DATOS###
    Myfile1 = 'D:/Users/practti5/.spyder-py3'
    Myfile2 = 'inventarioreubicacion.csv'
    Pathfile = os.path.join(Myfile1, Myfile2)
    df_final.to_csv(Pathfile, sep='\t', index=False)
    # bd_wcsreserva.EjecutarQuery(q_deleteinventario, False)
    # bd_wcsreserva.EjecutarQuery(q_bulkinventario, False)
except Exception as error:
    print(error)
    logging.debug(error) 
