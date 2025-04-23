#------------------------------------------------------------------------------------------------
# SubProcess:
#           Corte_Fichero_SubProcess_Slave:
#               Ejecución de todos los hilos SubProcess.
# Parámetros:
#           1: Número de Hilo[n] SubProcess / N partes del File.txt
#           2: Fichero de Entrada "FileN.txt" 
#           3: Oracle Tabla Destino
#           4: Oracle DNS sTv.BIACT_ora_dns
#           5: Oracle UID sTv.BIACT_ora_uid
#           6: Oracle PWD sTv.BIACT_ora_pwd
# Librerías:
#           Master:
#               cfg: Parámetros comunes para el Master y Slave
#               subprocess: Orquestador de SubProcess
#               os: Interacción con el S.O. Windows
#               datetime: Señales de tiempo
#           Slave:
#               pandas: Gestión analítica del fichero .txt en un DataFrame
#               pyodbc: Inserción de datos del DataFrame en Oracle
#               datetime: Señales de tiempo
#               sys: Interactuar con los argumentos recibidos
#                                                                          By --> SteveCarpio2024
#------------------------------------------------------------------------------------------------

from   cfg.BIACTIVOS_librerias_s import *

#print(f"Parametro0:  {sys.argv[0]}")
#print(f"Parametro1:  {sys.argv[1]}")
#print(f"Parametro2:  {sys.argv[2]}")
#print(f"Parametro3:  {sys.argv[3]}")
#print(f"Parametro4:  {sys.argv[4]}")
#print(f"Parametro5:  {sys.argv[5]}")
#print(f"Parametro6:  {sys.argv[6]}")
#print(f"Parametro6:  {sys.argv[7]}")

#if __name__ == "__main__":
if len(sys.argv) == 8:  # 8
    PAR0 = sys.argv[0]         # Nombre del fichero de ejecución
    PAR1 = sys.argv[1]         # Número de Hilo
    PAR2 = sys.argv[2]         # Tabla de Entrada "FileN" sin extensión '.txt'
    PAR3 = sys.argv[3]         # Oracle Tabla
    PAR4 = sys.argv[4]         # Oracle DNS sTv.BIACT_ora_dns
    PAR5 = sys.argv[5]         # Oracle UID sTv.BIACT_ora_uid
    PAR6 = sys.argv[6]         # Oracle PWD sTv.BIACT_ora_pwd
    PAR7 = sys.argv[7]         # PERIODO AAAAMM

    if len(PAR1) == 1:
        PAR11=f"0{PAR1}" 
    else:
        PAR11=f"{PAR1}"

    ### ----------------------------------- Funciones -----------------------------------

    # 1 - def Oracle_Establece_Conexión(par_dsn , par_uid , par_pwd) 
    def Oracle_Establece_Conexion(par_dsn, par_uid,par_pwd):
        try:
            # Cadena de conexión a la base de datos Oracle
            connection_string = f'DSN={par_dsn};UID={par_uid};PWD={par_pwd};'
            # Establecer la conexión y un cursor a la base de datos Oracle
            conexion = pyodbc.connect(connection_string)
            cursor = conexion.cursor()
        # cursor.fast_executemany = True  <--- es para SQL Server aquí no me vale
            print(f"    Hilo[{PAR11}] - {dt.now().time()} - Conexión establecida.... ")
            return conexion, cursor
        except pyodbc.Error as e:
            print(f'    Hilo[{PAR11}] - {dt.now().time()} - Error al conectar con Oracle: {e}')
            return None, None 

    # 2 - def Oracle_Cerrar_Conexion(conexion , cursor)
    def Oracle_Cerrar_Conexion(conexion, cursor):
        try:
            if cursor:
                cursor.close()
            if conexion:
                conexion.close()
            print(f"    Hilo[{PAR11}] - {dt.now().time()} - Conexión cerrada........ ")
        except pyodbc.Error as e:
            print(f'    Hilo[{PAR11}] - Error al cerrar la conexión: {e}')

    ### -------------------------------- Inicio del programa ----------------------------

    # Oracle, Parámetros de conexión:
    oracle_dns=PAR4
    oracle_uid=PAR5
    oracle_pwd=PAR6

    # Oracle, Establecer Conexión Oracle:
    conexion, cursor=Oracle_Establece_Conexion(oracle_dns, oracle_uid, oracle_pwd)

    if (conexion != None) or (cursor != None):
        # Parámetros otros, file 'txt' y variables de apoyo:
        var_periodo = PAR7    # var_periodo = int(f'{dt.now().year}{dt.now().month}')
        var_fecha_proceso = dt.now().date()
        files_csv = PAR2
        file_path = f"C:\\MisCompilados\\file_in\\BIACTIVOS\\{files_csv}.txt"

        # Importación del fichero "FileN.txt" en un DataFrame:
        df = pd.read_csv(file_path, sep='\t', header=None,
            names=['NUMERO_DE_OPERACION', 'NUMERO_DE_CUOTA', 'FECHA_DE_VENCIMIENTO', 'CAPITAL', 'INTERES', 'IVA',
                'TOTAL_CUOTA','TOTAL_RECAUDADO', 'FECHA_DE_PAGO', 'FECHA_DESCUENTO_DE_NOMINA', 'ESTADO_DEL_PAGO'],
            dtype={'NUMERO_DE_OPERACION': str, 'NUMERO_DE_CUOTA': int, 'CAPITAL': float, 'INTERES': float, 'IVA': float, 
                'TOTAL_CUOTA':float, 'TOTAL_RECAUDADO':float,'ESTADO_DEL_PAGO':str ,'ESTADO_DEL_PAGO':str},
            parse_dates=['FECHA_DE_VENCIMIENTO','FECHA_DE_PAGO','FECHA_DESCUENTO_DE_NOMINA'],
            dayfirst=True
        )

        # Imprime log del estado del Hilo
        print(f'    Hilo[{PAR11}] - {dt.now().time()} - Insert Into Oracle {PAR3} {len(df):,} records From {PAR2}.txt'.replace(",","."))

        ### -------------------------------------- Oracle -----------------------------------
        # Parámetros necesarios para Exportación del DataFrame al Oracle de TdA:
        table_name = PAR3

        # Oracle, Recorro el DataFrame registro por registro
        for index, row in df.iterrows():
            # Tratamiento de valores NaT a None
            tmpFECHA1=row['FECHA_DE_PAGO']
            tmpFECHA2=row['FECHA_DESCUENTO_DE_NOMINA']
            if pd.isna(tmpFECHA1):
                tmpFECHA1=None
            if pd.isna(tmpFECHA2):
                tmpFECHA2=None    
            # Ejecución del INSERT SQL
            cursor.execute(
                f"""INSERT INTO {table_name} (NUMERO_DE_OPERACION, NUMERO_DE_CUOTA, FECHA_DE_VENCIMIENTO, CAPITAL, INTERES, IVA,
                TOTAL_CUOTA, TOTAL_RECAUDADO, FECHA_DE_PAGO, FECHA_DESCUENTO_DE_NOMINA, ESTADO_DEL_PAGO, PERIODO, FECHA_PROCESO) 
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                row['NUMERO_DE_OPERACION'], row['NUMERO_DE_CUOTA'], row['FECHA_DE_VENCIMIENTO'], row['CAPITAL'], row['INTERES'], 
                row['IVA'], row['TOTAL_CUOTA'], row['TOTAL_RECAUDADO'], tmpFECHA1, tmpFECHA2, 
                row['ESTADO_DEL_PAGO'], var_periodo, var_fecha_proceso
            )

        # Oracle, Confirma los cambios
        conexion.commit()
        print(f'    Hilo[{PAR11}] - {dt.now().time()} - Commit OK')

        # Oracle, Cierre de conexiones y liberación de memoria:
        Oracle_Cerrar_Conexion(conexion, cursor)
        ### --------------------------------- Fin del programa ------------------------------
else:
    print("  ¡¡¡ AVISO: Este código es una rutina de SUBPROCESS !!!")
    print("se debe ejecutar desde: Corte_fichero_SubProcess_Master.py")
