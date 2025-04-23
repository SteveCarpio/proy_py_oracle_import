#------------------------------------------------------------------------------------------------
# Programa: 
#           Corte_Fichero_SubProcess_Master: 
#               Orquestador de procesos, lanzará N.Hilos SubProcess definidos AdDoc.
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
#               sys: Iteración con los argumentos del Sistema
#               datetime: Señales de tiempo
#           Slave:
#               cfg: Parámetros comunes para el Master y Slave
#               pandas: Gestión analítica del fichero .txt en un DataFrame
#               pyodbc: Inserción de datos del DataFrame en Oracle
#               datetime: Señales de tiempo
#               sys: Interactuar con los argumentos recibidos
#                                                                          By --> SteveCarpio2024
#------------------------------------------------------------------------------------------------

import cfg.BIACTIVOS_variables as sTv
from   cfg.BIACTIVOS_librerias_m import *

#-------------- Librerías SLAVE (dependencias para Compile)
#import pandas as pd
#import pyodbc
#--------------

# File de entrada de ANEXO3.txt
file_entrada = f"C:\\MisCompilados\\PROY_BIACTIVOS\\file_in\\File.txt"

# --------------------------------------------------------------------------

# MODO: Desarrollo
file_slave = f"C:\\Users\\scarpio\\Documents\\GitHub\\proy_py_oracle_import\\src\\BIACTIVOS_Slave.py"

# MODO: Compile 
#current_dir = os.path.dirname(sys.executable)
#file_slave = os.path.join(current_dir, 'BIACTIVOS_Slave.exe')

# --------------------------------------------------------------------------

# Parámetros Comunes
tiempo_inicio = dt.now()
tabla_txt = ["x"]
procesos = []

# Función que nos servirá para dividir el fichero de entrada ".txt" en varios partes/hilos
def dividir_txt_x_partes():
    ruta_archivo = file_entrada   
    num_partes = numero_hilos 
    
    # Leer el contenido del archivo original  
    with open(ruta_archivo, 'r') as archivo:  
        lineas = archivo.readlines()

    # Calcular el número de líneas por archivo
    total_lineas = len(lineas)
    lineas_por_parte = total_lineas // num_partes
    sobrante = total_lineas % num_partes

    # Directorio donde se guardarán los archivos divididos
    directorio = os.path.dirname(ruta_archivo)

    # Dividir el archivo en partes y guardarlas en nuevos archivos
    inicio = 0
    for i in range(1, num_partes + 1):
        # Calcular el índice de fin para cada parte
        fin = inicio + lineas_por_parte + (1 if i <= sobrante else 0)  # Agrega una línea extra a los primeros archivos si hay sobrante
        nombre_parte = f"File{i}.txt"
        ruta_parte = os.path.join(directorio, nombre_parte)
        
        # Escribir cada parte en un nuevo archivo
        with open(ruta_parte, 'w') as archivo_parte:
            archivo_parte.writelines(lineas[inicio:fin])
        
        # Actualizar el índice de inicio para la siguiente parte
        inicio = fin

        # Crea lista con los nombres de salida
        tabla_txt.append(f"File{i}")
    # Imprimir un mensaje para la log
    print(f"  El archivo de entrada tiene {len(lineas):,} registros dividido en {numero_hilos} partes.".replace(",","."))

# Función que nos lanzará los SUBPROCESS de la lista "procesos[...]" 
def ejecucion_multi_hilo():
    print(f"  Invocado {numero_hilos} Hilos SubProcess.\n")
    # Ejecutar cada proceso sin esperar
    for i in range(1, numero_hilos + 1):
        # Iniciar el proceso y pasar el argumento
        # Desarrollo
        proceso = subprocess.Popen(["python", file_slave, str(i), tabla_txt[i], str(tabla_ora), oracle_dns, oracle_uid, oracle_pwd, str(var_periodo)])  # Desarrollo
        # Compile
        #proceso = subprocess.Popen([file_slave, str(i), tabla_txt[i], str(tabla_ora), oracle_dns, oracle_uid, oracle_pwd, str(var_periodo)])            # Compile
        procesos.append(proceso)  
    # Esperar a que todos los procesos terminen
    for proceso in procesos:
        proceso.wait()

# Función necesario para validar si existe el file.txt de entrada
def validar_existe_txt():
    valida_si_existe = os.path.isfile(file_entrada)
    if valida_si_existe == False:
        print(f"¡¡¡ ERROR: No existe el fichero {file_entrada} !!!")
    return valida_si_existe  # True or False

### -------------------------------- Inicio del programa ----------------------------

oracle_dns   = sTv.BIACT_ora_dns
oracle_uid   = sTv.BIACT_ora_uid
oracle_pwd   = sTv.BIACT_ora_pwd
tabla_ora    = sTv.BIACT_tab_ora
numero_hilos = sTv.BIACT_num_hil

xyear = dt.now().year
xmonth = dt.now().month
var_periodo = int(f'{xyear}{xmonth:02d}')

# ---------------------------------- MENU 
os.system('cls')
print(f" CORTE DE FICHERO DE BIACTIVOS\n -----------------------------\n")
print(f"   Parámetros por defecto:\n")
print(f"     Número de Hilos: {numero_hilos}")
print(f"     Tabla Oracle:    {tabla_ora}")
print(f"     Oracle DNS:      {oracle_dns}")
print(f"     Oracle UID:      {oracle_uid}")
print(f"     Oracle PWD:      {oracle_pwd}")
print(f"     Periodo AAAAMM:  {var_periodo}\n")

sw=0
tmp1=""
tmp2=""
tmp3=""
tmp4=""
tmp5=""
tmp6=""
continuar = "n"
continuar = input("¿ Quiere cambiar, Número de hilos (s/n) ?:    ")
if continuar == "s" or continuar == "S":
    numero_hilos = int(input(f"  ¡ Indique número de hilos! [{numero_hilos}]: "))
    if numero_hilos < 2 or numero_hilos > 18:
        print("    ¡Atención! Valor fuera del rango, usar valores del (2 ..al.. 18) hilos")
        print(f"      Se usará el valor predeterminado: {sTv.BIACT_num_hil}")
        numero_hilos = sTv.BIACT_num_hil
    sw=1
    tmp1="*"

continuar = "n"
continuar = input("¿ Quiere cambiar, Tabla Oracle (s/n) ?:       ")
if continuar == "s" or continuar == "S":
    tabla_ora = input(f"  ¡ Indique el nombre de la Tabla Oracle! [{tabla_ora}]: ")
    sw=1
    tmp2="*"

continuar = "n"
continuar = input("¿ Quiere cambiar, DNS Oracle (s/n) ?:         ")
if continuar == "s" or continuar == "S":
    oracle_dns = input(f"  ¡ Indique el DNS Oracle! [{oracle_dns}]: ")
    sw=1
    tmp3="*"

continuar = "n"
continuar = input("¿ Quiere cambiar, UID Oracle (s/n) ?:         ")
if continuar == "s" or continuar == "S":
    oracle_uid = input(f"  ¡ Indique el UID Oracle! [{oracle_uid}]: ")
    sw=1
    tmp4="*"

continuar = "n"
continuar = input("¿ Quiere cambiar, PWD Oracle (s/n) ?:         ")
if continuar == "s" or continuar == "S":
    oracle_pwd = input(f"  ¡ Indique el PWD Oracle! [{oracle_pwd}]: ")
    sw=1
    tmp5="*"

continuar = "n"
continuar = input("¿ Quiere cambiar, Periodo AAAAMM (s/n) ?:     ")
if continuar == "s" or continuar == "S":
    var_periodo = int(input(f"  ¡ Indique el Periodo! [{var_periodo}]: "))
    sw=1
    tmp6="*"

if sw == 1:
    os.system('cls')
    print(f" CORTE DE FICHERO DE BIACTIVOS\n -----------------------------\n")
    print(f"   Parámetros modificados (*):\n")
    print(f"     Número de Hilos: {tmp1}{numero_hilos}{tmp1}")
    print(f"     Tabla Oracle:    {tmp2}{tabla_ora}{tmp2}")
    print(f"     Oracle DNS:      {tmp3}{oracle_dns}{tmp3}")
    print(f"     Oracle UID:      {tmp4}{oracle_uid}{tmp4}")
    print(f"     Oracle PWD:      {tmp5}{oracle_pwd}{tmp5}")
    print(f"     Periodo AAAAMM:  {tmp6}{var_periodo}{tmp6}\n")


continuar = "n"
continuar = input("\n¿ EJECUTAMOS EL PROCESO (s/n) ?: ")
if continuar == "s" or continuar == "S":
    print("\n- - - - - - - - - - - - - - - - - - - - - - - - - - -  - - - - ")

    # ---------------------------------- EJECUCION DEL PROCESO
    # Imprimir inicio del SubProcess multi-hilo
    print(f"\nINICIO - SUBPROCESS-MULTI HILO.... {tiempo_inicio} \n")

    # Llamar a las funciones
    existe_file = validar_existe_txt()
    if existe_file:
        dividir_txt_x_partes()
        ejecucion_multi_hilo()

    # Calcular el Tiempo Total transcurrido del proceso
    diferencia_total = dt.now() - tiempo_inicio
    minutos_totales = diferencia_total.total_seconds() // 60  # Minutos totales : // División entera
    segundos_restantes = round(diferencia_total.total_seconds() % 60, 2) # Segundos restantes y redondear a 2 decimales

    # Imprimir fin del SubProcess multi-hilo
    print(f"\nFIN - SUBPROCESS-MULTI HILO.... {dt.now()} ---> {int(minutos_totales)}min {segundos_restantes:.0f}seg \n")
    continuar = input("\n\n --- FIN --- ")
else:
    print(" --- EXIT --- ")

### --------------------------------- Fin del programa ------------------------------
