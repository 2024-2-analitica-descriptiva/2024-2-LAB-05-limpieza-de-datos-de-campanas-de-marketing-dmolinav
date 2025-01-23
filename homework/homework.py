"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel


def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """

    return
import pandas as pd
import os
import zipfile

# Rutas
carpeta_entrada = "files/input/"
carpeta_salida = "files/output/"

# crear carpeta si no existe
os.makedirs(carpeta_salida, exist_ok=True)  

def process_campaing_data(ruta_archivo):
    """
    Procesa un archivo CSV comprimido en formato ZIP, limpia los datos y genera los tres archivos CSV solicitados.
    """
    with zipfile.ZipFile(ruta_archivo, 'r') as archivo_zip:
        
        nombre_archivo_zip = archivo_zip.namelist()[0]
        
        
        with archivo_zip.open(nombre_archivo_zip) as archivo:
            datos = pd.read_csv(archivo)

    

    # Datos del Cliente (cliente.csv)
    datos_cliente = datos[['client_id', 'age', 'job', 'marital', 'education', 'credit_default', 'mortgage']].copy()
    datos_cliente['job'] = datos_cliente['job'].str.replace('.', '').str.replace('-', '_')
    datos_cliente['education'] = datos_cliente['education'].replace('unknown', pd.NA)
    datos_cliente['education'] = datos_cliente['education'].str.replace('.', '_')
    datos_cliente['credit_default'] = datos_cliente['credit_default'].map({'yes': 1, 'no': 0})
    datos_cliente['mortgage'] = datos_cliente['mortgage'].map({'yes': 1, 'no': 0})

    # Datos de la Campaña (campana.csv)
    datos_campana = datos[['client_id', 'number_contacts', 'contact_duration', 
                      'previous_campaign_contacts', 'previous_outcome', 'campaign_outcome']].copy()
    datos_campana['previous_outcome'] = datos_campana['previous_outcome'].map({'success': 1}).fillna(0)
    datos_campana['campaign_outcome'] = datos_campana['campaign_outcome'].map({'yes': 1}).fillna(0)
    
    # Crear la columna 'last_contact_date' en formato YYYY-MM-DD (solo año 2022)
    datos_campana['last_contact_date'] = datos['month'] + ' ' + datos['day'].astype(str) + ' 2022'
    datos_campana['last_contact_date'] = pd.to_datetime(datos_campana['last_contact_date'], format='%b %d %Y').dt.strftime('%Y-%m-%d')

    return datos_cliente, datos_campana

def clean_campaign_data():
    """
    Procesa todos los archivos CSV comprimidos en la carpeta 'files/input/'.
    """
    todos_datos_cliente = []
    todos_datos_campana = []
    
   
    for nombre_archivo in os.listdir(carpeta_entrada):
        if nombre_archivo.endswith('.csv.zip'):
            ruta_archivo = os.path.join(carpeta_entrada, nombre_archivo)
            datos_cliente, datos_campana = process_campaing_data(ruta_archivo)
            
            # Añadir los dataframes a listas
            todos_datos_cliente.append(datos_cliente)
            todos_datos_campana.append(datos_campana)

    # Concatenar en un solo dataframe
    datos_finales_cliente = pd.concat(todos_datos_cliente, ignore_index=True)
    datos_finales_campana = pd.concat(todos_datos_campana, ignore_index=True)


    if 'cons_price_idx' in datos_finales_cliente.columns and 'euribor_three_months' in datos_finales_cliente.columns:
        datos_economicos = datos_finales_cliente[['client_id', 'cons_price_idx', 'euribor_three_months']].copy()
    else:
        # llenar con NaN columnas faltantes
        datos_economicos = datos_finales_cliente[['client_id']].copy()
        datos_economicos['cons_price_idx'] = pd.NA
        datos_economicos['euribor_three_months'] = pd.NA
    
    # Guardar los archivos CSV en la carpeta de salida
    datos_finales_cliente.to_csv(os.path.join(carpeta_salida, 'client.csv'), index=False)
    datos_finales_campana.to_csv(os.path.join(carpeta_salida, 'campaign.csv'), index=False)
    
   
    datos_economicos.to_csv(os.path.join(carpeta_salida, 'economics.csv'), index=False)

if __name__ == "__main__":
    clean_campaign_data()
