#!/usr/bin/env python
# coding: utf-8

# In[2]:


from google.cloud import pubsub_v1
from google.cloud import storage
from google.cloud import bigquery
import csv
import json

# Configura la información de tu proyecto y el tema de Pub/Sub
project_id = "model-journal-395918"
subscription_id = "EnergyDemand4-sub"
archivo_csv = "datos.csv"
bucket_name = "bucketmal"
dataset_id = "dataset1Metadata"#añadido
tabla_id = "EnergyTable"#añadido
schema = [
    bigquery.SchemaField("Energy", "STRING"),
    bigquery.SchemaField("Value", "FLOAT"),
    bigquery.SchemaField("Renovable", "STRING"),
    bigquery.SchemaField("Time", "TIMESTAMP"),
]

# Crea un cliente de Pub/Sub
subscriber = pubsub_v1.SubscriberClient()

# Crea una tabla en BigQuery si no existe--> Se ha creado este método
def create_table_if_not_exists(dataset_id, table_id):
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(table_id)

    try:
        # Intenta obtener la tabla
        table = bigquery_client.get_table(table_ref)
    except Exception as e:
        # Si la tabla no existe, la crea
        if "Not found" in str(e):
            schema = [
                bigquery.SchemaField("Energy", "STRING"),
                bigquery.SchemaField("Value", "FLOAT"),
                bigquery.SchemaField("Renovable", "STRING"),
                bigquery.SchemaField("Time", "TIMESTAMP"),
            ]
            table = bigquery.Table(table_ref, schema=schema)
            bigquery_client.create_table(table)
            #Hasta aquí

# Define la función que se ejecutará cuando se reciba un mensaje
def callback(message):
    # Procesa el mensaje recibido y lo convierte a formato json
    mensaje_json = message.data.decode("utf-8")
    data = json.loads(mensaje_json)

    # Crea una lista para almacenar todas las filas de datos
    rows_to_insert = []

    # Abre el archivo CSV en modo de escritura
    with open(archivo_csv, 'a', newline='') as file:
        escritor = csv.DictWriter(file, fieldnames=data[0].keys())

        # Si el archivo está vacío, escribe el encabezado
        if file.tell() == 0:
            escritor.writeheader()

        # Itera sobre las filas en el mensaje y escribe en el archivo CSV
        for fila in data:
            escritor.writerow(fila)

            # Procesa los datos para que coincidan con el esquema de BigQuery
            row = (
                fila["Energy"],
                fila["Value"],
                fila["Renovable"],
                fila["Time"]
            )
            rows_to_insert.append(row)

    # Se reconoce el mensaje y se marca como procesado
    message.ack()

    if rows_to_insert:
        # Inserta todas las filas en BigQuery
        bigquery_client = bigquery.Client()
        dataset_ref = bigquery_client.dataset(dataset_id)
        table_ref = dataset_ref.table(tabla_id)

        table = bigquery_client.get_table(table_ref)
        bigquery_client.insert_rows(table, rows_to_insert)
        print(f"Datos insertados en la tabla {tabla_id} y escritos en el archivo CSV.")

    print("Procesamiento y carga de datos en BigQuery y CSV completados.")
    ##Termina aqui

# Llamada para crear la tabla si no existe
create_table_if_not_exists(dataset_id, tabla_id)
# Crea la suscripción
subscription_path = subscriber.subscription_path(project_id, subscription_id)

# Inicia la recepción de mensajes
subscriber.subscribe(subscription_path, callback=callback)

print(f"Escuchando mensajes en tiempo real. Los datos se guardan en {archivo_csv}")

# Mantén el programa en ejecución para recibir mensajes en tiempo real
import time
while True:
    time.sleep(120)  # Puedes ajustar el intervalo de tiempo si es necesario


# In[ ]:




