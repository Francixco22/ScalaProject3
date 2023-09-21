#!/usr/bin/env python
# coding: utf-8

# In[ ]:


from google.cloud import bigquery
import csv
import json
import io
import time
from google.cloud import pubsub_v1
from google.cloud import storage
project_id = "model-journal-395918"
subscription_id = "EnergyDemand4-sub"
archivo_csv = "datos.csv"
bucket_name = "bucketmal"
dataset_id = "dataset1Metadata"#añadido
tabla_id = "EnergyTable"#añadido

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

received_data = []

# Función para escribir los datos en CSV y BigQuery
def write_data_to_csv_and_bigquery(data):
    if not data:
        return

    # Abre el archivo CSV en modo de apertura y escritura
    with open(archivo_csv, 'a', newline='') as file:
        csv_writer = csv.DictWriter(file, fieldnames=data[0].keys())
        # Si el archivo está vacío, escribe los encabezados
        if file.tell() == 0:
            csv_writer.writeheader()
        csv_writer.writerows(data)
        print("open")

    # Sube el archivo CSV a Cloud Storage
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(archivo_csv)
    print("bucket.blob")

    with open(archivo_csv, "rb") as source_file:
        blob.upload_from_file(source_file, content_type='text/csv')
        print("open")

    print(f"Archivo CSV guardado en {blob.name} en el bucket {bucket_name}")

    # Crea una tabla en BigQuery y carga los datos desde el archivo CSV
    bigquery_client = bigquery.Client()
    dataset_ref = bigquery_client.dataset(dataset_id)
    table_ref = dataset_ref.table(tabla_id)
    print("tabla cargada")

    job_config = bigquery.LoadJobConfig(
        autodetect=True,
        skip_leading_rows=1,
        source_format=bigquery.SourceFormat.CSV,
    )

    print("BQ client")
    table = bigquery_client.get_table(table_ref)
    bigquery_client.load_table_from_uri(
        f"gs://{bucket_name}/{archivo_csv}", table_ref, job_config=job_config
    ).result()

    print(f"Datos cargados en la tabla {tabla_id}")


# Llamada para crear la tabla si no existe
create_table_if_not_exists(dataset_id, tabla_id)
print("create_table_if_not_exists")
# Crea la suscripción
subscription_path = subscriber.subscription_path(project_id, subscription_id)
print("subscriptioncreated")

received_data = []

# Define la función que se ejecutará cuando se reciba un mensaje
def callback(message):
    try:
        # Procesa el mensaje recibido y lo convierte a formato json
        mensaje_json = message.data.decode("utf-8")
        data = json.loads(mensaje_json)
        received_data.extend(data)
        print("datos cargados")

        # Se reconoce el mensaje y se marca como procesado
        message.ack()
        print("Mensaje reconocido")
        
        # Llama a la función para escribir los datos en CSV y BigQuery
        write_data_to_csv_and_bigquery(received_data)
        print("datos escritos")
        received_data.clear()#Esta ha sido la última línea implementada
    
    except Exception as e:
        print(f"Error al procesar el mensaje: {str(e)}")

# Inicia la recepción de mensajes
subscriber.subscribe(subscription_path, callback=callback)

print(f"Escuchando mensajes en tiempo real. Los datos se guardan en CSV, Cloud Storage y BigQuery")

# Mantén el programa en ejecución para recibir mensajes en tiempo real
while True:
    
    time.sleep(60)  # Puedes ajustar el intervalo de tiempo si es necesario

