import os
import pandas as pd

# Importa la base de datos
HOUSING_PATH = os.path.join("", "", "")

def load_DataBase(csv, housing_path=HOUSING_PATH):
    csv_path = os.path.join(housing_path, csv)
    return pd.read_csv(csv_path)

client = load_DataBase('client_table.csv')
trip = load_DataBase('trip_table.csv')
reservation = load_DataBase('reservation_table.csv')
driver = load_DataBase('driver_table.csv')

#Creación de la nueva base de datos importando los atributos
Data = pd.DataFrame(columns=['trip_id', 'departure_at', 'arrival_at', 'route_name', 'vehicle_capacity', 'sold seats', 'revenue', 'occupancy'])
    
Data["trip_id"] = pd.concat([pd.DataFrame([i], columns=['A']) for i in trip["trip_id"]],
          ignore_index=True)

Data["departure_at"] = pd.concat([pd.DataFrame([i], columns=['A']) for i in trip["departure_at"]],
          ignore_index=True)

Data["arrival_at"] = pd.concat([pd.DataFrame([i], columns=['A']) for i in trip["arrival_at"]],
          ignore_index=True)

Data["route_name"] = pd.concat([pd.DataFrame([i], columns=['A']) for i in trip["route_name"]],
          ignore_index=True)

Data["vehicle_capacity"] = pd.concat([pd.DataFrame([i], columns=['A']) for i in trip["vehicle_capacity"]],
          ignore_index=True)

#Obtener los sold seats
for indexat in range(len(trip["trip_id"])+1):
    
    df_filter = reservation['trip_id'].isin([indexat])
    row = reservation[df_filter]
    b = row['seats'].sum(axis = 0)
    Data['sold seats'][indexat-1] = b
    
#Obtener revenue 
Data["revenue"] = Data["sold seats"] * trip["seat_price"]

#obtener occupancy
Data["occupancy"] = (Data["sold seats"] / trip["vehicle_capacity"])*100

#Convertir el dataframe en archivo CSV 
Data.to_csv('Base de datos completa.csv')

filtered_df=Data.query("departure_at >= '2017-11-01' and departure_at <='2017-12-01'")
print(filtered_df)

filtered_df.to_csv('Base de datos filtrada por fecha.csv')

#Cambiar el formato del dataframe filtrado para pasarlo a google sheets

filtered_df['trip_id'] = filtered_df['trip_id'].astype(int)
filtered_df['vehicle_capacity'] = filtered_df['vehicle_capacity'].astype(int)
filtered_df['sold seats'] = filtered_df['sold seats'].astype(int)
filtered_df['revenue'] = filtered_df['revenue'].astype(int)
filtered_df['occupancy'] = filtered_df['occupancy'].astype(int)

#Conexión con Google Sheets e introducir los datos del dataframe
import gspread
from gspread_dataframe import set_with_dataframe

gc = gspread.service_account(filename='datosurv-b42ae25415d1.json')
sh = gc.open("A")
ws = sh.get_worksheet(0)

# Enviar los datos a Google Sheets
row=1
col=1
worksheet = ws
set_with_dataframe(worksheet,filtered_df, row, col)