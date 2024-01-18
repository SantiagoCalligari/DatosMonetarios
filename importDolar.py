import pandas as pd
import mysql.connector


def separar_fecha(fecha):
    fecha = fecha.split(".")
    año = fecha[2]
    mes = fecha[1]
    return (año, mes)


mydb = mysql.connector.connect(
    host="localhost", user="root", password="", database="Data"
)


df = pd.read_csv("Dolar.csv", usecols=["Fecha", "Último"])
mycursor = mydb.cursor()

for row in df.iterrows():
    (año, mes) = separar_fecha(row[1]["Fecha"])
    ultimo = row[1]["Último"].replace(",", ".")
    cotizacion = float(ultimo)
    sql = "INSERT INTO ValorDolar ( Año, Mes, Cotizacion, Acumulado) VALUES (%s, %s, %s, %s)"
    val = (año, mes, cotizacion, cotizacion * 100.0)
    print(año, mes, cotizacion)
    mycursor.execute(sql, val)
    mydb.commit()
