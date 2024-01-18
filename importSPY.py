import pandas as pd
import mysql.connector


def separar_fecha(fecha):
    fecha = fecha.split("/")
    año = fecha[2]
    mes = fecha[0]
    return (año, mes)


mydb = mysql.connector.connect(
    host="localhost", user="root", password="", database="Data"
)


df = pd.read_csv("SPY500.csv", usecols=["Date", "Price"])
mycursor = mydb.cursor()

for row in df.iterrows():
    (año, mes) = separar_fecha(row[1]["Date"])

    ultimo = row[1]["Price"].replace(",", "")
    cotizacion = float(ultimo)
    mycursor.execute(
        "SELECT Cotizacion FROM ValorDolar WHERE Año = %s AND Mes = %s", (año, mes)
    )
    valor_dolar = mycursor.fetchone()
    acumulado = cotizacion * 0.1572 * float(valor_dolar[0])
    sql = (
        "INSERT INTO SPY500 ( Año, Mes, Cotizacion, Acumulado) VALUES (%s, %s, %s, %s)"
    )
    val = (año, mes, cotizacion, acumulado)
    mycursor.execute(sql, val)
    mydb.commit()
