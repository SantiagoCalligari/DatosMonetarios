import pandas as pd
import mysql.connector


mydb = mysql.connector.connect(
    host="localhost", user="root", password="", database="Data"
)


def tasa_mensual(tasa):
    tasa = tasa.replace(",", ".")
    return float(tasa) / 12.0


def separar_fecha(fecha):
    meses = {
        "ene": 1,
        "feb": 2,
        "mar": 3,
        "abr": 4,
        "may": 5,
        "jun": 6,
        "jul": 7,
        "ago": 8,
        "sep": 9,
        "oct": 10,
        "nov": 11,
        "dic": 12,
    }
    partes = fecha.split("-")
    mes = meses[partes[0].lower()]
    año = (
        int(partes[1]) + 2000
    )  # Suponiendo que los años están en formato de dos dígitos
    if año > 2080:
        año = año - 100
    return (año, mes)


df = pd.read_csv("data.csv", usecols=["fecha", "tasa"])
first = True
acumulado = 100

mycursor = mydb.cursor()
for row in df.iterrows():
    tasa = tasa_mensual(row[1]["tasa"])
    if first:
        first = False
        last_tasa = tasa
    else:
        acumulado = acumulado * (1 + last_tasa / 100)

    (año, mes) = separar_fecha(row[1]["fecha"])
    sql = "INSERT INTO InterésPlazoFijo ( Año, Mes, Tasa, Acumulado) VALUES (%s, %s, %s, %s)"
    val = (año, mes, tasa, acumulado)
    mycursor.execute(sql, val)
    mydb.commit()
