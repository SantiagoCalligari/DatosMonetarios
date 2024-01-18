import pandas as pd
import plotly.express as px
import pymysql

# Conexión a la base de datos MySQL
conexion = pymysql.connect(
    host="localhost", user="isoroc", password="muriel", database="DatosMonetarios"
)
cursor = conexion.cursor()

# Consultas SQL para obtener los datos de las tablas
consulta_plazo_fijo = "SELECT Año, Mes, Acumulado FROM InterésPlazoFijo"
consulta_spy500 = "SELECT Año, Mes, Acumulado FROM SPY500"
consulta_valor_dolar = "SELECT Año, Mes, Acumulado FROM ValorDolar"

# Leer los datos en DataFrames de Pandas
df_plazo_fijo = pd.read_sql(consulta_plazo_fijo, conexion)
df_spy500 = pd.read_sql(consulta_spy500, conexion)
df_valor_dolar = pd.read_sql(consulta_valor_dolar, conexion)

# Cerrar la conexión a la base de datos
conexion.close()

# Crear una nueva columna "Fecha" como un objeto datetime
df_plazo_fijo["Fecha"] = pd.to_datetime(
    df_plazo_fijo[["Año", "Mes"]].astype(str).agg("-".join, axis=1), format="%Y-%m"
)
df_spy500["Fecha"] = pd.to_datetime(
    df_spy500[["Año", "Mes"]].astype(str).agg("-".join, axis=1), format="%Y-%m"
)
df_valor_dolar["Fecha"] = pd.to_datetime(
    df_valor_dolar[["Año", "Mes"]].astype(str).agg("-".join, axis=1), format="%Y-%m"
)

# Agregar una columna "Fuente" a cada DataFrame
df_plazo_fijo["Fuente"] = "PlazoFijo"
df_spy500["Fuente"] = "SPY500"
df_valor_dolar["Fuente"] = "ValorDolar"

# Concatenar los DataFrames en uno solo
df_combined = pd.concat([df_plazo_fijo, df_spy500, df_valor_dolar])

# Crear un gráfico de líneas con Plotly
fig = px.line(
    df_combined,
    x="Fecha",
    y="Acumulado",
    color="Fuente",
    labels={"Acumulado": "Acumulado en Pesos"},
    title="Rendimiento de Plazo Fijo, SPY500 y Valor Dolar para Todos los Meses",
    template="plotly",
)

# Mostrar el gráfico
fig.write_html("grafico_interactivo.html")
fig.show()
