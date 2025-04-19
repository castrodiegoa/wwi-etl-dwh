import pandas as pd


# ----------------------------------------------------------------
#  DIM TIEMPO
# ----------------------------------------------------------------
def build_dim_tiempo(fact_facturas_base_df):
    """
    Crea la dimensión de tiempo a partir de las fechas encontradas como InvoiceDate en la tabla Invoices.
    """

    # Extraer las fechas únicas
    fechas = fact_facturas_base_df["fecha_operacion"].dropna().unique()

    # Asegurarnos de convertir a datetime
    fechas = pd.to_datetime(fechas)

    # Construir un DataFrame base con esas fechas
    dim_tiempo = pd.DataFrame({"fecha_completa": fechas})

    # Crear atributos de fecha
    dim_tiempo["dia"] = dim_tiempo["fecha_completa"].dt.day
    dim_tiempo["mes"] = dim_tiempo["fecha_completa"].dt.month
    dim_tiempo["anio"] = dim_tiempo["fecha_completa"].dt.year

    # Nombre del día y nombre del mes
    dim_tiempo["nombre_dia"] = dim_tiempo["fecha_completa"].dt.day_name(locale="es_ES")
    dim_tiempo["nombre_mes"] = dim_tiempo["fecha_completa"].dt.month_name(
        locale="es_ES"
    )

    # Generar surrogate key
    dim_tiempo = dim_tiempo.sort_values("fecha_completa").reset_index(drop=True)
    dim_tiempo["tiempo_id"] = dim_tiempo.index + 1

    # Orden final de columnas
    dim_tiempo = dim_tiempo[
        [
            "tiempo_id",
            "fecha_completa",
            "dia",
            "mes",
            "anio",
            "nombre_dia",
            "nombre_mes",
        ]
    ]

    return dim_tiempo


# -------------- DIM CLIENTE --------------
def build_dim_cliente(dim_cliente_base_df):
    """
    Crea la dimensión de cliente a partir de la tabla customers de los clientes que aparecen en invoices.
    """

    # Aseguramos clientes únicos
    dim_cliente = dim_cliente_base_df.copy().drop_duplicates(subset=["cliente_id"])

    # Orden final de columnas
    dim_cliente = dim_cliente[
        [
            "cliente_id",
            "nombre_cliente",
            "nombre_categoria_cliente",
            "ciudad_entrega",
        ]
    ]

    return dim_cliente


# -------------- DIM PRODUCTO --------------
def build_dim_producto(dim_producto_base_df):
    """
    Crea la dimensión de producto a partir de la tabla StockItems de los productos que aparecen en InvoiceLines.
    """

    # Aseguramos productos únicos
    dim_producto = dim_producto_base_df.copy().drop_duplicates(subset=["producto_id"])

    # Orden final de columnas
    dim_producto = dim_producto[
        [
            "producto_id",
            "nombre_producto",
            "tamano",
        ]
    ]

    return dim_producto


# -------------- DIM EMPLEADO --------------
def build_dim_empleado(dim_empleado_base_df):
    """
    Crea la dimensión de empleado a partir de la tabla People de los empleados que aparecen en Invoices.
    """

    # Aseguramos empleados únicos
    dim_empleado = dim_empleado_base_df.copy().drop_duplicates(subset=["empleado_id"])

    # Orden final de columnas
    dim_empleado = dim_empleado[
        [
            "empleado_id",
            "nombre_empleado",
            "es_vendedor",
            "nombre_preferido",
        ]
    ]

    return dim_empleado


# -------------- FACT FACTURAS --------------
def build_fact_facturas(
    fact_facturas_base_df, dim_tiempo, dim_cliente, dim_producto, dim_empleado
):
    df = fact_facturas_base_df.copy()

    df["fecha_operacion"] = pd.to_datetime(df["fecha_operacion"])

    df = df.merge(dim_tiempo, left_on="fecha_operacion", right_on="fecha_completa")
    df = df.merge(dim_cliente, left_on="cliente_id", right_on="cliente_id")
    df = df.merge(dim_producto, left_on="producto_id", right_on="producto_id")
    df = df.merge(dim_empleado, left_on="empleado_id", right_on="empleado_id")

    return df[
        [
            "factura_id",
            "tiempo_id",
            "cliente_id",
            "producto_id",
            "empleado_id",
            "linea_factura_id",
            "cantidad",
            "precio_unitario",
            "tasa_impuesto",
            "monto_impuesto",
            "precio_extendido",
            "ganancia_linea",
        ]
    ]
