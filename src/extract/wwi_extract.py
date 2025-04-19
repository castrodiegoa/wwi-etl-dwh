import pandas as pd
from sqlalchemy import create_engine
from src.config.config import SQLSERVER_CONFIG


def get_sqlserver_engine():
    user = SQLSERVER_CONFIG["user"]
    password = SQLSERVER_CONFIG["password"]
    host = SQLSERVER_CONFIG["host"]
    port = SQLSERVER_CONFIG["port"]
    database = SQLSERVER_CONFIG["database"]
    url = (
        f"mssql+pyodbc://{user}:{password}"
        f"@{host}:{port}/{database}?driver=ODBC+Driver+18+for+SQL+Server;"
        "TrustServerCertificate=yes"
    )
    engine = create_engine(url)
    return engine


def extract_fact_facturas_base():
    engine = get_sqlserver_engine()
    query = """
    SELECT
        I.InvoiceID      AS factura_id,
        I.CustomerID     AS cliente_id,
        I.SalespersonPersonID AS empleado_id,
        L.InvoiceLineID  AS linea_factura_id,
        L.StockItemID    AS producto_id,
        I.InvoiceDate    AS fecha_operacion,
        L.Quantity       AS cantidad,
        L.UnitPrice      AS precio_unitario,
        L.TaxRate        AS tasa_impuesto,
        L.TaxAmount      AS monto_impuesto,
        L.ExtendedPrice  AS precio_extendido,
        L.LineProfit     AS ganancia_linea
    FROM Sales.Invoices I
    JOIN Sales.InvoiceLines L
      ON I.InvoiceID = L.InvoiceID
    """
    fact_facturas_base_df = pd.read_sql(query, con=engine)
    return fact_facturas_base_df


def extract_dim_empleado():
    engine = get_sqlserver_engine()
    query = """
    SELECT
        P.PersonID       AS empleado_id,
        P.FullName       AS nombre_empleado,
        P.IsSalesperson  AS es_vendedor,
        P.PreferredName  AS nombre_preferido
    FROM Sales.Invoices I
    JOIN Application.People P
      ON I.SalespersonPersonID = P.PersonID
    """
    dim_empleado_base_df = pd.read_sql(query, con=engine)
    return dim_empleado_base_df


def extract_dim_producto():
    engine = get_sqlserver_engine()
    query = """
    SELECT
        S.StockItemID    AS producto_id,
        S.StockItemName  AS nombre_producto,
        S.Size           AS tamano
    FROM Sales.InvoiceLines L
    JOIN Warehouse.StockItems S
      ON L.StockItemID = S.StockItemID
    """
    dim_producto_base_df = pd.read_sql(query, con=engine)
    return dim_producto_base_df


def extract_dim_cliente():
    engine = get_sqlserver_engine()
    query = """
    SELECT
        C.CustomerID     AS cliente_id,
        C.CustomerName   AS nombre_cliente,
        CC.CustomerCategoryName AS nombre_categoria_cliente,
        CI.CityName      AS ciudad_entrega
    FROM Sales.Customers C
    JOIN Sales.Invoices I
      ON C.CustomerID = I.CustomerID
    JOIN Sales.CustomersCategories CC
      ON C.CustomerCategoryID = CC.CustomerCategoryID
    JOIN Application.Cities CI
      ON C.DeliveryCityID = CI.CityID
    """
    dim_cliente_base_df = pd.read_sql(query, con=engine)
    return dim_cliente_base_df
