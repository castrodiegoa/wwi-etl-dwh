from src.extract.wwi_extract import (
    extract_fact_facturas_base,
    extract_dim_empleado,
    extract_dim_producto,
    extract_dim_cliente,
)
from src.transform.transform_data import (
    build_dim_tiempo,
    build_dim_cliente,
    build_dim_producto,
    build_dim_empleado,
    build_fact_facturas,
)
from src.load.postgres_load import load_to_postgres


def main():
    # EXTRACCIÓN
    fact_facturas_base_df = extract_fact_facturas_base()
    dim_empleado_base_df = extract_dim_empleado()
    dim_producto_base_df = extract_dim_producto()
    dim_cliente_base_df = extract_dim_cliente()

    # TRANSFORMACIÓN – Dimensiones
    dim_tiempo = build_dim_tiempo(fact_facturas_base_df)
    dim_cliente = build_dim_cliente(dim_cliente_base_df)
    dim_producto = build_dim_producto(dim_producto_base_df)
    dim_empleado = build_dim_empleado(dim_empleado_base_df)

    # CARGA – Dimensiones
    load_to_postgres(dim_tiempo, "dim_tiempo")
    load_to_postgres(dim_cliente, "dim_cliente")
    load_to_postgres(dim_producto, "dim_producto")
    load_to_postgres(dim_empleado, "dim_empleado")

    # TRANSFORMACIÓN Y CARGA – Hechos
    fact_df = build_fact_facturas(
        fact_facturas_base_df, dim_tiempo, dim_cliente, dim_producto, dim_empleado
    )
    load_to_postgres(fact_df, "fact_facturas")

    print("ETL completado.")


if __name__ == "__main__":
    main()
