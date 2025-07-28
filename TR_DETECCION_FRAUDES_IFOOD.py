# Databricks notebook source
# MAGIC %md
# MAGIC Descripcion : En esta notebook se genera la tabla final con kpi sobre las ventas de tld y lo cobrado a traves de 3po

# COMMAND ----------

# MAGIC %run "../../../00- Common/00.01_init_variables"

# COMMAND ----------

# MAGIC %run "../../../00- Common/00.02_load_table_include"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Define and load input widgets
# MAGIC

# COMMAND ----------

dbutils.widgets.text("fecha_ayer", "", "Fecha ayer (YYYY-MM-DD)")
dbutils.widgets.text("pipeline_run_id", "", "Pipeline Run ID")
dbutils.widgets.dropdown("execution_mode", "DEFAULT", ["CURRENT_MONTH", "PREVIOUS_MONTH", "DEFAULT"], "Execution Mode")

pipeline_run_id = dbutils.widgets.get('pipeline_run_id').strip() if dbutils.widgets.get('pipeline_run_id').strip() != '' else 'Ejecución Manual'


# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Initialize variables and functions
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta
import calendar




fecha_ayer_str = dbutils.widgets.get("fecha_ayer").strip()
execution_mode = dbutils.widgets.get("execution_mode")

base_date = None
if fecha_ayer_str:
    try:
        base_date = datetime.strptime(fecha_ayer_str, '%Y-%m-%d').date()
    except ValueError:
        dbutils.notebook.exit("{fecha_ayer_str} is not in the valid format: YYYY-MM-DD.")
else:
    base_date = (datetime.today() - timedelta(days=1)).date()
    print(f"Using current date: {base_date.strftime('%Y-%m-%d, %A')}")

if execution_mode == "DEFAULT":
    weekday = (base_date + timedelta(days=1)).weekday()
    
    if weekday == 0:  # Monday
        execution_mode = "PREVIOUS_MONTH"
        print("Today is Monday. Setting mode to PREVIOUS_MONTH.")
    elif weekday == 3:  # Thursday
        execution_mode = "CURRENT_MONTH"
        print("Today is Thursday. Setting mode to CURRENT_MONTH.")
    else:
        day_name = base_date.strftime("%A")
        print(f"Today is {day_name}. No load scheduled for this day.")
        dbutils.notebook.exit(f"No data processing scheduled for {day_name}.")

fecha_desde = None
fecha_ayer = None

if execution_mode == "PREVIOUS_MONTH":
    fecha_ayer = base_date.replace(day=1) - timedelta(days=1)
    fecha_desde = fecha_ayer.replace(day=1)

elif execution_mode == "CURRENT_MONTH":
    fecha_desde = base_date.replace(day=1)
    fecha_ayer = base_date - timedelta(days=1)
    
    if fecha_ayer < fecha_desde:
        dbutils.notebook.exit("Cannot process current month's data on the 1st day.")


if fecha_desde and fecha_ayer:
    fecha_desde = fecha_ayer - timedelta(days=60)
    fecha_desde_str = fecha_desde.strftime('%Y-%m-%d')

    fecha_hasta_str = fecha_ayer.strftime('%Y-%m-%d')

    
    print("Final Calculated Date Range:")
    print(f"  Fecha Desde (Start Date): {fecha_desde_str}")
    print(f"  Fecha Hasta  (End Date):   {fecha_hasta_str}")
    
    
else:
    print(f"\nError: Could not determine a valid date range for mode '{execution_mode}'.")

# COMMAND ----------

#catalog_name, schema_name, table_name, table_full_name = get_table_full_name()
_, _, _, table_full_name = get_table_full_name()


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Create or alter table
# MAGIC

# COMMAND ----------

# Define metadata for the DETECCION_FRAUDES_IFOOD table

dict_table_metadata = {
    "comment": "Esta tabla contiene datos para la detección de fraudes de iFood, incluyendo detalles de ventas, transacciones y cancelaciones.",
    
    "columns": [
        {"name": "calendar_month_id", "type": "STRING", "comment": "Identificador del mes calendario."},
        {"name": "source_to_target_currency_rate", "type": "DECIMAL(38, 18)", "comment": "Tasa de cambio de la moneda de origen a la de destino."},
        {"name": "sales_business_dt", "type": "DATE", "comment": "Fecha comercial de la venta."},
        {"name": "selector", "type": "STRING", "comment": "Campo selector para filtros específicos."},
        {"name": "integration_type", "type": "STRING", "comment": "Tipo de integración."},
        {"name": "integration_group", "type": "STRING", "comment": "Grupo de integración."},
        {"name": "concat_key", "type": "STRING", "comment": "Clave concatenada para identificador único."},
        {"name": "ownerships", "type": "STRING", "comment": "Información de propiedad."},
        {"name": "special_sale_storearea", "type": "STRING", "comment": "Área de la tienda para ventas especiales."},
        {"name": "country_name_desc", "type": "STRING", "comment": "Nombre descriptivo del país."},
        {"name": "location_acronym_cd", "type": "STRING", "comment": "Código de acrónimo de la ubicación."},
        {"name": "key", "type": "STRING", "comment": "Clave genérica."},
        {"name": "external_order_merchant_id", "type": "STRING", "comment": "ID del comerciante para el pedido externo."},
        {"name": "sales_date", "type": "DATE", "comment": "Fecha de la venta."},
        {"name": "sales_business_date", "type": "TIMESTAMP", "comment": "Fecha y hora comercial de la venta."},
        {"name": "sales_start_dttm", "type": "TIMESTAMP", "comment": "Fecha y hora de inicio de la venta."},
        {"name": "sales_end_dttm", "type": "TIMESTAMP", "comment": "Fecha y hora de fin de la venta."},
        {"name": "sales_transaction_id", "type": "BIGINT", "comment": "ID de la transacción de venta."},
        {"name": "special_sale_order", "type": "STRING", "comment": "Pedido de venta especial, parte de la clave primaria."},
        {"name": "salekey", "type": "STRING", "comment": "Clave de la venta."},
        {"name": "pos_register_id", "type": "STRING", "comment": "ID de la caja registradora (POS)."},
        {"name": "pos_register_number", "type": "STRING", "comment": "Número de la caja registradora (POS)."},
        {"name": "channel_name_desc", "type": "STRING", "comment": "Descripción del canal de venta."},
        {"name": "subchannel_name_desc", "type": "STRING", "comment": "Descripción del subcanal de venta."},
        {"name": "integrated", "type": "DECIMAL(38, 18)", "comment": "Valor que indica si la transacción fue integrada."},
        {"name": "sales_type_id", "type": "STRING", "comment": "ID del tipo de venta."},
        {"name": "tld_gross_sale", "type": "DECIMAL(26, 5)", "comment": "Venta bruta de TLD."},
        {"name": "sales_transaction_id_nc", "type": "BIGINT", "comment": "ID de transacción de la nota de crédito."},
        {"name": "nc_duplicated", "type": "INT", "comment": "Indicador de nota de crédito duplicada."},
        {"name": "sales_date_nc", "type": "DATE", "comment": "Fecha de la nota de crédito."},
        {"name": "sales_start_dttm_nc", "type": "TIMESTAMP", "comment": "Fecha y hora de inicio de la nota de crédito."},
        {"name": "sales_end_dttm_nc", "type": "TIMESTAMP", "comment": "Fecha y hora de fin de la nota de crédito."},
        {"name": "nc_gross_sale", "type": "DECIMAL(26, 5)", "comment": "Venta bruta de la nota de crédito."},
        {"name": "nc_time_seg", "type": "BIGINT", "comment": "Tiempo en segundos de la nota de crédito."},
        {"name": "nc_time_min", "type": "BIGINT", "comment": "Tiempo en minutos de la nota de crédito."},
        {"name": "external_order_created_at_gmt", "type": "TIMESTAMP", "comment": "Fecha de creación del pedido externo (GMT)."},
        {"name": "external_order_updated_at_gmt", "type": "TIMESTAMP", "comment": "Fecha de actualización del pedido externo (GMT)."},
        {"name": "external_order_created_at", "type": "STRING", "comment": "Fecha de creación del pedido externo (local)."},
        {"name": "external_order_updated_at", "type": "STRING", "comment": "Fecha de actualización del pedido externo (local)."},
        {"name": "process_time_external_order_seg", "type": "BIGINT", "comment": "Tiempo de procesamiento del pedido externo en segundos."},
        {"name": "reception_time_tld_seg", "type": "BIGINT", "comment": "Tiempo de recepción en TLD en segundos."},
        {"name": "tx_time_tld_seg", "type": "BIGINT", "comment": "Tiempo de transacción en TLD en segundos."},
        {"name": "external_order_description", "type": "STRING", "comment": "Descripción del pedido externo."},
        {"name": "external_order_payment_id", "type": "STRING", "comment": "ID del pago del pedido externo."},
        {"name": "external_order_provider_id", "type": "STRING", "comment": "ID del proveedor del pedido externo."},
        {"name": "external_order_status", "type": "STRING", "comment": "Estado del pedido externo."},
        {"name": "external_order_sub_status", "type": "STRING", "comment": "Sub-estado del pedido externo."},
        {"name": "external_order_amount_value", "type": "DECIMAL(12, 2)", "comment": "Valor del monto del pedido externo."},
        {"name": "external_order_captured_value", "type": "DECIMAL(20, 2)", "comment": "Valor capturado del pedido externo."},
        {"name": "external_order_refunded_value", "type": "DECIMAL(20, 2)", "comment": "Valor reembolsado del pedido externo."},
        {"name": "external_order_payment_type", "type": "STRING", "comment": "Tipo de pago del pedido externo."},
        {"name": "external_order_payment_method", "type": "STRING", "comment": "Método de pago del pedido externo."},
        {"name": "external_order_payment_brand", "type": "STRING", "comment": "Marca del método de pago del pedido externo."},
        {"name": "external_order_liability", "type": "STRING", "comment": "Responsabilidad del pedido externo."},
        {"name": "external_order_integrated_payment_value", "type": "DECIMAL(21, 2)", "comment": "Valor del pago integrado del pedido externo."},
        {"name": "external_order_integrated_payment_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia del pago integrado."},
        {"name": "external_order_integrated_cancelation_value", "type": "DECIMAL(20, 2)", "comment": "Valor de la cancelación integrada."},
        {"name": "external_order_integrated_cancelation_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia de la cancelación integrada."},
        {"name": "external_order_manual_payment_value", "type": "DECIMAL(21, 2)", "comment": "Valor del pago manual."},
        {"name": "external_order_manual_payment_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia del pago manual."},
        {"name": "external_order_manual_cancelation_value", "type": "DECIMAL(20, 2)", "comment": "Valor de la cancelación manual."},
        {"name": "external_order_manual_cancelation_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia de la cancelación manual."},
        {"name": "external_order_integrated_refunded_value", "type": "DECIMAL(20, 2)", "comment": "Valor reembolsado integrado."},
        {"name": "external_order_integrated_refunded_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia del reembolso integrado."},
        {"name": "external_order_manual_refunded_value", "type": "DECIMAL(20, 2)", "comment": "Valor reembolsado manual."},
        {"name": "external_order_manual_refunded_diff", "type": "DECIMAL(27, 5)", "comment": "Diferencia del reembolso manual."},
        {"name": "external_order_integrated_partially_refunded_value", "type": "DECIMAL(20, 2)", "comment": "Valor parcialmente reembolsado integrado."},
        {"name": "external_order_manual_partially_refunded_value", "type": "DECIMAL(20, 2)", "comment": "Valor parcialmente reembolsado manual."},
        {"name": "external_order_integrated_partially_cancelation_value", "type": "DECIMAL(20, 2)", "comment": "Valor de cancelación parcial integrada."},
        {"name": "external_order_manual_partially_cancelation_value", "type": "DECIMAL(20, 2)", "comment": "Valor de cancelación parcial manual."},
        {"name": "transaction_status", "type": "STRING", "comment": "Estado de la transacción."},
        {"name": "nc_status", "type": "STRING", "comment": "Estado de la nota de crédito."},
        {"name": "external_order_cancellation_date", "type": "STRING", "comment": "Fecha de cancelación del pedido externo."},
        {"name": "external_order_cancellation_liability", "type": "STRING", "comment": "Responsabilidad de la cancelación del pedido externo."},
        {"name": "external_order_cancellation_code_description", "type": "STRING", "comment": "Descripción del código de cancelación."},
        {"name": "external_order_integrated_venta_bruta", "type": "DECIMAL(26, 5)", "comment": "Venta bruta integrada del pedido externo."},
        {"name": "external_order_integrated_venta_bruta_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Venta bruta integrada sin impacto."},
        {"name": "external_order_integrated_cancelamiento_total", "type": "DECIMAL(26, 5)", "comment": "Cancelación total integrada."},
        {"name": "external_order_integrated_cancelamiento_total_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Cancelación total integrada sin impacto."},
        {"name": "external_order_integrated_cancelamiento_parcial", "type": "DECIMAL(26, 5)", "comment": "Cancelación parcial integrada."},
        {"name": "external_order_integrated_cancelamiento_parcial_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Cancelación parcial integrada sin impacto."},
        {"name": "external_order_manual_venta_bruta", "type": "DECIMAL(26, 5)", "comment": "Venta bruta manual."},
        {"name": "external_order_manual_venta_bruta_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Venta bruta manual sin impacto."},
        {"name": "external_order_manual_cancelamiento_total", "type": "DECIMAL(26, 5)", "comment": "Cancelación total manual."},
        {"name": "external_order_manual_cancelamiento_total_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Cancelación total manual sin impacto."},
        {"name": "external_order_manual_cancelamiento_parcial", "type": "DECIMAL(26, 5)", "comment": "Cancelación parcial manual."},
        {"name": "external_order_manual_cancelamiento_parcial_sem_impacto", "type": "DECIMAL(26, 5)", "comment": "Cancelación parcial manual sin impacto."}
    ],

    "primary_key": [
        "special_sale_order"
    ]
}


create_or_alter_table(
    table_name=table_full_name,
    dict_table_metadata=dict_table_metadata
 )


# COMMAND ----------

# MAGIC %md
# MAGIC # 4. Get new data from source tables
# MAGIC

# COMMAND ----------

# DBTITLE 1,Creacion de tablas temporales
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW tld_br AS

SELECT
  st.sales_transaction_id,
  st.specialsaleorderld AS special_sale_order_ori,
  CASE
    WHEN LENGTH(st.specialsaleorderld) > 4 THEN SUBSTRING_INDEX(st.specialsaleorderld, ' ', 1)
    WHEN LENGTH(st.specialsaleorderld) > 4 THEN st.specialsaleorderld
  END AS special_sale_order_new,
  st.specialsaletype AS specialsaletypeori,
  'IFOOD' AS specialsaletypenew,
  st.salekey,
  st.integrated,
  st.sales_type_id,
  st.pos_register_id,
  st.country_id,
  cou.country_name_desc,
  loc.ownerships,
  st.loyalty_mcid AS mc_id,
  st.sales_date,
  st.sales_business_dt AS fecha,
  st.sales_start_dttm,
  st.sales_end_dttm,
  lss.sale_channel_id,
  lss.sale_subchannel_id,
  cm.sale_channel_desc AS channel_name_desc,
  lss.sale_subchannel_desc AS subchannel_name_desc,
  ROUND(st.sales_gross_amt, 5) AS venta_bruta,
  st.manager_associate_id,
  st.sales_associate_id,
  loc.location_id,
  loc.location_base_id,
  loc.location_name,
  loc.loc_store_oak_id,
  st.special_sale_storearea
FROM
  {l1_raw_catalog_name}.adw.sales_transaction AS st
  INNER JOIN
    {l2_foundation_catalog_name}.common.lk_sale_subchannel AS lss
    ON
      st.sale_subchannel_id = lss.sale_subchannel_id
  INNER JOIN
    {l2_foundation_catalog_name}.common.lk_sale_channel AS cm
    ON
      lss.sale_channel_id = cm.sale_channel_id
  INNER JOIN
    {l3_foundation_catalog_name}.common.dim_location AS loc
    ON
      st.location_id = loc.location_id AND loc.location_end_dt = '9999-12-31T00:00:00.000Z'
  INNER JOIN
    {l3_foundation_catalog_name}.common.dim_country AS cou
    ON
      st.country_id = cou.country_id AND cou.country_end_dt = '9999-12-31T00:00:00.000Z'
  INNER JOIN
    {l1_raw_catalog_name}.adw.payment_line_brasil AS pl
    ON
      st.sales_transaction_id = pl.sales_transaction_id AND cou.country_id = pl.country_id
WHERE
  st.sales_business_dt BETWEEN '{fecha_desde - timedelta(days=1)}T00:00:00.000' AND '{fecha_ayer}T23:59:59.999'
  AND lss.sale_subchannel_id IN (2001)
  AND pl.payment_subtype_id = '28_086'
  AND st.country_id = '086'
  AND st.sales_type_id IN (1, 2)
  AND loc.ownerships_desc_reporting LIKE '%ArcopCo%'

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_nc AS
SELECT DISTINCT
  sales_start_dttm,
  sales_end_dttm,
  sales_date,
  location_name,
  sales_transaction_id,
  special_sale_order_new AS special_sale_order,
  venta_bruta AS venta_bruta_nc,
  ROW_NUMBER() OVER (
    PARTITION BY special_sale_order_new
    ORDER BY
      sales_transaction_id DESC
  ) AS aux_orden_nc
FROM
  tld_br
WHERE
  sales_type_id = 2
  AND special_sale_order_new IS NOT NULL

"""
)

# COMMAND ----------

# DBTITLE 1,Duplicados
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_nc_duplicadas AS
SELECT DISTINCT special_sale_order
FROM
  cte_nc
WHERE
  aux_orden_nc > 1

"""
)

# COMMAND ----------

# DBTITLE 1,tabla ifood 3po
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_3po AS
SELECT
  p.loja_id AS merchant_id,
  l.ownerships,
  m.name AS merchant_name,
  m.corporatename,
  c.country_name_desc,
  m.`LOCAL` AS location_acronym_cd,
  FROM_UTC_TIMESTAMP(p.data_criacao_pedido_associado, c.country_timezone) AS data_criacao_pedido_associado_gmt,
  FROM_UTC_TIMESTAMP(p.data_faturamento, c.country_timezone) AS data_faturamento_gmt,
  p.*
FROM
  {l2_foundation_catalog_name}.cancelaciones.tr_ifood_reconciliation AS p
  LEFT JOIN
    {l1_raw_catalog_name}.landing.ifood_merchants AS m
    ON
      p.loja_id = m.id
  LEFT JOIN
    {l3_foundation_catalog_name}.common.dim_country AS c
    ON
      c.country_short_abbreviation_cd = 'BR'
      AND c.country_end_dt = '9999-12-31'
  LEFT JOIN
    {l1_raw_catalog_name}.adw.dim_lk_location_base AS l
    ON
      c.country_id = l.country_id AND m.`LOCAL` = l.location_acronym_cd

WHERE
  p.data_fato_gerador BETWEEN '{fecha_desde}T00:00:00.000' AND '{fecha_ayer}T23:59:59.999'

"""
)

# COMMAND ----------

# DBTITLE 1,3po integradas
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_3po_integradas AS
SELECT DISTINCT i.*
FROM
  cte_3po AS i
  INNER JOIN
    tld_br AS t
    ON
      i.pedido_associado_ifood = t.special_sale_order_new

"""
)

# COMMAND ----------

# DBTITLE 1,3po manuales
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_3po_manuales AS
SELECT DISTINCT
  p.*,
  CONCAT(CAST(p.data_criacao_pedido_associado_gmt AS DATE), p.location_acronym_cd, CAST(p.monto_cobrado AS DECIMAL(10, 2))) AS 3po_concat,


  ROW_NUMBER() OVER (
    PARTITION BY CONCAT(CAST(p.data_criacao_pedido_associado_gmt AS DATE), p.location_acronym_cd, CAST(p.monto_cobrado AS DECIMAL(10, 2)))
    ORDER BY
      1 DESC
  ) AS aux_3po_concat_orden

FROM
  cte_3po AS p
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      cte_3po_integradas AS i
    WHERE
      i.pedido_associado_ifood = p.pedido_associado_ifood
  )

"""
)


# COMMAND ----------

# DBTITLE 1,duplicados
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_concatenados_duplicados AS
SELECT DISTINCT m.3po_concat
FROM
  cte_3po_manuales AS m
WHERE
  m.aux_3po_concat_orden > 1

"""
)

# COMMAND ----------

# DBTITLE 1,3po manuales asociables
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_3po_manuales_asociables AS
SELECT m.*
FROM
  cte_3po_manuales AS m
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      cte_concatenados_duplicados AS md
    WHERE
      m.3po_concat = md.3po_concat
  )

"""
)

# COMMAND ----------

# DBTITLE 1,3po manuales no asociables
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_3po_manuales_no_asociadas AS
SELECT m.*
FROM
  cte_3po_manuales AS m
WHERE
  EXISTS (
    SELECT 1
    FROM
      cte_concatenados_duplicados AS md
    WHERE
      m.3po_concat = md.3po_concat
  )

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_tld_integradas AS
SELECT
  CONCAT(t.country_name_desc, '-', LEFT(t.location_name, 3)) AS key,
  t.*
FROM
  tld_br AS t
  INNER JOIN
    cte_3po_integradas AS i
    ON
      t.special_sale_order_new = i.pedido_associado_ifood
WHERE
  sales_type_id = 1
  AND t.special_sale_order_new IS NOT null

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_tld_manuales AS
SELECT
  a.*,
  CONCAT(a.country_name_desc, '-', LEFT(a.location_name, 3)) AS key,
  CONCAT(CAST(a.sales_end_dttm AS DATE), LEFT(a.location_name, 3), CAST(a.venta_bruta AS DECIMAL(10, 2))) AS concat,
  ROW_NUMBER() OVER (
    PARTITION BY CONCAT(LEFT(a.location_name, 3), CAST(a.sales_end_dttm AS DATE), CAST(a.venta_bruta AS DECIMAL(10, 2)))
    ORDER BY
      1 DESC
  ) AS aux_concat_orden
FROM
  tld_br AS a
WHERE
  a.sales_type_id = 1
  AND NOT EXISTS (
    SELECT 1
    FROM
      cte_tld_integradas AS i
    WHERE
      i.sales_transaction_id = a.sales_transaction_id
  )

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_tld_manuales_mismo_concat AS
SELECT DISTINCT m.concat
FROM
  cte_tld_manuales AS m
WHERE
  m.aux_concat_orden > 1

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_tld_manuales_asociables AS
SELECT DISTINCT t.*
FROM
  cte_tld_manuales AS t
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      cte_tld_manuales_mismo_concat AS m
    WHERE
      m.concat = t.concat
  )

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_tld_manuales_no_asociables AS
SELECT t.*
FROM
  cte_tld_manuales AS t
WHERE
  EXISTS (
    SELECT 1
    FROM
      cte_tld_manuales_mismo_concat AS m
    WHERE
      m.concat = t.concat
  )

"""
)

# COMMAND ----------

# DBTITLE 1,Vistas temporales para generar la tabla cte_temp integradas
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp_integradas AS

SELECT DISTINCT --transacciones tld integradas
  '3PO' AS selector,
  'Integradas' AS tipo_integracion,
  NULL AS clave_concatenada,
  a.special_sale_storearea,
  a.ownerships,
  a.country_name_desc,
  LEFT(a.location_name, 3) AS location_acronym_cd,
  a.key,
  a.sales_date,
  fecha,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.sales_transaction_id,
  a.special_sale_order_new,
  salekey,
  pos_register_id,
  SUBSTRING(pos_register_id, 0, CHARINDEX('_', pos_register_id) - 1) AS pos_register_number,
  channel_name_desc,
  subchannel_name_desc,
  integrated,
  sales_type_id,
  a.venta_bruta,
  nc.sales_transaction_id AS sales_transaction_id_nc,
  CASE WHEN nc_d.special_sale_order IS NULL THEN 0 ELSE 1 END AS nc_duplicada,
  nc.sales_date AS sales_date_nc,
  nc.sales_start_dttm AS sales_start_dttm_nc,
  nc.sales_end_dttm AS sales_end_dttm_nc,
  nc.venta_bruta_nc,
  DATEDIFF(SECOND, a.sales_end_dttm, nc.sales_end_dttm) AS tiempo_reintegro_nc_seg,
  DATEDIFF(MINUTE, a.sales_end_dttm, nc.sales_end_dttm) AS tiempo_reintegro_nc_min,
  y.data_criacao_pedido_associado_gmt AS 3po_created_at,
  y.data_faturamento_gmt AS 3po_updated_at,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado, y.data_faturamento) AS tiempo_proceso_3po_seg,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado_gmt, a.sales_start_dttm) AS tiempo_recepcion_tld_seg,
  DATEDIFF(SECOND, a.sales_start_dttm, a.sales_end_dttm) AS tiempo_transaccion_tld_seg,
  y.merchant_id,
  y.merchant_name,
  y.pedido_associado_ifood_curto AS payment_id,
  y.fato_gerador AS 3po_status,
  y.tipo_lancamento AS 3po_sub_status,
  y.monto_cobrado AS 3po_amount_value,
  y.valor_cancelado AS 3po_captured,
  y.valor_compensado AS 3po_refunded,

  CASE WHEN y.ressarcimento > 0
      THEN y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_parcial_sem_impacto + y.cancelamento_total_sem_impacto
        + venda_bruta + venda_bruta_sem_impacto
    ELSE venda_bruta + venda_bruta_sem_impacto
  END
  AS 3po_cobro_integrado,


  CASE WHEN fato_gerador IN ('Venda', 'Ocorrencia Venda') THEN a.venta_bruta - monto_cobrado
    WHEN fato_gerador = 'Cancelamento Parcial' THEN a.venta_bruta - monto_cobrado
    WHEN fato_gerador = 'Ressarcimento/Indenização' AND valor_cancelado < monto_cobrado THEN a.venta_bruta - monto_cobrado
    ELSE 0
  END AS 3po_diferencia_cobro_integrada,


  CASE WHEN (y.cancelamento_total < 0 OR y.cancelamento_parcial < 0 OR y.cancelamento_total_sem_impacto < 0 OR y.cancelamento_parcial_sem_impacto < 0) AND y.ressarcimento = 0
      THEN (y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_total_sem_impacto + y.cancelamento_parcial_sem_impacto)
    ELSE 0
  END AS 3po_cancelacion_integrada,


  CASE WHEN fato_gerador = 'Cancelamento Total' THEN a.venta_bruta - valor_cancelado
    ELSE 0
  END AS 3po_diferencia_cancelacion_integrada,

  0 AS 3po_cancelacion_parcial_integrada,

  CASE WHEN fato_gerador = 'Cancelamento Parcial' THEN a.venta_bruta - valor_cancelado
    ELSE 0
  END AS 3po_diferencia_cancelacion_parcial_integrada,



  CASE
    WHEN ressarcimento > 0 AND outros_agg > 0 THEN ressarcimento + outros_agg
    WHEN ressarcimento > 0 THEN ressarcimento
    WHEN outros_agg > 0 THEN outros_agg
    ELSE 0
  END AS 3po_compensacion_integrada,


  0 AS 3po_diferencia_compensacion_integrada,

  0 AS 3po_compensacion_parcial_integrada,

  CASE WHEN fato_gerador = 'Ressarcimento/Indenização' AND valor_cancelado < monto_cobrado THEN a.venta_bruta - valor_compensado
    ELSE 0
  END AS 3po_diferencia_compensacion_parcial_integrada,


  0 AS 3po_cobro_manual,
  0 AS 3po_diferencia_cobro_manual,
  0 AS 3po_cancelacion_manual,
  0 AS 3po_diferencia_cancelacion_manual,
  0 AS 3po_cancelacion_parcial_manual,
  0 AS 3po_diferencia_cancelacion_parcial_manual,
  0 AS 3po_compensacion_manual,
  0 AS 3po_diferencia_compensacion_manual,
  0 AS 3po_compensacion_parcial_manual,
  0 AS 3po_diferencia_compensacion_parcial_manual,

  y.motivo_cancelamento AS 3po_motivo_cancelacion,
  CASE WHEN fato_gerador = 'Ressarcimento/Indenização' THEN 'IFOOD'
    WHEN fato_gerador IN ('Cancelamento Total', 'Cancelamento Parcial') THEN 'AD'
  END AS 3po_responsable_cancelacion,

  y.responsavel_transacao AS 3po_responsable_transaccion,

  venda_bruta AS external_order_integrated_venta_bruta,
  venda_bruta_sem_impacto AS external_order_integrated_venta_bruta_sem_impacto,
  cancelamento_total AS external_order_integrated_cancelamiento_total,
  cancelamento_total_sem_impacto AS external_order_integrated_cancelamiento_total_sem_impacto,
  cancelamento_parcial AS external_order_integrated_cancelamiento_parcial,
  cancelamento_parcial_sem_impacto AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  0 AS external_order_manual_venta_bruta,
  0 AS external_order_manual_venta_bruta_sem_impacto,
  0 AS external_order_manual_cancelamiento_total,
  0 AS external_order_manual_cancelamiento_total_sem_impacto,
  0 AS external_order_manual_cancelamiento_parcial,
  0 AS external_order_manual_cancelamiento_parcial_sem_impacto

FROM
  cte_tld_integradas AS a
  INNER JOIN
    cte_3po_integradas AS y
    ON
      a.special_sale_order_new = y.pedido_associado_ifood
  LEFT JOIN
    cte_nc AS nc
    ON
      a.special_sale_order_new = nc.special_sale_order
  LEFT JOIN
    cte_nc_duplicadas AS nc_d
    ON
      nc.special_sale_order = nc_d.special_sale_order

"""
)

# COMMAND ----------

# DBTITLE 1,Vistas temporales para generar la tabla cte_temp manuales asociadas
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp_manuales_asociadas AS

SELECT DISTINCT --transacciones tld manuales asociables con 3po
  '3PO' AS selector,
  'Manuales asociadas' AS tipo_integracion,
  a.concat AS clave_concatenada,
  a.special_sale_storearea,
  a.ownerships,
  a.country_name_desc,
  LEFT(a.location_name, 3) AS location_acronym_cd,
  a.key,
  a.sales_date,
  fecha,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.sales_transaction_id,
  COALESCE(y.pedido_associado_ifood, a.special_sale_order_new),
  salekey,
  pos_register_id,
  SUBSTRING(pos_register_id, 0, CHARINDEX('_', pos_register_id) - 1) AS pos_register_number,
  channel_name_desc,
  subchannel_name_desc,
  integrated,
  sales_type_id,
  a.venta_bruta,
  null AS sales_transaction_id_nc,
  null AS nc_duplicada,
  null AS sales_date_nc,
  null AS sales_start_dttm_nc,
  null AS sales_end_dttm_nc,
  null AS venta_bruta_nc,
  null AS tiempo_reintegro_nc_seg,
  null AS tiempo_reintegro_nc_min,
  y.data_criacao_pedido_associado_gmt AS 3po_created_at,
  y.data_faturamento_gmt AS 3po_updated_at,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado, y.data_faturamento) AS tiempo_proceso_3po_seg,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado_gmt, a.sales_start_dttm) AS tiempo_recepcion_tld_seg,
  DATEDIFF(SECOND, a.sales_start_dttm, a.sales_end_dttm) AS tiempo_transaccion_tld_seg,
  y.merchant_id,
  y.merchant_name,
  y.pedido_associado_ifood_curto AS payment_id,
  y.fato_gerador AS 3po_status,
  y.tipo_lancamento AS 3po_sub_status,
  y.monto_cobrado AS 3po_amount_value,
  y.valor_cancelado AS 3po_captured,
  y.valor_compensado AS 3po_refunded,
  0 AS 3po_cobro_integrado,
  0 AS 3po_diferencia_cobro_integrada,
  0 AS 3po_cancelacion_integrada,
  0 AS 3po_diferencia_cancelacion_integrada,
  0 AS 3po_cancelacion_parcial_integrada,
  0 AS 3po_diferencia_cancelacion_parcial_integrada,
  0 AS 3po_compensacion_integrada,
  0 AS 3po_diferencia_compensacion_integrada,
  0 AS 3po_compensacion_parcial_integrada,
  0 AS 3po_diferencia_compensacion_parcial_integrada,
  CASE
    WHEN y.ressarcimento > 0
      THEN
        CAST(y.cancelamento_total AS DECIMAL)
        + CAST(y.cancelamento_parcial AS DECIMAL)
        + CAST(y.cancelamento_total_sem_impacto AS DECIMAL)
        + CAST(y.cancelamento_parcial_sem_impacto AS DECIMAL)
        + CAST(venda_bruta AS DECIMAL)
        + CAST(venda_bruta_sem_impacto AS DECIMAL)
    ELSE
      CAST(venda_bruta AS DECIMAL)
      + CAST(venda_bruta_sem_impacto AS DECIMAL)
  END AS 3po_cobro_manual,


  CASE WHEN fato_gerador IN ('Venda', 'Ocorrencia Venda') THEN a.venta_bruta - monto_cobrado
    WHEN fato_gerador = 'Cancelamento Parcial' THEN a.venta_bruta - monto_cobrado
    WHEN fato_gerador = 'Ressarcimento/Indenização' AND valor_cancelado < monto_cobrado THEN a.venta_bruta - monto_cobrado
    ELSE 0
  END AS 3po_diferencia_cobro_manual,


  CASE WHEN (
      y.cancelamento_total < 0 OR y.cancelamento_parcial < 0 OR y.cancelamento_parcial_sem_impacto
      < 0 OR y.cancelamento_total_sem_impacto < 0
    ) AND y.ressarcimento = 0
      THEN (y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_parcial_sem_impacto + y.cancelamento_total_sem_impacto)
    ELSE 0
  END AS 3po_cancelacion_manual,


  CASE WHEN fato_gerador = 'Cancelamento Total' THEN a.venta_bruta - valor_cancelado
    ELSE 0
  END AS 3po_diferencia_cancelacion_manual,

  0 AS 3po_cancelacion_parcial_manual,


  CASE WHEN fato_gerador = 'Cancelamento Parcial' THEN a.venta_bruta - valor_cancelado
    ELSE 0
  END AS 3po_diferencia_cancelacion_parcial_manual,


  CASE
    WHEN ressarcimento > 0 AND outros_agg > 0 THEN ressarcimento + outros_agg
    WHEN ressarcimento > 0 THEN ressarcimento
    WHEN outros_agg > 0 THEN outros_agg
    ELSE 0
  END AS 3po_compensacion_manual,


  0 AS 3po_diferencia_compensacion_manual,
  0 AS 3po_compensacion_parcial_manual,
  CASE WHEN fato_gerador = 'Ressarcimento/Indenização' AND valor_cancelado < monto_cobrado THEN a.venta_bruta - valor_compensado
    ELSE 0
  END AS 3po_diferencia_compensacion_parcial_manual,

  y.motivo_cancelamento AS 3po_motivo_cancelacion,
  CASE WHEN fato_gerador = 'Ressarcimento/Indenização' THEN 'IFOOD'
    WHEN fato_gerador IN ('Cancelamento Total', 'Cancelamento Parcial') THEN 'AD'
  END AS 3po_responsable_cancelacion,

  y.responsavel_transacao AS 3po_responsable_transaccion,

  0 AS external_order_integrated_venta_bruta,
  0 AS external_order_integrated_venta_bruta_sem_impacto,
  0 AS external_order_integrated_cancelamiento_total,
  0 AS external_order_integrated_cancelamiento_total_sem_impacto,
  0 AS external_order_integrated_cancelamiento_parcial,
  0 AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  venda_bruta AS external_order_manual_venta_bruta,
  venda_bruta_sem_impacto AS external_order_manual_venta_bruta_sem_impacto,
  cancelamento_total AS external_order_manual_cancelamiento_total,
  cancelamento_total_sem_impacto AS external_order_manual_cancelamiento_total_sem_impacto,
  cancelamento_parcial AS external_order_manual_cancelamiento_parcial,
  cancelamento_parcial_sem_impacto AS external_order_manual_cancelamiento_parcial_sem_impacto



FROM
  cte_tld_manuales_asociables AS a
  INNER JOIN
    cte_3po_manuales_asociables AS y
    ON
      a.concat = y.3po_concat

"""
)

# COMMAND ----------

# DBTITLE 1,Vistas temporales para generar la tabla cte_temp
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp_manuales_no_asociadas AS
SELECT DISTINCT --transacciones tld manuales que no pudieron encontrarse por concatenado
  '3PO' AS selector,
  'Manuales no asociadas' AS tipo_integracion,
  a.concat AS clave_concatenada,
  a.special_sale_storearea,
  a.ownerships,
  a.country_name_desc,
  LEFT(a.location_name, 3) AS location_acronym_cd,
  a.key,
  a.sales_date,
  fecha,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.sales_transaction_id,
  a.special_sale_order_new,
  salekey,
  pos_register_id,
  SUBSTRING(pos_register_id, 0, CHARINDEX('_', pos_register_id) - 1) AS pos_register_number,
  channel_name_desc,
  subchannel_name_desc,
  integrated,
  sales_type_id,
  a.venta_bruta,
  null AS sales_transaction_id_nc,
  null AS nc_duplicada,
  null AS sales_date_nc,
  null AS sales_start_dttm_nc,
  null AS sales_end_dttm_nc,
  null AS venta_bruta_nc,
  null AS tiempo_reintegro_nc_seg,
  null AS tiempo_reintegro_nc_min,
  null AS 3po_created_at,
  null AS 3po_updated_at,
  null AS tiempo_proceso_3po_seg,
  null AS tiempo_recepcion_tld_seg,
  DATEDIFF(SECOND, a.sales_start_dttm, a.sales_end_dttm) AS tiempo_transaccion_tld_seg,
  null AS merchant_id,
  null AS merchant_name,
  null AS payment_id,
  null AS 3po_status,
  null AS 3po_sub_status,
  null AS 3po_amount_value,
  null AS 3po_captured,
  null AS 3po_refunded,
  0 AS 3po_cobro_integrado,
  0 AS 3po_diferencia_cobro_integrada,
  0 AS 3po_cancelacion_integrada,
  0 AS 3po_diferencia_cancelacion_integrada,
  0 AS 3po_cancelacion_parcial_integrada,
  0 AS 3po_diferencia_cancelacion_parcial_integrada,
  0 AS 3po_compensacion_integrada,
  0 AS 3po_diferencia_compensacion_integrada,
  0 AS 3po_compensacion_parcial_integrada,
  0 AS 3po_diferencia_compensacion_parcial_integrada,
  0 AS 3po_cobro_manual,
  0 AS 3po_diferencia_cobro_manual,
  0 AS 3po_cancelacion_manual,
  0 AS 3po_diferencia_cancelacion_manual,
  0 AS 3po_cancelacion_parcial_manual,
  0 AS 3po_diferencia_cancelacion_parcial_manual,
  0 AS 3po_compensacion_manual,
  0 AS 3po_diferencia_compensacion_manual,
  0 AS 3po_compensacion_parcial_manual,
  0 AS 3po_diferencia_compensacion_parcial_manual,

  0 AS external_order_integrated_venta_bruta,
  0 AS external_order_integrated_venta_bruta_sem_impacto,
  0 AS external_order_integrated_cancelamiento_total,
  0 AS external_order_integrated_cancelamiento_total_sem_impacto,
  0 AS external_order_integrated_cancelamiento_parcial,
  0 AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  0 AS external_order_manual_venta_bruta,
  0 AS external_order_manual_venta_bruta_sem_impacto,
  0 AS external_order_manual_cancelamiento_total,
  0 AS external_order_manual_cancelamiento_total_sem_impacto,
  0 AS external_order_manual_cancelamiento_parcial,
  0 AS external_order_manual_cancelamiento_parcial_sem_impacto,

  null AS 3po_motivo_cancelacion,
  null AS 3po_responsable_cancelacion,
  null AS 3po_responsable_transaccion

FROM
  cte_tld_manuales_asociables AS a
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      cte_3po_manuales_asociables AS y
    WHERE
      y.3po_concat = a.concat
  )

UNION DISTINCT

SELECT DISTINCT --transaciones tld manuales no asociadas
  '3PO' AS selector,
  'Manuales no asociadas' AS tipo_integracion,
  a.concat AS clave_concatenada,
  a.special_sale_storearea,
  a.ownerships,
  country_name_desc,
  LEFT(a.location_name, 3) AS location_acronym_cd,
  a.key,
  a.sales_date,
  fecha,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.sales_transaction_id,
  a.special_sale_order_new,
  salekey,
  pos_register_id,
  SUBSTRING(pos_register_id, 0, CHARINDEX('_', pos_register_id) - 1) AS pos_register_number,
  channel_name_desc,
  subchannel_name_desc,
  integrated,
  sales_type_id,
  a.venta_bruta,
  null AS sales_transaction_id_nc,
  null,
  null AS sales_date_nc,
  null AS sales_start_dttm_nc,
  null AS sales_end_dttm_nc,
  null AS venta_bruta_nc,
  null AS tiempo_reintegro_nc_seg,
  null AS tiempo_reintegro_nc_min,
  null AS 3po_created_at,
  null AS 3po_updated_at,
  null AS tiempo_proceso_3po_seg,
  null AS tiempo_recepcion_tld_seg,
  DATEDIFF(SECOND, a.sales_start_dttm, a.sales_end_dttm) AS tiempo_transaccion_tld_seg,
  null AS merchant_id,
  null AS merchant_name,
  null AS payment_id,
  null AS 3po_status,
  null AS 3po_sub_status,
  null AS 3po_amount_value,
  null AS 3po_captured,
  null AS 3po_refunded,
  0 AS 3po_cobro_integrado,
  0 AS 3po_diferencia_cobro_integrada,
  0 AS 3po_cancelacion_integrada,
  0 AS 3po_diferencia_cancelacion_integrada,
  0 AS 3po_cancelacion_parcial_integrada,
  0 AS 3po_diferencia_cancelacion_parcial_integrada,
  0 AS 3po_compensacion_integrada,
  0 AS 3po_diferencia_compensacion_integrada,
  0 AS 3po_compensacion_parcial_integrada,
  0 AS 3po_diferencia_compensacion_parcial_integrada,
  0 AS 3po_cobro_manual,
  0 AS 3po_diferencia_cobro_manual,
  0 AS 3po_cancelacion_manual,
  0 AS 3po_diferencia_cancelacion_manual,
  0 AS 3po_cancelacion_parcial_manual,
  0 AS 3po_diferencia_cancelacion_parcial_manual,
  0 AS 3po_compensacion_manual,
  0 AS 3po_diferencia_compensacion_manual,
  0 AS 3po_compensacion_parcial_manual,
  0 AS 3po_diferencia_compensacion_parcial_manual,


  null AS 3po_motivo_cancelacion,
  null AS 3po_responsable_cancelacion,
  null AS 3po_responsable_transaccion,

  0 AS external_order_integrated_venta_bruta,
  0 AS external_order_integrated_venta_bruta_sem_impacto,
  0 AS external_order_integrated_cancelamiento_total,
  0 AS external_order_integrated_cancelamiento_total_sem_impacto,
  0 AS external_order_integrated_cancelamiento_parcial,
  0 AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  0 AS external_order_manual_venta_bruta,
  0 AS external_order_manual_venta_bruta_sem_impacto,
  0 AS external_order_manual_cancelamiento_total,
  0 AS external_order_manual_cancelamiento_total_sem_impacto,
  0 AS external_order_manual_cancelamiento_parcial,
  0 AS external_order_manual_cancelamiento_parcial_sem_impacto

FROM
  cte_tld_manuales_no_asociables AS a

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp_sin_integracion AS
SELECT DISTINCT
  '3PO' AS selector,
  '3po sin integración' AS tipo_integracion,
  null AS clave_concatenada,
  null AS special_sale_storearea,
  y.ownerships,
  y.country_name_desc,
  y.location_acronym_cd,
  CONCAT(y.country_name_desc, '-', LEFT(y.location_acronym_cd, 3)) AS key,
  null AS sales_date,
  null AS fecha,
  null AS sales_start_dttm,
  null AS sales_end_dttm,
  null AS sales_transaction_id,
  y.pedido_associado_ifood AS special_sales_order,
  null AS salekey,
  null AS pos_register_id,
  null AS pos_register_number,
  null AS channel_name_desc,
  null AS subchannel_name_desc,
  null AS integrated,
  null AS sales_type_id,
  null AS venta_bruta,
  null AS sales_transaction_id_nc,
  null AS nc_duplicada,
  null AS sales_date_nc,
  null AS sales_start_dttm_nc,
  null AS sales_end_dttm_nc,
  null AS venta_bruta_nc,
  null AS tiempo_reintegro_nc_seg,
  null AS tiempo_reintegro_nc_min,
  y.data_criacao_pedido_associado_gmt AS 3po_created_at,
  y.data_faturamento_gmt AS 3po_updated_at,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado, y.data_faturamento) AS tiempo_proceso_3po_seg,
  0 AS tiempo_recepcion_tld_seg,
  0 AS tiempo_transaccion_tld_seg,
  y.merchant_id,
  y.merchant_name,
  y.pedido_associado_ifood_curto AS payment_id,
  y.fato_gerador AS 3po_status,
  y.tipo_lancamento AS 3po_sub_status,
  y.monto_cobrado AS 3po_amount_value,
  y.valor_cancelado AS 3po_captured,
  y.valor_compensado AS 3po_refunded,
  0 AS 3po_cobro_integrado,
  0 AS 3po_diferencia_cobro_integrada,
  0 AS 3po_cancelacion_integrada,
  0 AS 3po_diferencia_cancelacion_integrada,
  0 AS 3po_cancelacion_parcial_integrada,
  0 AS 3po_diferencia_cancelacion_parcial_integrada,
  0 AS 3po_compensacion_integrada,
  0 AS 3po_diferencia_compensacion_integrada,
  0 AS 3po_compensacion_parcial_integrada,
  0 AS 3po_diferencia_compensacion_parcial_integrada,


  CASE
    WHEN y.ressarcimento > 0
      THEN
        CAST(y.cancelamento_total AS DECIMAL)
        + CAST(y.cancelamento_parcial AS DECIMAL)
        + CAST(y.cancelamento_total_sem_impacto AS DECIMAL)
        + CAST(y.cancelamento_parcial_sem_impacto AS DECIMAL)
        + CAST(y.venda_bruta AS DECIMAL)
        + CAST(y.venda_bruta_sem_impacto AS DECIMAL)
    ELSE
      CAST(y.venda_bruta AS DECIMAL)
      + CAST(y.venda_bruta_sem_impacto AS DECIMAL)
  END AS 3po_cobro_manual,


  0 AS 3po_diferencia_cobro_manual,

  CASE WHEN (
      y.cancelamento_total < 0 OR y.cancelamento_parcial < 0 OR y.cancelamento_total_sem_impacto
      < 0 OR y.cancelamento_parcial_sem_impacto < 0
    ) AND y.ressarcimento = 0
      THEN (y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_parcial_sem_impacto + y.cancelamento_total_sem_impacto)
    ELSE 0
  END AS 3po_cancelacion_manual,

  0 AS 3po_diferencia_cancelacion_manual,

  0 AS 3po_cancelacion_parcial_manual,


  0 AS 3po_diferencia_cancelacion_parcial_manual,

  CASE
    WHEN y.ressarcimento > 0 AND y.outros_agg > 0 THEN y.ressarcimento + y.outros_agg
    WHEN y.ressarcimento > 0 THEN y.ressarcimento
    WHEN y.outros_agg > 0 THEN y.outros_agg
    ELSE 0
  END AS 3po_compensacion_manual,

  0 AS 3po_diferencia_compensacion_manual,

  0 AS 3po_compensacion_parcial_manual,

  0 AS 3po_diferencia_compensacion_parcial_manual,
  y.motivo_cancelamento AS 3po_motivo_cancelacion,
  CASE WHEN y.fato_gerador = 'Ressarcimento/Indenização' THEN 'IFOOD'
    WHEN y.fato_gerador IN ('Cancelamento Total', 'Cancelamento Parcial') THEN 'AD'
  END AS 3po_responsable_cancelacion,

  y.responsavel_transacao AS 3po_responsable_transaccion,

  0 AS external_order_integrated_venta_bruta,
  0 AS external_order_integrated_venta_bruta_sem_impacto,
  0 AS external_order_integrated_cancelamiento_total,
  0 AS external_order_integrated_cancelamiento_total_sem_impacto,
  0 AS external_order_integrated_cancelamiento_parcial,
  0 AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  y.venda_bruta AS external_order_manual_venta_bruta,
  y.venda_bruta_sem_impacto AS external_order_manual_venta_bruta_sem_impacto,
  y.cancelamento_total AS external_order_manual_cancelamiento_total,
  y.cancelamento_total_sem_impacto AS external_order_manual_cancelamiento_total_sem_impacto,
  y.cancelamento_parcial AS external_order_manual_cancelamiento_parcial,
  y.cancelamento_parcial_sem_impacto AS external_order_manual_cancelamiento_parcial_sem_impacto


FROM
  cte_3po_manuales_no_asociadas AS y

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp_sin_integracion_1 AS
SELECT DISTINCT
  '3PO' AS selector,
  '3po sin integración' AS tipo_integracion,
  null AS clave_concatenada,
  null AS special_sale_storearea,
  y.ownerships,
  y.country_name_desc,
  y.location_acronym_cd,
  CONCAT(y.country_name_desc, '-', LEFT(y.location_acronym_cd, 3)) AS key,
  null AS sales_date,
  null AS fecha,
  null AS sales_start_dttm,
  null AS sales_end_dttm,
  null AS sales_transaction_id,
  y.pedido_associado_ifood AS special_sales_order,
  null AS salekey,
  null AS pos_register_id,
  null AS pos_register_number,
  null AS channel_name_desc,
  null AS subchannel_name_desc,
  null AS integrated,
  null AS sales_type_id,
  null AS venta_bruta,
  null AS sales_transaction_id_nc,
  null AS nc_duplicada,
  null AS sales_date_nc,
  null AS sales_start_dttm_nc,
  null AS sales_end_dttm_nc,
  null AS venta_bruta_nc,
  null AS tiempo_reintegro_nc_seg,
  null AS tiempo_reintegro_nc_min,
  y.data_criacao_pedido_associado_gmt AS 3po_created_at,
  y.data_faturamento_gmt AS 3po_updated_at,
  DATEDIFF(SECOND, y.data_criacao_pedido_associado, y.data_faturamento) AS tiempo_proceso_3po_seg,
  0 AS tiempo_recepcion_tld_seg,
  0 AS tiempo_transaccion_tld_seg,
  y.merchant_id,
  y.merchant_name,
  y.pedido_associado_ifood_curto AS payment_id,
  y.fato_gerador AS 3po_status,
  y.tipo_lancamento AS 3po_sub_status,
  y.monto_cobrado AS 3po_amount_value,
  y.valor_cancelado AS 3po_captured,
  y.valor_compensado AS 3po_refunded,
  0 AS 3po_cobro_integrado,
  0 AS 3po_diferencia_cobro_integrada,
  0 AS 3po_cancelacion_integrada,
  0 AS 3po_diferencia_cancelacion_integrada,
  0 AS 3po_cancelacion_parcial_integrada,
  0 AS 3po_diferencia_cancelacion_parcial_integrada,
  0 AS 3po_compensacion_integrada,
  0 AS 3po_diferencia_compensacion_integrada,
  0 AS 3po_compensacion_parcial_integrada,
  0 AS 3po_diferencia_compensacion_parcial_integrada,

  CASE WHEN y.ressarcimento > 0
      THEN y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_parcial_sem_impacto + y.cancelamento_total_sem_impacto
        + y.venda_bruta + y.venda_bruta_sem_impacto
    ELSE y.venda_bruta + y.venda_bruta_sem_impacto
  END
  AS 3po_cobro_manual,

  0 AS 3po_diferencia_cobro_manual,


  CASE WHEN (
      y.cancelamento_total < 0 OR y.cancelamento_parcial < 0 OR y.cancelamento_total_sem_impacto
      < 0 OR y.cancelamento_parcial_sem_impacto < 0
    ) AND y.ressarcimento = 0
      THEN (y.cancelamento_total + y.cancelamento_parcial + y.cancelamento_parcial_sem_impacto + y.cancelamento_total_sem_impacto)
    ELSE 0
  END AS 3po_cancelacion_manual,

  0 AS 3po_diferencia_cancelacion_manual,

  0 AS 3po_cancelacion_parcial_manual,


  0 AS 3po_diferencia_cancelacion_parcial_manual,

  CASE
    WHEN y.ressarcimento > 0 AND y.outros_agg > 0 THEN y.ressarcimento + y.outros_agg
    WHEN y.ressarcimento > 0 THEN y.ressarcimento
    WHEN y.outros_agg > 0 THEN y.outros_agg
    ELSE 0
  END AS 3po_compensacion_manual,

  0 AS 3po_diferencia_compensacion_manual,


  0 AS 3po_compensacion_parcial_manual,

  0 AS 3po_diferencia_compensacion_parcial_manual,

  y.motivo_cancelamento AS 3po_motivo_cancelacion,
  CASE WHEN y.fato_gerador = 'Ressarcimento/Indenização' THEN 'IFOOD'
    WHEN y.fato_gerador IN ('Cancelamento Total', 'Cancelamento Parcial') THEN 'AD'
  END AS 3po_responsable_cancelacion,

  y.responsavel_transacao AS 3po_responsable_transaccion,

  0 AS external_order_integrated_venta_bruta,
  0 AS external_order_integrated_venta_bruta_sem_impacto,
  0 AS external_order_integrated_cancelamiento_total,
  0 AS external_order_integrated_cancelamiento_total_sem_impacto,
  0 AS external_order_integrated_cancelamiento_parcial,
  0 AS external_order_integrated_cancelamiento_parcial_sem_impacto,

  y.venda_bruta AS external_order_manual_venta_bruta,
  y.venda_bruta_sem_impacto AS external_order_manual_venta_bruta_sem_impacto,
  y.cancelamento_total AS external_order_manual_cancelamiento_total,
  y.cancelamento_total_sem_impacto AS external_order_manual_cancelamiento_total_sem_impacto,
  y.cancelamento_parcial AS external_order_manual_cancelamiento_parcial,
  y.cancelamento_parcial_sem_impacto AS external_order_manual_cancelamiento_parcial_sem_impacto


FROM
  cte_3po_manuales_asociables AS y
WHERE
  NOT EXISTS (
    SELECT 1
    FROM
      cte_tld_manuales_asociables AS a
    WHERE
      y.3po_concat = a.concat
  )

"""
)

# COMMAND ----------

# DBTITLE 1,Creacion de la tabla cte_temp con union de las demas tablas
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp AS

SELECT *
FROM
  cte_temp_integradas

UNION DISTINCT

SELECT *
FROM
  cte_temp_manuales_asociadas

UNION DISTINCT

SELECT *
FROM
  cte_temp_manuales_no_asociadas

UNION DISTINCT

SELECT *
FROM
  cte_temp_sin_integracion

UNION DISTINCT

SELECT *
FROM
  cte_temp_sin_integracion_1

"""
)

# COMMAND ----------


spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_temp2 AS
SELECT
  CASE WHEN fecha IS null THEN CAST(`3po_created_at` AS DATE) ELSE CAST(fecha AS DATE) END AS sales_business_dt,
  *,

  CASE
    WHEN 3po_status = 'Ressarcimento/Indenização' AND `3po_captured` < `3po_amount_value` AND sales_transaction_id IS NOT null THEN 'Compensada parcialmente'
    WHEN 3po_status = 'Ressarcimento/Indenização' AND `3po_captured` >= `3po_amount_value` AND sales_transaction_id IS NOT null THEN 'Compensada'
    WHEN 3po_status = 'Cancelamento Parcial' AND sales_transaction_id IS NOT null THEN 'Cancelada parcialmente'
    WHEN 3po_status = 'Cancelamento Total' AND sales_transaction_id IS NOT null THEN 'Cancelada'
    WHEN 3po_status = 'Venda' AND sales_transaction_id IS NOT null THEN 'Cobrada'
    WHEN 3po_status = 'Ocorrencia Venda' AND sales_transaction_id IS NOT null THEN 'Cobrada por tercero'
    WHEN sales_transaction_id IS null OR 3po_status IS null THEN 'No encontrada'
  END AS estado_transacccion,

  CASE WHEN nc_duplicada = 1 THEN 'Nota de crédito duplicada'
    WHEN sales_transaction_id_nc IS null THEN 'No aplica NC'
    WHEN sales_transaction_id_nc IS NOT null THEN 'Nota de crédito aplicada'
  END AS estado_nc,

  ROW_NUMBER() OVER (
    PARTITION BY sales_transaction_id
    ORDER BY
      1
  ) AS aux_orden

FROM
  cte_temp

"""
)

# COMMAND ----------

# DBTITLE 1,Cruce con currency_rate

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_currency_rate AS

SELECT
  cr.source_currency_cd,
  DATE_FORMAT(cr.curr_trans_calendar_dt, 'yMM') AS calendar_month_id,
  cr.source_to_target_currency_rate
FROM
  {l2_foundation_catalog_name}.common.hist_currency_translation_rate AS cr
WHERE
  cr.currency_type_id = 4
  AND CURRENT_DATE BETWEEN cr.curr_translation_rate_start_dt AND cr.curr_translation_rate_end_dt

"""
)

# COMMAND ----------

spark.sql(f"""

CREATE OR REPLACE TEMP VIEW ifood_vista AS
SELECT
  DATE_FORMAT(sales_business_dt, 'yMM') AS calendar_month_id,
  cr.source_to_target_currency_rate,
  sales_business_dt,
  selector,
  tipo_integracion AS integration_type,
  CASE WHEN tipo_integracion IN ('Integradas', 'Manuales asociadas') THEN 'Integrado' ELSE 'Manual' END AS integration_group,
  clave_concatenada AS concat_key,
  ownerships,
  special_sale_storearea,
  t.country_name_desc,
  location_acronym_cd,
  key,
  merchant_id AS external_order_merchant_id,
  t.sales_date,
  fecha AS sales_business_date,
  sales_start_dttm,
  sales_end_dttm,
  sales_transaction_id,
  special_sale_order_new AS special_sale_order,
  salekey,
  pos_register_id,
  pos_register_number,
  channel_name_desc,
  subchannel_name_desc,
  integrated,
  sales_type_id,
  venta_bruta AS tld_gross_sale,
  sales_transaction_id_nc,
  nc_duplicada AS nc_duplicated,
  t.sales_date_nc,
  sales_start_dttm_nc,
  sales_end_dttm_nc,
  venta_bruta_nc AS nc_gross_sale,
  tiempo_reintegro_nc_seg AS nc_time_seg,
  tiempo_reintegro_nc_min AS nc_time_min,
  3po_created_at AS external_order_created_at_gmt,
  3po_updated_at AS external_order_updated_at_gmt,
  null AS external_order_created_at,
  null AS external_order_updated_at,
  tiempo_proceso_3po_seg AS process_time_external_order_seg,
  tiempo_recepcion_tld_seg AS reception_time_tld_seg,
  tiempo_transaccion_tld_seg AS tx_time_tld_seg,
  merchant_name AS external_order_description,
  t.payment_id AS external_order_payment_id,
  'IFOOD' AS external_order_provider_id,
  3po_status AS external_order_status,
  3po_sub_status AS external_order_sub_status,
  3po_amount_value AS external_order_amount_value,
  3po_captured AS external_order_captured_value,
  3po_refunded AS external_order_refunded_value,
  'null' AS external_order_payment_type,
  'null' AS external_order_payment_method,
  'null' AS external_order_payment_brand,
  3po_responsable_transaccion AS external_order_liability,
  3po_cobro_integrado AS external_order_integrated_payment_value,
  3po_diferencia_cobro_integrada AS external_order_integrated_payment_diff,
  3po_cancelacion_integrada AS external_order_integrated_cancelation_value,
  3po_diferencia_cancelacion_integrada AS external_order_integrated_cancelation_diff,
  3po_cobro_manual AS external_order_manual_payment_value,
  3po_diferencia_cobro_manual AS external_order_manual_payment_diff,
  3po_cancelacion_manual AS external_order_manual_cancelation_value,
  3po_diferencia_cancelacion_manual AS external_order_manual_cancelation_diff,
  3po_compensacion_integrada AS external_order_integrated_refunded_value,
  3po_diferencia_compensacion_integrada AS external_order_integrated_refunded_diff,
  3po_compensacion_manual AS external_order_manual_refunded_value,
  3po_diferencia_compensacion_manual AS external_order_manual_refunded_diff,
  3po_compensacion_parcial_integrada AS external_order_integrated_partially_refunded_value,
  3po_compensacion_parcial_manual AS external_order_manual_partially_refunded_value,
  3po_cancelacion_parcial_integrada AS external_order_integrated_partially_cancelation_value,
  3po_cancelacion_parcial_manual AS external_order_manual_partially_cancelation_value,
  estado_transacccion AS transaction_status,
  estado_nc AS nc_status,
  'null' AS external_order_cancellation_date,
  `3po_responsable_cancelacion` AS external_order_cancellation_liability,
  `3po_motivo_cancelacion` AS external_order_cancellation_code_description,

  external_order_integrated_venta_bruta,
  external_order_integrated_venta_bruta_sem_impacto,
  external_order_integrated_cancelamiento_total,
  external_order_integrated_cancelamiento_total_sem_impacto,
  external_order_integrated_cancelamiento_parcial,
  external_order_integrated_cancelamiento_parcial_sem_impacto,

  external_order_manual_venta_bruta,
  external_order_manual_venta_bruta_sem_impacto,
  external_order_manual_cancelamiento_total,
  external_order_manual_cancelamiento_total_sem_impacto,
  external_order_manual_cancelamiento_parcial,
  external_order_manual_cancelamiento_parcial_sem_impacto,

  '{pipeline_run_id}' AS adls_audit_run_id,
  FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'UTC-3') AS adls_audit_date
FROM
  cte_temp2 AS t
  LEFT JOIN
    {l2_foundation_catalog_name}.adw.lk_country AS c
    ON
      t.country_name_desc = c.country_name_desc
  LEFT JOIN
    cte_currency_rate AS cr
    ON
      c.currency_cd = cr.source_currency_cd
      AND DATE_FORMAT(sales_business_dt, 'yMM') = cr.calendar_month_id

"""
)

# COMMAND ----------

# DBTITLE 1,Creacion de tabla temporal final
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW deteccion_fraudes_ifood_temp AS
SELECT *
FROM
  ifood_vista

"""
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Load new data into target table

# COMMAND ----------

sql_clause = f""" sales_business_dt BETWEEN '{fecha_desde - timedelta(days=1)}T00:00:00.000' AND '{fecha_ayer}T23:59:59.999'"""

# COMMAND ----------

load_table_replace(f"{table_full_name}", 
                      'DETECCION_FRAUDES_IFOOD_TEMP',
                      sql_clause,
                      vacuum_retain_days=1,
                      run_id=pipeline_run_id,
                      optimize_flg=True
                      )