# Databricks notebook source
# MAGIC %run "/Data Analytics/01- Circuito Industrial/00- Common/00.01_init_variables"

# COMMAND ----------

# MAGIC %run "/Data Analytics/01- Circuito Industrial/00- Common/00.02_load_table_include"

# COMMAND ----------

# MAGIC %md
# MAGIC # 1. Define widgets
# MAGIC

# COMMAND ----------

dbutils.widgets.text('fecha_ayer', '')
dbutils.widgets.text('mercados', '', 'Country IDs (tuple) e.g., ("080", "131")') # Example default for UY, CR
dbutils.widgets.text('pipeline_run_id', '')

# COMMAND ----------

# MAGIC %md
# MAGIC # 2. Initialize variables and functions
# MAGIC

# COMMAND ----------

from datetime import datetime, timedelta

# COMMAND ----------

# Get widget values
fecha_ayer = dbutils.widgets.get('fecha_ayer').strip() if dbutils.widgets.get('fecha_ayer').strip() != '' else (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
mercados = dbutils.widgets.get('mercados').strip() # Keep as string, will be used directly in SQL IN clause
pipeline_run_id = dbutils.widgets.get('pipeline_run_id').strip() if dbutils.widgets.get('pipeline_run_id').strip() != '' else 'Ejecución Manual'

# Get target table details using the helper function
_, _, _, table_full_name = get_table_full_name()

# COMMAND ----------

try:
    fecha_ayer_date = datetime.strptime(fecha_ayer, '%Y-%m-%d')
    primer_dia_mes = f"{fecha_ayer_date.year}-{fecha_ayer_date.month:02d}-01"
    primer_dia_mes_date = datetime.strptime(primer_dia_mes, '%Y-%m-%d')
    ventana_dias_num = (fecha_ayer_date - primer_dia_mes_date).days + 1 # +1 to include the first day

    # Define processing window logic
    if ventana_dias_num < 10:
        dias_ventana = -9 # Process last 10 days including fecha_ayer (fecha_ayer - 9 days)


    else:
        dias_ventana = (ventana_dias_num -1) * -1 # Process from the 1st of the month up to fecha_ayer
    print(f"Processing Window Start Date Offset (dias_ventana): {dias_ventana} days")
    start_date_calc = fecha_ayer_date + timedelta(days=dias_ventana)
    print(f"Calculated Processing Start Date: {start_date_calc.strftime('%Y-%m-%d')}")
    print(f"Calculated Processing End Date: {fecha_ayer}")
    dias_ventana = -30

except ValueError as e:
    raise ValueError(f"Error parsing date 'fecha_ayer': {fecha_ayer}. Ensure format is YYYY-MM-DD. Error: {e}")


# COMMAND ----------

# MAGIC %md
# MAGIC # 3. Create or alter table
# MAGIC

# COMMAND ----------

# Define metadata for the table
dict_table_metadata = {
    "comment": "Esta tabla contiene datos para la detección de fraudes de Yuno, incluyendo detalles de ventas, transacciones y cancelaciones.",
    "columns": [
        {"name": "calendar_month_id", "type": "STRING", "comment": "Identificador del mes calendario."},
        {"name": "id", "type": "STRING", "comment": "Identificador único de la transacción, parte de la clave primaria."},
        {"name": "source_to_target_currency_rate", "type": "STRING", "comment": "Tasa de cambio de la moneda de origen a la de destino."},
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
        {"name": "sales_date", "type": "TIMESTAMP", "comment": "Fecha de la venta."},
        {"name": "sales_business_date", "type": "TIMESTAMP", "comment": "Fecha y hora comercial de la venta."},
        {"name": "sales_start_dttm", "type": "TIMESTAMP", "comment": "Fecha y hora de inicio de la venta."},
        {"name": "sales_end_dttm", "type": "TIMESTAMP", "comment": "Fecha y hora de fin de la venta."},
        {"name": "sales_transaction_id", "type": "BIGINT", "comment": "ID de la transacción de venta."},
        {"name": "special_sale_order", "type": "STRING", "comment": "Pedido de venta especial."},
        {"name": "salekey", "type": "STRING", "comment": "Clave de la venta."},
        {"name": "pos_register_id", "type": "STRING", "comment": "ID de la caja registradora (POS)."},
        {"name": "pos_register_number", "type": "STRING", "comment": "Número de la caja registradora (POS)."},
        {"name": "channel_name_desc", "type": "STRING", "comment": "Descripción del canal de venta."},
        {"name": "subchannel_name_desc", "type": "STRING", "comment": "Descripción del subcanal de venta."},
        {"name": "integrated", "type": "FLOAT", "comment": "Valor que indica si la transacción fue integrada."},
        {"name": "sales_type_id", "type": "STRING", "comment": "ID del tipo de venta."},
        {"name": "tld_gross_sale", "type": "FLOAT", "comment": "Venta bruta de TLD."},
        {"name": "sales_transaction_id_nc", "type": "BIGINT", "comment": "ID de transacción de la nota de crédito."},
        {"name": "nc_duplicated", "type": "BIGINT", "comment": "Indicador de nota de crédito duplicada."},
        {"name": "sales_date_nc", "type": "TIMESTAMP", "comment": "Fecha de la nota de crédito."},
        {"name": "sales_start_dttm_nc", "type": "TIMESTAMP", "comment": "Fecha y hora de inicio de la nota de crédito."},
        {"name": "sales_end_dttm_nc", "type": "TIMESTAMP", "comment": "Fecha y hora de fin de la nota de crédito."},
        {"name": "nc_gross_sale", "type": "FLOAT", "comment": "Venta bruta de la nota de crédito."},
        {"name": "nc_time_seg", "type": "BIGINT", "comment": "Tiempo en segundos de la nota de crédito."},
        {"name": "nc_time_min", "type": "BIGINT", "comment": "Tiempo en minutos de la nota de crédito."},
        {"name": "external_order_created_at_gmt", "type": "STRING", "comment": "Fecha de creación del pedido externo (GMT)."},
        {"name": "external_order_updated_at_gmt", "type": "STRING", "comment": "Fecha de actualización del pedido externo (GMT)."},
        {"name": "external_order_created_at", "type": "TIMESTAMP", "comment": "Fecha de creación del pedido externo (local)."},
        {"name": "external_order_updated_at", "type": "TIMESTAMP", "comment": "Fecha de actualización del pedido externo (local)."},
        {"name": "process_time_external_order_seg", "type": "BIGINT", "comment": "Tiempo de procesamiento del pedido externo en segundos."},
        {"name": "reception_time_tld_seg", "type": "BIGINT", "comment": "Tiempo de recepción en TLD en segundos."},
        {"name": "tx_time_tld_seg", "type": "BIGINT", "comment": "Tiempo de transacción en TLD en segundos."},
        {"name": "external_order_description", "type": "STRING", "comment": "Descripción del pedido externo."},
        {"name": "external_order_payment_id", "type": "STRING", "comment": "ID del pago del pedido externo."},
        {"name": "external_order_provider_id", "type": "STRING", "comment": "ID del proveedor del pedido externo."},
        {"name": "external_order_status", "type": "STRING", "comment": "Estado del pedido externo."},
        {"name": "external_order_sub_status", "type": "STRING", "comment": "Sub-estado del pedido externo."},
        {"name": "external_order_amount_value", "type": "FLOAT", "comment": "Valor del monto del pedido externo."},
        {"name": "external_order_captured_value", "type": "STRING", "comment": "Valor capturado del pedido externo."},
        {"name": "external_order_refunded_value", "type": "STRING", "comment": "Valor reembolsado del pedido externo."},
        {"name": "external_order_payment_type", "type": "STRING", "comment": "Tipo de pago del pedido externo."},
        {"name": "external_order_payment_method", "type": "STRING", "comment": "Método de pago del pedido externo."},
        {"name": "external_order_payment_brand", "type": "STRING", "comment": "Marca del método de pago del pedido externo."},
        {"name": "external_order_liability", "type": "STRING", "comment": "Responsabilidad del pedido externo."},
        {"name": "external_order_integrated_payment_value", "type": "FLOAT", "comment": "Valor del pago integrado del pedido externo."},
        {"name": "external_order_integrated_payment_diff", "type": "FLOAT", "comment": "Diferencia del pago integrado."},
        {"name": "external_order_integrated_cancelation_value", "type": "FLOAT", "comment": "Valor de la cancelación integrada."},
        {"name": "external_order_integrated_cancelation_diff", "type": "FLOAT", "comment": "Diferencia de la cancelación integrada."},
        {"name": "external_order_manual_payment_value", "type": "FLOAT", "comment": "Valor del pago manual."},
        {"name": "external_order_manual_payment_diff", "type": "FLOAT", "comment": "Diferencia del pago manual."},
        {"name": "external_order_manual_cancelation_value", "type": "FLOAT", "comment": "Valor de la cancelación manual."},
        {"name": "external_order_manual_cancelation_diff", "type": "FLOAT", "comment": "Diferencia de la cancelación manual."},
        {"name": "external_order_integrated_refunded_value", "type": "FLOAT", "comment": "Valor reembolsado integrado."},
        {"name": "external_order_integrated_refunded_diff", "type": "FLOAT", "comment": "Diferencia del reembolso integrado."},
        {"name": "external_order_manual_refunded_value", "type": "FLOAT", "comment": "Valor reembolsado manual."},
        {"name": "external_order_manual_refunded_diff", "type": "FLOAT", "comment": "Diferencia del reembolso manual."},
        {"name": "external_order_integrated_partially_refunded_value", "type": "FLOAT", "comment": "Valor parcialmente reembolsado integrado."},
        {"name": "external_order_manual_partially_refunded_value", "type": "FLOAT", "comment": "Valor parcialmente reembolsado manual."},
        {"name": "external_order_integrated_partially_cancelation_value", "type": "FLOAT", "comment": "Valor de cancelación parcial integrada."},
        {"name": "external_order_manual_partially_cancelation_value", "type": "FLOAT", "comment": "Valor de cancelación parcial manual."},
        {"name": "transaction_status", "type": "STRING", "comment": "Estado de la transacción."},
        {"name": "nc_status", "type": "STRING", "comment": "Estado de la nota de crédito."},
        {"name": "external_order_cancellation_date", "type": "STRING", "comment": "Fecha de cancelación del pedido externo."},
        {"name": "external_order_cancellation_liability", "type": "STRING", "comment": "Responsabilidad de la cancelación del pedido externo."},
        {"name": "external_order_cancellation_code_description", "type": "STRING", "comment": "Descripción del código de cancelación."}
    ],
    "primary_key": ["id"]
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

l1_raw_catalog_name_prod = 'l1_raw'

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD (`_TLD_YUNO`)
spark.sql(f"""
CREATE OR REPLACE TEMPORARY VIEW tr_deteccion_fraudes_yuno_TLD_YUNO AS

SELECT
    st.SALES_TRANSACTION_ID,
    st.SPECIALSALEORDERLD AS SPECIAL_SALE_ORDER,
    st.SALEKEY,
    st.INTEGRATED,
    st.SALES_TYPE_ID,
    st.POS_REGISTER_ID,
    st.COUNTRY_ID,
    cou.COUNTRY_NAME_DESC,
    loc.OWNERSHIPS,
    st.LOYALTY_MCID AS MC_ID,
    st.SALES_DATE,
    st.SALES_BUSINESS_DT AS FECHA,
    st.sales_start_dttm,
    st.sales_end_dttm,
    lss.SALE_CHANNEL_ID,
    lss.SALE_SUBCHANNEL_ID,
    cm.SALE_CHANNEL_DESC AS CHANNEL_NAME_DESC,
    lss.SALE_SUBCHANNEL_DESC AS SUBCHANNEL_NAME_DESC,
    ROUND(st.SALES_GROSS_AMT, 5) AS VENTA_BRUT_LC,
    st.MANAGER_ASSOCIATE_ID,
    st.SALES_ASSOCIATE_ID,
    loc.LOCATION_ID,
    loc.LOCATION_BASE_ID,
    loc.LOCATION_NAME,
    loc.LOC_STORE_OAK_ID
FROM
    {l1_raw_catalog_name_prod}.adw.SALES_TRANSACTION_SIN_BRASIL ST -- Table for non-Brazil countries
    INNER JOIN {l2_foundation_catalog_name}.common.lk_sale_subchannel lss ON st.sale_subchannel_id = lss.sale_subchannel_id
    INNER JOIN {l2_foundation_catalog_name}.common.lk_sale_channel cm ON lss.sale_channel_id = cm.sale_channel_id
    INNER JOIN {l3_foundation_catalog_name}.common.dim_location loc ON loc.LOCATION_ID = st.LOCATION_ID AND loc.LOCATION_END_DT = '9999-12-31T00:00:00Z'
    INNER JOIN {l3_foundation_catalog_name}.common.dim_country cou ON cou.country_id = st.COUNTRY_ID AND cou.COUNTRY_END_DT = '9999-12-31T00:00:00Z'
    LEFT JOIN {l1_raw_catalog_name_prod}.adw.payment_line_sin_brasil pl on st.SALES_TRANSACTION_ID = pl.SALES_TRANSACTION_ID and st.country_id = pl.country_id and st.location_id = pl.location_id
WHERE
    st.SALES_BUSINESS_DT BETWEEN date_add('{fecha_ayer}T00:00:00.000', {dias_ventana}) AND '{fecha_ayer}T23:59:59.999'
    AND (
        (lss.SALE_SUBCHANNEL_ID IN (2002) AND upper(st.PARTNER_DESC) = 'MCD APP') OR
        lss.SALE_SUBCHANNEL_ID IN (1001, 1002, 1003) OR
        (lss.SALE_SUBCHANNEL_ID IN (1004) AND st.channel_id <> 99)
    )
    AND st.COUNTRY_ID IN {mercados}
    AND st.SALES_GROSS_AMT NOT BETWEEN 0 AND 0.2
    AND st.SALES_TYPE_ID IN (1, 2)
    
    ------- omitir transacciones en efectivo en colombia en yuno
    and pl.PAYMENT_SUBTYPE_ID not in ('1_102','47_102')
    -----------------------------------------------------------
/*
UNION ALL -- Combine with Brazil data

-- Section for Brazil (not filtered by 'mercados' widget)
SELECT
    st.SALES_TRANSACTION_ID,
    st.SPECIALSALEORDERLD AS SPECIAL_SALE_ORDER,
    st.SALEKEY,
    st.INTEGRATED,
    st.SALES_TYPE_ID,
    st.POS_REGISTER_ID,
    st.COUNTRY_ID,
    cou.COUNTRY_NAME_DESC,
    loc.OWNERSHIPS,
    st.LOYALTY_MCID AS MC_ID,
    st.SALES_DATE,
    st.SALES_BUSINESS_DT AS FECHA,
    st.sales_start_dttm,
    st.sales_end_dttm,
    lss.SALE_CHANNEL_ID,
    lss.SALE_SUBCHANNEL_ID,
    cm.SALE_CHANNEL_DESC AS CHANNEL_NAME_DESC,
    lss.SALE_SUBCHANNEL_DESC AS SUBCHANNEL_NAME_DESC,
    ROUND(st.SALES_GROSS_AMT, 5) AS VENTA_BRUT_LC,
    st.MANAGER_ASSOCIATE_ID,
    st.SALES_ASSOCIATE_ID,
    loc.LOCATION_ID,
    loc.LOCATION_BASE_ID,
    loc.LOCATION_NAME,
    loc.LOC_STORE_OAK_ID
FROM
    {l1_raw_catalog_name_prod}.adw.SALES_TRANSACTION ST -- Table specifically for Brazil
    INNER JOIN {l2_foundation_catalog_name}.common.lk_sale_subchannel lss ON st.sale_subchannel_id = lss.sale_subchannel_id
    INNER JOIN {l2_foundation_catalog_name}.common.lk_sale_channel cm ON lss.sale_channel_id = cm.sale_channel_id
    INNER JOIN {l3_foundation_catalog_name}.common.dim_location loc ON loc.LOCATION_ID = st.LOCATION_ID AND loc.LOCATION_END_DT = '9999-12-31T00:00:00Z'
    INNER JOIN {l3_foundation_catalog_name}.common.dim_country cou ON cou.country_id = st.COUNTRY_ID AND cou.COUNTRY_END_DT = '9999-12-31T00:00:00Z'
WHERE
    st.SALES_BUSINESS_DT BETWEEN date_add('{fecha_ayer}T00:00:00.000', {dias_ventana}) AND '{fecha_ayer}T23:59:59.999'
    AND (
        (lss.SALE_SUBCHANNEL_ID IN (2002) AND upper(st.PARTNER_DESC) = 'MCD APP') OR
        lss.SALE_SUBCHANNEL_ID IN (1001, 1002, 1003) OR
        (lss.SALE_SUBCHANNEL_ID IN (1004) AND st.channel_id <> 99)
    )
    -- No COUNTRY_ID filter here, assuming this table is only Brazil and should always be included
    AND st.SALES_GROSS_AMT NOT BETWEEN 0 AND 0.2
    AND st.SALES_TYPE_ID IN (1, 2) -- Include sales (1) and credit notes (2)
    */
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Notas de Crédito (`cte_nc`)
spark.sql( f"""
CREATE OR REPLACE TEMP VIEW cte_nc AS
SELECT DISTINCT
    sales_start_dttm,
    sales_end_dttm,
    SALES_DATE,
    LOCATION_NAME,
    SALES_TRANSACTION_ID,
    SPECIAL_SALE_ORDER,
    VENTA_BRUT_LC AS VENTA_BRUTA_NC,
    ROW_NUMBER() OVER(PARTITION BY SPECIAL_SALE_ORDER ORDER BY SALES_TRANSACTION_ID DESC) as aux_Orden_NC
FROM
    tr_deteccion_fraudes_yuno_TLD_YUNO
WHERE
    SALES_TYPE_ID = 2
    AND SPECIAL_SALE_ORDER IS NOT NULL
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Notas de Crédito Duplicadas (`cte_nc_duplicadas`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_nc_duplicadas AS
SELECT DISTINCT
    SPECIAL_SALE_ORDER
FROM
    cte_nc
WHERE
    aux_Orden_NC > 1
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Transacciones Yuno (`cte_yuno_transactions`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_transactions AS
SELECT
    y.*,
    ROW_NUMBER() OVER(PARTITION BY merchant_order_id ORDER BY created_at DESC) AS aux_ordenTransaction
FROM
    {l2_foundation_catalog_name}.app_yuno.tr_transactions y 
    INNER JOIN {l3_foundation_catalog_name}.common.dim_country c ON c.COUNTRY_SHORT_ABBREVIATION_CD = y.country AND c.COUNTRY_END_DT = '9999-12-31T00:00:00Z'
WHERE
    y.status = 'SUCCEEDED'
    AND y.created_at BETWEEN
        TIMESTAMPADD(HOUR, -1 * (TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), c.COUNTRY_TIMEZONE))), date_add('{fecha_ayer}T00:00:00.000', {dias_ventana}))
    AND
        TIMESTAMPADD(HOUR, -1 * (TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), c.COUNTRY_TIMEZONE))), '{fecha_ayer}T23:59:59.999')
"""
)
print("Created temporary view cte_yuno_transactions.")

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Última Transacción Yuno (`cte_yuno_ultima_transaction`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_ultima_transaction AS
SELECT
    payment_id,
    provider_id
FROM
    cte_yuno_transactions
WHERE
    aux_ordenTransaction = 1
"""
)
print("Created temporary view cte_yuno_ultima_transaction.")

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Pagos Yuno (`cte_yuno`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno AS
SELECT
    c.COUNTRY_NAME_DESC,
    l.LOCATION_ACRONYM_CD,
    l.OWNERSHIPS,
    SUBSTRING(p.merchant_order_id, CHARINDEX('-', p.merchant_order_id) + 1, LENGTH(p.merchant_order_id) - CHARINDEX('-', p.merchant_order_id)) AS SPECIAL_SALES_ORDER,
    FROM_UTC_TIMESTAMP(p.created_at, c.COUNTRY_TIMEZONE) AS created_at_local,
    FROM_UTC_TIMESTAMP(p.updated_at, c.COUNTRY_TIMEZONE) AS updated_at_local,
    ROW_NUMBER() OVER(PARTITION BY p.merchant_order_id ORDER BY p.updated_at DESC) as aux_OrdenTransaccion,
    p.* 
FROM
    {l2_foundation_catalog_name}.app_yuno.tr_payments p
    LEFT JOIN {l3_foundation_catalog_name}.common.dim_country c ON c.COUNTRY_SHORT_ABBREVIATION_CD = p.country AND c.COUNTRY_END_DT = '9999-12-31T00:00:00Z'
    LEFT JOIN {l3_foundation_catalog_name}.common.dim_location l ON l.COUNTRY_ID = c.COUNTRY_ID AND l.LOCATION_ACRONYM_CD = LEFT(p.merchant_order_id, 3) AND l.location_end_dt = '9999-12-31T00:00:00Z'
WHERE
    p.status IN ('SUCCEEDED', 'REFUNDED')
    AND p.created_at BETWEEN
        TIMESTAMPADD(HOUR, -1 * (TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), c.COUNTRY_TIMEZONE))), date_add('{fecha_ayer}T00:00:00.000', {dias_ventana}))
    AND
        TIMESTAMPADD(HOUR, -1 * (TIMESTAMPDIFF(HOUR, current_timestamp(), FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), c.COUNTRY_TIMEZONE))), '{fecha_ayer}T23:59:59.999')
"""
)
print("Created temporary view cte_yuno.")

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Último Pago Yuno (`cte_yuno_ultimo_pago`)
spark.sql( f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_ultimo_pago AS
SELECT
    *
FROM
    cte_yuno
WHERE
    aux_OrdenTransaccion = 1
"""
)
print("Created temporary view cte_yuno_ultimo_pago.")

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Pagos Yuno Integrados (`cte_yuno_integradas`)
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW cte_yuno_integradas AS 

SELECT DISTINCT
  y.*
FROM
  cte_yuno_ultimo_pago y
INNER JOIN
  tr_deteccion_fraudes_yuno_TLD_YUNO t ON t.SPECIAL_SALE_ORDER = y.SPECIAL_SALES_ORDER;
"""
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Pagos Yuno Manuales (`cte_yuno_manuales`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_manuales AS
SELECT DISTINCT
    y.*,
    CONCAT(CAST(y.updated_at_local AS DATE), y.LOCATION_ACRONYM_CD, CAST(y.amount_value AS INT)) AS yuno_CONCAT,
    ROW_NUMBER() OVER(PARTITION BY CONCAT(CAST(y.updated_at_local AS DATE), y.LOCATION_ACRONYM_CD, CAST(y.amount_value AS INT)) ORDER BY y.updated_at DESC) AS aux_yuno_CONCAT_orden
FROM
    cte_yuno_ultimo_pago y
WHERE
    NOT EXISTS (SELECT 1 FROM cte_yuno_integradas i WHERE i.merchant_order_id = y.merchant_order_id)
"""
)
print("Created temporary view cte_yuno_manuales.")

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Concatenados Duplicados Yuno (`cte_concatenados_duplicados`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_concatenados_duplicados AS
SELECT DISTINCT
    m.yuno_CONCAT
FROM
    cte_yuno_manuales m
WHERE
    aux_yuno_CONCAT_orden > 1 
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Pagos Yuno Manuales Asociables (`cte_yuno_manuales_asociables`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_manuales_asociables AS
SELECT
    m.*
FROM
    cte_yuno_manuales m
WHERE
    NOT EXISTS (SELECT 1 FROM cte_concatenados_duplicados md WHERE m.yuno_CONCAT = md.yuno_CONCAT)
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Pagos Yuno Manuales No Asociadas (`cte_yuno_manuales_no_asociadas`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_yuno_manuales_no_asociadas AS
SELECT
    m.*
FROM
    cte_yuno_manuales m
WHERE
    EXISTS (SELECT 1 FROM cte_concatenados_duplicados md WHERE m.yuno_CONCAT = md.yuno_CONCAT)
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Transacciones TLD Manuales (`cte_tld_manuales`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_tld_manuales AS
SELECT
    a.*,
    CONCAT(CAST(a.sales_end_dttm AS DATE), LEFT(a.LOCATION_NAME, 3), CAST(a.VENTA_BRUT_LC AS INT)) AS CONCAT,
    
    ROW_NUMBER() OVER(PARTITION BY CONCAT(LEFT(a.LOCATION_NAME, 3), CAST(a.sales_end_dttm AS DATE), CAST(a.VENTA_BRUT_LC AS INT)) ORDER BY 1 DESC) AS aux_CONCAT_orden
FROM
    tr_deteccion_fraudes_yuno_TLD_YUNO a
WHERE
    a.SALES_TYPE_ID = 1 
    AND a.SPECIAL_SALE_ORDER IS NULL
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Concatenados Duplicados TLD (`cte_tld_manuales_mismo_concat`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_tld_manuales_mismo_concat AS
SELECT DISTINCT
    m.concat
FROM
    cte_tld_manuales m
WHERE
    aux_CONCAT_orden > 1
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Manuales Asociables (`cte_tld_manuales_asociables`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_tld_manuales_asociables AS
SELECT DISTINCT
    t.*
FROM
    cte_tld_manuales t
WHERE
    
    NOT EXISTS (SELECT 1 FROM cte_tld_manuales_mismo_concat m WHERE m.concat = t.concat)
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Manuales No Asociables (`cte_tld_manuales_no_asociables`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_tld_manuales_no_asociables AS
SELECT
    t.*
FROM
    cte_tld_manuales t
WHERE
   
    EXISTS (SELECT 1 FROM cte_tld_manuales_mismo_concat m WHERE m.concat = t.concat)
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Transacciones Integradas (`transacciones_integradas_temp`)
spark.sql(f"""

CREATE OR REPLACE TEMP VIEW transacciones_integradas_temp AS 

  SELECT DISTINCT
  'App' as Selector,
  'Integradas' as tipo_integracion,
  NULL as clave_concatenada,
  a.OWNERSHIPS,
  a.COUNTRY_NAME_DESC,
  LEFT(a.LOCATION_NAME,3) as LOCATION_ACRONYM_CD,
  concat(a.COUNTRY_NAME_DESC,'-',LEFT(a.LOCATION_NAME,3)) as Key,
  a.SALES_DATE,
  FECHA,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.SALES_TRANSACTION_ID,
  a.SPECIAL_SALE_ORDER,
  SALEKEY,
  POS_REGISTER_ID,
  SUBSTRING(POS_REGISTER_ID, 0, CHARINDEX('_', POS_REGISTER_ID) - 1) AS POS_REGISTER_NUMBER,
  CHANNEL_NAME_DESC,
  SUBCHANNEL_NAME_DESC,
  INTEGRATED,
  SALES_TYPE_ID,
  a.VENTA_BRUT_LC,
  nc.SALES_TRANSACTION_ID as SALES_TRANSACTION_ID_NC,
  case when nc_d.special_sale_order is null then 0 else 1 end as NC_duplicada,
  nc.SALES_DATE as SALES_DATE_NC,
  nc.sales_start_dttm as sales_start_dttm_nc,
  nc.sales_end_dttm as sales_end_dttm_nc,
  nc.VENTA_BRUTA_NC,
  datediff(second, a.sales_end_dttm, nc.sales_end_dttm) as Tiempo_Reintegro_NC_seg,
  datediff(minute, a.sales_end_dttm, nc.sales_end_dttm) as Tiempo_Reintegro_NC_min,
  y.created_at as yuno_created_at,
  y.updated_at as yuno_updated_at,
  y.created_at_local as yuno_created_at_gmt,
  y.updated_at_local as yuno_updated_at_gmt,
  datediff(second, yuno_created_at, yuno_updated_at) as Tiempo_proceso_yuno_seg,
  datediff(second, yuno_updated_at_gmt, a.sales_start_dttm) as Tiempo_recepcion_TLD_seg,
  datediff(second,  a.sales_start_dttm,  a.sales_end_dttm) as Tiempo_transaccion_TLD_seg,
  y.description,
  y.payment_id,
  y.status as yuno_status,
  y.sub_status as yuno_sub_status,
  y.amount_value as yuno_amount_value,
  y.captured as yuno_captured,
  y.refunded as yuno_refunded,
  case when sub_status = 'PARTIALLY_REFUNDED' then amount_value - refunded
        when status = 'SUCCEEDED' then amount_value 
        else 0 end as yuno_cobro_integrado,

  case when sub_status = 'PARTIALLY_REFUNDED' then VENTA_BRUT_LC - amount_value - refunded 
    when status = 'SUCCEEDED' then VENTA_BRUT_LC - amount_value 
    else 0 end as yuno_diferencia_cobro_integrado,

  0 as yuno_cancelacion_integrada,
  0 as yuno_diferencia_cancelacion_integrada,
  case when status = 'REFUNDED' then refunded else 0 end as yuno_compensacion_integrada,
  case when status = 'REFUNDED' then VENTA_BRUT_LC - refunded else 0 end as yuno_diferencia_compensacion_integrada,
  case when sub_status = 'PARTIALLY_REFUNDED' then refunded else 0 end as yuno_compensacion_parcial_integrada,
  0 as yuno_cobro_manual,
  0 as yuno_diferencia_cobro_manual,
  0 as yuno_cancelacion_manual,
  0 as yuno_diferencia_cancelacion_manual,
  0 as yuno_compensacion_manual,
  0 as yuno_diferencia_compensacion_manual,
  0 as yuno_compensacion_parcial_manual
 
FROM
  tr_deteccion_fraudes_yuno_TLD_YUNO a 
INNER JOIN
  cte_yuno_integradas y on y.SPECIAL_SALES_ORDER = a.SPECIAL_SALE_ORDER
LEFT JOIN
  cte_nc nc on nc.SPECIAL_SALE_ORDER = a.SPECIAL_SALE_ORDER
LEFT JOIN
  cte_nc_duplicadas nc_d on nc_d.SPECIAL_SALE_ORDER = nc.SPECIAL_SALE_ORDER
 
WHERE
  a.SALES_TYPE_ID = 1
  and a.SPECIAL_SALE_ORDER IS NOT NULL

  """
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Manuales Asociables (`tld_manuales_asociables`)
spark.sql(f"""
create or replace temp view tld_manuales_asociables as

SELECT DISTINCT 
  'App' as Selector,
  'Manuales asociadas' as tipo_integracion,
  a.CONCAT as clave_concatenada,
  a.OWNERSHIPS,
  a.COUNTRY_NAME_DESC,
  LEFT(a.LOCATION_NAME,3) as LOCATION_ACRONYM_CD,
  concat(a.COUNTRY_NAME_DESC,'-',LEFT(a.LOCATION_NAME,3)) as Key,
  a.SALES_DATE,
  FECHA,
  a.sales_start_dttm,
  a.sales_end_dttm,
  a.SALES_TRANSACTION_ID,
  a.SPECIAL_SALE_ORDER,
  SALEKEY,
  POS_REGISTER_ID,
  SUBSTRING(POS_REGISTER_ID, 0, CHARINDEX('_', POS_REGISTER_ID) - 1) AS POS_REGISTER_NUMBER,
  CHANNEL_NAME_DESC,
  SUBCHANNEL_NAME_DESC,
  INTEGRATED,
  SALES_TYPE_ID,
  a.VENTA_BRUT_LC,
  null as SALES_TRANSACTION_ID_NC,
  null as NC_duplicada,
  null as SALES_DATE_NC,
  null as sales_start_dttm_nc,
  null as sales_end_dttm_nc,
  null as VENTA_BRUTA_NC,
  null as Tiempo_Reintegro_NC_seg,
  null as Tiempo_Reintegro_NC_min,
  y.created_at as yuno_created_at,
  y.updated_at as yuno_updated_at,
  y.created_at_local as yuno_created_at_gmt,
  y.updated_at_local as yuno_updated_at_gmt,
  datediff(second, yuno_created_at, yuno_updated_at) as Tiempo_proceso_yuno_seg,
  datediff(second, yuno_updated_at_gmt, a.sales_start_dttm) as Tiempo_recepcion_TLD_seg,
  datediff(second,  a.sales_start_dttm,  a.sales_end_dttm) as Tiempo_transaccion_TLD_seg,
  y.description,
  y.payment_id,
  y.status as yuno_status,
  y.sub_status as yuno_sub_status,
  y.amount_value as yuno_amount_value,
  y.captured as yuno_captured,
  y.refunded as yuno_refunded,
  0 as yuno_cobro_integrado,
  0 as yuno_diferencia_cobro_integrado,
  0 as yuno_cancelacion_integrada,
  0 as yuno_diferencia_cancelacion_integrada,
  0 as yuno_compensacion_integrada,
  0 as yuno_diferencia_compensacion_integrada,
  0 as yuno_compensacion_parcial_integrada,
  case when sub_status = 'PARTIALLY_REFUNDED' then amount_value - refunded
        when status = 'SUCCEEDED' then amount_value 
        else 0 end as yuno_cobro_manual,

  case when sub_status = 'PARTIALLY_REFUNDED' then VENTA_BRUT_LC - amount_value - refunded 
    when status = 'SUCCEEDED' then VENTA_BRUT_LC - amount_value 
    else 0 end as yuno_diferencia_cobro_manual,

  0 as yuno_cancelacion_manual,
  0 as yuno_diferencia_cancelacion_manual,
  case when status = 'REFUNDED' then refunded else 0 end as yuno_compensacion_manual,
  case when status = 'REFUNDED' then VENTA_BRUT_LC - refunded else 0 end as yuno_diferencia_compensacion_manual,
  case when sub_status = 'PARTIALLY_REFUNDED' then refunded else 0 end as yuno_compensacion_parcial_manual
 
FROM
  cte_tld_manuales_asociables a 
INNER JOIN
  cte_yuno_manuales_asociables y on y.yuno_CONCAT = a.CONCAT

  """
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Manuales No Asociadas (vía Concat Key) (`tld_manuales_no_concatenado`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW tld_manuales_no_concatenado AS
SELECT DISTINCT 
    'App' AS Selector,
    'Manuales no asociadas' AS tipo_integracion,
    a.CONCAT AS clave_concatenada,
    a.OWNERSHIPS,
    a.COUNTRY_NAME_DESC,
    LEFT(a.LOCATION_NAME, 3) AS LOCATION_ACRONYM_CD,
    CONCAT(a.COUNTRY_NAME_DESC, '-', LEFT(a.LOCATION_NAME, 3)) AS Key,
    a.SALES_DATE,
    a.FECHA,
    a.sales_start_dttm,
    a.sales_end_dttm,
    a.SALES_TRANSACTION_ID,
    a.SPECIAL_SALE_ORDER,
    a.SALEKEY,
    a.POS_REGISTER_ID,
    SUBSTRING(a.POS_REGISTER_ID, 1, COALESCE(NULLIF(CHARINDEX('_', a.POS_REGISTER_ID), 0) - 1, LENGTH(a.POS_REGISTER_ID))) AS POS_REGISTER_NUMBER,
    a.CHANNEL_NAME_DESC,
    a.SUBCHANNEL_NAME_DESC,
    a.INTEGRATED,
    a.SALES_TYPE_ID,
    a.VENTA_BRUT_LC,
    NULL AS SALES_TRANSACTION_ID_NC,
    NULL AS NC_duplicada,
    NULL AS SALES_DATE_NC,
    NULL AS sales_start_dttm_nc,
    NULL AS sales_end_dttm_nc,
    NULL AS VENTA_BRUTA_NC,
    NULL AS Tiempo_Reintegro_NC_seg,
    NULL AS Tiempo_Reintegro_NC_min,
    NULL AS yuno_created_at,
    NULL AS yuno_updated_at,
    NULL AS yuno_created_at_local,
    NULL AS yuno_updated_at_local,
    NULL AS Tiempo_proceso_yuno_seg,
    NULL AS Tiempo_recepcion_TLD_seg,
    DATEDIFF(second, a.sales_start_dttm, a.sales_end_dttm) AS Tiempo_transaccion_TLD_seg,
    NULL AS description,
    NULL AS payment_id,
    NULL AS yuno_status,
    NULL AS yuno_sub_status,
    NULL AS yuno_amount_value,
    NULL AS yuno_captured,
    NULL AS yuno_refunded,
    0 AS yuno_cobro_integrado,
    0 AS yuno_diferencia_cobro_integrado,
    0 AS yuno_cancelacion_integrada,
    0 AS yuno_diferencia_cancelacion_integrada,
    0 AS yuno_compensacion_integrada,
    0 AS yuno_diferencia_compensacion_integrada,
    0 AS yuno_compensacion_parcial_integrada,
    0 AS yuno_cobro_manual,
    0 AS yuno_diferencia_cobro_manual,
    0 AS yuno_cancelacion_manual,
    0 AS yuno_diferencia_cancelacion_manual,
    0 AS yuno_compensacion_manual,
    0 AS yuno_diferencia_compensacion_manual,
    0 AS yuno_compensacion_parcial_manual
FROM
    cte_tld_manuales_asociables a
WHERE
    
    NOT EXISTS (SELECT 1 FROM cte_yuno_manuales_asociables y WHERE y.yuno_CONCAT = a.CONCAT)
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Manuales No Asociadas (Concat Duplicado) (`transacciones_manuales_no_asociadas`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW transacciones_manuales_no_asociadas AS
SELECT DISTINCT
    'App' AS Selector,
    'Manuales no asociadas' AS tipo_integracion,
    a.CONCAT AS clave_concatenada,
    a.OWNERSHIPS,
    a.COUNTRY_NAME_DESC,
    LEFT(a.LOCATION_NAME, 3) AS LOCATION_ACRONYM_CD,
    CONCAT(a.COUNTRY_NAME_DESC, '-', LEFT(a.LOCATION_NAME, 3)) AS Key,
    a.SALES_DATE,
    a.FECHA,
    a.sales_start_dttm,
    a.sales_end_dttm,
    a.SALES_TRANSACTION_ID,
    a.SPECIAL_SALE_ORDER,
    a.SALEKEY,
    a.POS_REGISTER_ID,
    SUBSTRING(a.POS_REGISTER_ID, 1, COALESCE(NULLIF(CHARINDEX('_', a.POS_REGISTER_ID), 0) - 1, LENGTH(a.POS_REGISTER_ID))) AS POS_REGISTER_NUMBER,
    a.CHANNEL_NAME_DESC,
    a.SUBCHANNEL_NAME_DESC,
    a.INTEGRATED,
    a.SALES_TYPE_ID,
    a.VENTA_BRUT_LC,
    NULL AS SALES_TRANSACTION_ID_NC,
    NULL AS NC_duplicada,
    NULL AS SALES_DATE_NC,
    NULL AS sales_start_dttm_nc,
    NULL AS sales_end_dttm_nc,
    NULL AS VENTA_BRUTA_NC,
    NULL AS Tiempo_Reintegro_NC_seg,
    NULL AS Tiempo_Reintegro_NC_min,
    NULL AS yuno_created_at,
    NULL AS yuno_updated_at,
    NULL AS yuno_created_at_local,
    NULL AS yuno_updated_at_local,
    NULL AS Tiempo_proceso_yuno_seg,
    NULL AS Tiempo_recepcion_TLD_seg,
    DATEDIFF(second, a.sales_start_dttm, a.sales_end_dttm) AS Tiempo_transaccion_TLD_seg,
    NULL AS description,
    NULL AS payment_id,
    NULL AS yuno_status,
    NULL AS yuno_sub_status,
    NULL AS yuno_amount_value,
    NULL AS yuno_captured,
    NULL AS yuno_refunded,
    0 AS yuno_cobro_integrado,
    0 AS yuno_diferencia_cobro_integrado,
    0 AS yuno_cancelacion_integrada,
    0 AS yuno_diferencia_cancelacion_integrada,
    0 AS yuno_compensacion_integrada,
    0 AS yuno_diferencia_compensacion_integrada,
    0 AS yuno_compensacion_parcial_integrada,
    0 AS yuno_cobro_manual,
    0 AS yuno_diferencia_cobro_manual,
    0 AS yuno_cancelacion_manual,
    0 AS yuno_diferencia_cancelacion_manual,
    0 AS yuno_compensacion_manual,
    0 AS yuno_diferencia_compensacion_manual,
    0 AS yuno_compensacion_parcial_manual
FROM
    cte_tld_manuales_no_asociables AS a
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de TLD Integradas Sin Yuno (`transacciones_integradas_no_yuno`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW transacciones_integradas_no_yuno AS
SELECT DISTINCT
    'App' AS Selector,
    'Manuales no asociadas' AS tipo_integracion,
    NULL AS clave_concatenada,
    a.OWNERSHIPS,
    a.COUNTRY_NAME_DESC,
    LEFT(a.LOCATION_NAME, 3) AS LOCATION_ACRONYM_CD,
    CONCAT(a.COUNTRY_NAME_DESC, '-', LEFT(a.LOCATION_NAME, 3)) AS Key,
    a.SALES_DATE,
    a.FECHA,
    a.sales_start_dttm,
    a.sales_end_dttm,
    a.SALES_TRANSACTION_ID,
    a.SPECIAL_SALE_ORDER,
    a.SALEKEY,
    a.POS_REGISTER_ID,
    SUBSTRING(a.POS_REGISTER_ID, 1, COALESCE(NULLIF(CHARINDEX('_', a.POS_REGISTER_ID), 0) - 1, LENGTH(a.POS_REGISTER_ID))) AS POS_REGISTER_NUMBER,
    a.CHANNEL_NAME_DESC,
    a.SUBCHANNEL_NAME_DESC,
    a.INTEGRATED,
    a.SALES_TYPE_ID,
    a.VENTA_BRUT_LC,
    NULL AS SALES_TRANSACTION_ID_NC,
    NULL AS NC_duplicada,
    NULL AS SALES_DATE_NC,
    NULL AS sales_start_dttm_nc,
    NULL AS sales_end_dttm_nc,
    NULL AS VENTA_BRUTA_NC,
    NULL AS Tiempo_Reintegro_NC_seg,
    NULL AS Tiempo_Reintegro_NC_min,
    NULL AS yuno_created_at,
    NULL AS yuno_updated_at,
    NULL AS yuno_created_at_local,
    NULL AS yuno_updated_at_local,
    NULL AS Tiempo_proceso_yuno_seg,
    NULL AS Tiempo_recepcion_TLD_seg,
    DATEDIFF(second, a.sales_start_dttm, a.sales_end_dttm) AS Tiempo_transaccion_TLD_seg,
    NULL AS description,
    NULL AS payment_id,
    NULL AS yuno_status,
    NULL AS yuno_sub_status,
    NULL AS yuno_amount_value,
    NULL AS yuno_captured,
    NULL AS yuno_refunded,
    0 AS yuno_cobro_integrado,
    0 AS yuno_diferencia_cobro_integrado,
    0 AS yuno_cancelacion_integrada,
    0 AS yuno_diferencia_cancelacion_integrada,
    0 AS yuno_compensacion_integrada,
    0 AS yuno_diferencia_compensacion_integrada,
    0 AS yuno_compensacion_parcial_integrada,
    0 AS yuno_cobro_manual,
    0 AS yuno_diferencia_cobro_manual,
    0 AS yuno_cancelacion_manual,
    0 AS yuno_diferencia_cancelacion_manual,
    0 AS yuno_compensacion_manual,
    0 AS yuno_diferencia_compensacion_manual,
    0 AS yuno_compensacion_parcial_manual
FROM
    tr_deteccion_fraudes_yuno_TLD_YUNO a
WHERE
    NOT EXISTS (SELECT 1 FROM cte_yuno_integradas y WHERE y.SPECIAL_SALES_ORDER = a.SPECIAL_SALE_ORDER)
    AND a.SALES_TYPE_ID = 1
    AND a.SPECIAL_SALE_ORDER IS NOT NULL
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Yuno Manuales No Asociadas (Concat Duplicado) (`transacciones_yuno_manuales`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW transacciones_yuno_manuales AS
SELECT DISTINCT
    'App' AS Selector,
    'Yuno sin integración' AS tipo_integracion,
    y.yuno_CONCAT AS clave_concatenada,
    y.OWNERSHIPS,
    y.COUNTRY_NAME_DESC,
    y.LOCATION_ACRONYM_CD,
    CONCAT(y.COUNTRY_NAME_DESC, '-', y.LOCATION_ACRONYM_CD) AS Key,
    NULL AS SALES_DATE,
    NULL AS FECHA,
    NULL AS sales_start_dttm,
    NULL AS sales_end_dttm,
    NULL AS SALES_TRANSACTION_ID,
    y.SPECIAL_SALES_ORDER,
    NULL AS SALEKEY,
    NULL AS POS_REGISTER_ID,
    NULL AS POS_REGISTER_NUMBER,
    NULL AS CHANNEL_NAME_DESC,
    NULL AS SUBCHANNEL_NAME_DESC,
    NULL AS INTEGRATED,
    NULL AS SALES_TYPE_ID,
    NULL AS VENTA_BRUT_LC,
    NULL AS SALES_TRANSACTION_ID_NC,
    NULL AS NC_duplicada,
    NULL AS SALES_DATE_NC,
    NULL AS sales_start_dttm_nc,
    NULL AS sales_end_dttm_nc,
    NULL AS VENTA_BRUTA_NC,
    NULL AS Tiempo_Reintegro_NC_seg,
    NULL AS Tiempo_Reintegro_NC_min,
    y.created_at AS yuno_created_at,
    y.updated_at AS yuno_updated_at,
    y.created_at_local AS yuno_created_at_local,
    y.updated_at_local AS yuno_updated_at_local,
    DATEDIFF(second, y.created_at, y.updated_at) AS Tiempo_proceso_yuno_seg,
    NULL AS Tiempo_recepcion_TLD_seg,
    NULL AS Tiempo_transaccion_TLD_seg,
    y.description,
    y.payment_id,
    y.status AS yuno_status,
    y.sub_status AS yuno_sub_status,
    y.amount_value AS yuno_amount_value,
    y.captured AS yuno_captured,
    y.refunded AS yuno_refunded,
    0 as yuno_cobro_integrado,
    0 as yuno_diferencia_cobro_integrado,
    0 as yuno_cancelacion_integrada,
    0 as yuno_diferencia_cancelacion_integrada,
    0 as yuno_compensacion_integrada,
    0 as yuno_diferencia_compensacion_integrada,
    0 as yuno_compensacion_parcial_integrada,
    case when sub_status = 'PARTIALLY_REFUNDED' then amount_value - refunded
            when status = 'SUCCEEDED' then amount_value 
            else 0 end as yuno_cobro_manual,
    0 as yuno_diferencia_cobro_manual,
    case when status = 'REFUNDED' or sub_status = 'PARTIALLY_REFUNDED' then refunded else 0 end as yuno_cancelacion_manual,
    0 as yuno_diferencia_cancelacion_manual,
    0 as yuno_compensacion_manual,
    0 as yuno_diferencia_compensacion_manual,
    0 as yuno_compensacion_parcial_manual
FROM
    cte_yuno_manuales_no_asociadas y
"""
)


# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Yuno Manuales No Asociadas (Sin Match TLD) (`transacciones_no_integradas_en_tld`)
spark.sql(f"""

create or replace temp view transacciones_no_integradas_en_tld as

SELECT DISTINCT
  'App' as Selector,
  'Yuno sin integración' as tipo_integracion,
  null as clave_concatenada,
  OWNERSHIPS,
  COUNTRY_NAME_DESC,
  null as LOCATION_ACRONYM_CD,
  concat(COUNTRY_NAME_DESC,'-',LEFT(y.merchant_order_id,3)) as Key,
  null as SALES_DATE,
  null as FECHA,
  null as sales_start_dttm,
  null as sales_end_dttm,
  null as SALES_TRANSACTION_ID,
  y.SPECIAL_SALES_ORDER,
  null as SALEKEY,
  null as POS_REGISTER_ID,
  null AS POS_REGISTER_NUMBER,
  null as CHANNEL_NAME_DESC,
  null as SUBCHANNEL_NAME_DESC,
  null as INTEGRATED,
  null as SALES_TYPE_ID,
  null as VENTA_BRUTA,
  null as SALES_TRANSACTION_ID_NC,
  null,
  null as SALES_DATE_NC,
  null as sales_start_dttm_nc,
  null as sales_end_dttm_nc,
  null as VENTA_BRUTA_NC,
  null as Tiempo_Reintegro_NC_seg,
  null as Tiempo_Reintegro_NC_min,
  y.created_at as yuno_created_at,
  y.updated_at as yuno_updated_at,
  y.created_at_local as yuno_created_at_gmt,
  y.updated_at_local as yuno_updated_at_gmt,
  datediff(second, yuno_created_at, yuno_updated_at) as Tiempo_proceso_yuno_seg,
  null as Tiempo_recepcion_TLD_seg,
  null as Tiempo_transaccion_TLD_seg,
  y.description,
  y.payment_id,
  y.status as yuno_status,
  y.sub_status as yuno_sub_status,
  y.amount_value as yuno_amount_value,
  y.captured as yuno_captured,
  y.refunded as yuno_refunded,
  0 as yuno_cobro_integrado,
  0 as yuno_diferencia_cobro_integrado,
  0 as yuno_cancelacion_integrada,
  0 as yuno_diferencia_cancelacion_integrada,
  0 as yuno_compensacion_integrada,
  0 as yuno_diferencia_compensacion_integrada,
  0 as yuno_compensacion_parcial_integrada,
  case when sub_status = 'PARTIALLY_REFUNDED' then amount_value - refunded
        when status = 'SUCCEEDED' then amount_value 
        else 0 end as yuno_cobro_manual,
  0 as yuno_diferencia_cobro_manual,
  case when status = 'REFUNDED' or sub_status = 'PARTIALLY_REFUNDED' then refunded else 0 end as yuno_cancelacion_manual,
  0 as yuno_diferencia_cancelacion_manual,
  0 as yuno_compensacion_manual,
  0 as yuno_diferencia_compensacion_manual,
  0 as yuno_compensacion_parcial_manual
 
FROM
  cte_yuno_manuales_asociables y
WHERE
  not exists (select 1 from cte_tld_manuales_asociables a where y.yuno_CONCAT = a.CONCAT)
  

 
 """
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal Unificada (`cte_temp`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_temp AS

SELECT * FROM transacciones_integradas_temp
UNION ALL
SELECT * FROM tld_manuales_asociables
UNION ALL
SELECT * FROM tld_manuales_no_concatenado
UNION ALL
SELECT * FROM transacciones_manuales_no_asociadas
UNION ALL
SELECT * FROM transacciones_integradas_no_yuno
UNION ALL
SELECT * FROM transacciones_yuno_manuales
UNION ALL
SELECT * FROM transacciones_no_integradas_en_tld
"""
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal con Estados Calculados (`cte_temp2`)
spark.sql(f"""
create or replace temp view cte_temp2 as

SELECT 
    CAST(COALESCE(fecha, yuno_updated_at_gmt) AS DATE) as sales_business_dt,
    *,
case when
    tipo_integracion = 'Manual'
    and
    (yuno_cancelacion_integrada + yuno_diferencia_cancelacion_integrada
    + yuno_cancelacion_manual
    + yuno_cobro_integrado + yuno_diferencia_cobro_integrado
    + yuno_cobro_manual) = 0 then 1 else 0 end as Error_o_Fraude,
 
case 
    when yuno_status = 'REFUNDED' and SALES_TRANSACTION_ID is not null then 'Compensada'
    when yuno_status = 'REFUNDED' and SALES_TRANSACTION_ID is null then 'Cancelada'
    when yuno_sub_status = 'PARTIALLY_REFUNDED' and SALES_TRANSACTION_ID is not null then 'Compensada parcialmente'
    when yuno_sub_status = 'PARTIALLY_REFUNDED' and SALES_TRANSACTION_ID is null then 'Cancelada parcialmente'
    when yuno_status = 'SUCCEEDED' and SALES_TRANSACTION_ID is not null then 'Cobrada'
    when yuno_status = 'SUCCEEDED' and SALES_TRANSACTION_ID is null then 'No encontrada'
    when yuno_status is null then 'No encontrada' end as Estado_Transacccion,

case when NC_Duplicada = 1 then 'Nota de crédito duplicada'
when SALES_TRANSACTION_ID_NC is null then 'No aplica NC'
when SALES_TRANSACTION_ID_NC is not null then 'Nota de crédito aplicada' end as Estado_NC


FROM
  cte_temp
 """
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal de Tipos de Cambio (`cte_currency_rate`)
spark.sql(f"""
CREATE OR REPLACE TEMP VIEW cte_currency_rate AS
SELECT DISTINCT
    cr.SOURCE_CURRENCY_CD,
    DATE_FORMAT(cr.CURR_TRANS_CALENDAR_DT, 'yMM') AS calendar_month_id,
    cr.SOURCE_TO_TARGET_CURRENCY_RATE
FROM
    {l2_foundation_catalog_name}.common.hist_currency_translation_rate cr
WHERE
    cr.CURRENCY_TYPE_ID = 4
    AND current_date BETWEEN cr.CURR_TRANSLATION_RATE_START_DT AND cr.CURR_TRANSLATION_RATE_END_DT
"""
)

# COMMAND ----------

# DBTITLE 1,Creación de Vista Temporal Final (`_TEMP`)

spark.sql(f"""

create or replace temp view tr_deteccion_fraudes_yuno_TEMP as 

SELECT
  date_format(sales_business_dt, 'yMM') as calendar_month_id,
  cr.source_to_target_currency_rate,
sales_business_dt,
Selector,
tipo_integracion as Integration_type,
case when tipo_integracion in ('Integradas', 'Manuales asociadas') then 'Integrado' else 'Manual' end as Integration_group,
clave_concatenada as concat_key,
OWNERSHIPS,
null as SPECIAL_SALE_STOREAREA,
t.COUNTRY_NAME_DESC,
LOCATION_ACRONYM_CD,
Key,
null as external_order_merchant_id,
t.SALES_DATE,
FECHA as sales_business_date,
sales_start_dttm,
sales_end_dttm,
SALES_TRANSACTION_ID,
SPECIAL_SALE_ORDER,
SALEKEY,
POS_REGISTER_ID,
POS_REGISTER_NUMBER,
CHANNEL_NAME_DESC,
SUBCHANNEL_NAME_DESC,
INTEGRATED,
SALES_TYPE_ID,
VENTA_BRUT_LC as TLD_gross_sale,
SALES_TRANSACTION_ID_NC,
NC_duplicada as NC_duplicated,
t.SALES_DATE_NC,
sales_start_dttm_nc,
sales_end_dttm_nc,
VENTA_BRUTA_NC as NC_gross_sale,
Tiempo_Reintegro_NC_seg as NC_time_seg,
Tiempo_Reintegro_NC_min as NC_time_min,
yuno_created_at as external_order_created_at_gmt,
yuno_updated_at as external_order_updated_at_gmt,
yuno_created_at_gmt as external_order_created_at,
yuno_updated_at_gmt as external_order_updated_at,
Tiempo_proceso_yuno_seg as Process_time_external_order_seg,
Tiempo_recepcion_TLD_seg as Reception_time_TLD_seg,
Tiempo_transaccion_TLD_seg as Tx_time_TLD_seg,
description as external_order_description,
t.payment_id as external_order_payment_id,
provider_id as external_order_provider_id,
yuno_status as external_order_status,
yuno_sub_status as external_order_sub_status,
yuno_amount_value as external_order_amount_value,
yuno_captured as external_order_captured_value,
yuno_refunded as external_order_refunded_value,
null as external_order_payment_type,
null as external_order_payment_method,
null as external_order_payment_brand,
null as external_order_liability,
yuno_cobro_integrado as external_order_integrated_payment_value,
yuno_diferencia_cobro_integrado as external_order_integrated_payment_diff,
yuno_cancelacion_integrada as external_order_integrated_cancelation_value,
yuno_diferencia_cancelacion_integrada as external_order_integrated_cancelation_diff,
yuno_cobro_manual as external_order_manual_payment_value,
yuno_diferencia_cobro_manual as external_order_manual_payment_diff,
yuno_cancelacion_manual as external_order_manual_cancelation_value,
yuno_diferencia_cancelacion_manual as external_order_manual_cancelation_diff,
yuno_compensacion_integrada as external_order_integrated_refunded_value,
yuno_diferencia_compensacion_integrada as external_order_integrated_refunded_diff,
yuno_compensacion_manual as external_order_manual_refunded_value,
yuno_diferencia_compensacion_manual as external_order_manual_refunded_diff,
yuno_compensacion_parcial_integrada as external_order_integrated_partially_refunded_value,
yuno_compensacion_parcial_manual as external_order_manual_partially_refunded_value,
null as external_order_integrated_partially_cancelation_value,
null as external_order_manual_partially_cancelation_value,
Estado_Transacccion as transaction_status,
Estado_NC as NC_status,
null as external_order_cancellation_date,
null as external_order_cancellation_liability,
null as external_order_cancellation_code_description,
concat(nvl(cast(sales_business_dt as string), '0'),'-',nvl(SPECIAL_SALE_ORDER,0),'-',nvl(SALEKEY,0)) as ID,
'{pipeline_run_id}' AS ADLS_AUDIT_RUN_ID,
 FROM_UTC_TIMESTAMP(CURRENT_TIMESTAMP(), 'UTC-3') AS ADLS_AUDIT_DATE

FROM
  cte_temp2 t
LEFT JOIN
  cte_yuno_ultima_transaction y on y.payment_id = t.payment_id
LEFT JOIN
  {l3_foundation_catalog_name}.common.dim_country c ON c.COUNTRY_NAME_DESC = t.COUNTRY_NAME_DESC AND c.COUNTRY_END_DT = '9999-12-31T00:00:00Z'
LEFT JOIN
  cte_currency_rate cr on cr.SOURCE_CURRENCY_CD = c.CURRENCY_CD
                        and date_format(sales_business_dt, 'yMM') = cr.calendar_month_id

 
 """
)

# COMMAND ----------

# MAGIC %md
# MAGIC # 5. Load new data into target table

# COMMAND ----------

start_date = (datetime.strptime(fecha_ayer, '%Y-%m-%d') + timedelta(days=dias_ventana)).strftime('%Y-%m-%d')

sql_clause = f""" sales_business_dt BETWEEN date_add('{fecha_ayer}T00:00:00.000', {dias_ventana}) AND '{fecha_ayer}T23:59:59.999'"""

# COMMAND ----------

load_table_replace(f"{table_full_name}", 
                      'tr_deteccion_fraudes_yuno_TEMP',
                      sql_clause,
                      vacuum_retain_days=1,
                      run_id=pipeline_run_id,
                      optimize_flg=True
                      )