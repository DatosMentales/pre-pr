Tu tarea es analizar el siguiente script SQL y determinar si cumple con los siguientes estándares:
1. Nombres en snake_case y en inglés
2. Tipos de datos consistentes con el modelo existente.
3. Convenciones semánticas coherentes.
Algunas recomendaciones para el uso de sufijos:
para Clave o Identificador utilizar sufijo Id si el tipo de dato es numérico o Cd si el datos es tipo texto - Ejemplo: Item_id o Order_Type_Cd
para Código Numérico que No son clave utilizar sufijo Num – Ejemplo: Order_Line_Num
para Descripciones o Nombres utilizar sufijo Desc o Desc1 o Desc2 – Ejemplo: Client_Desc
para Descripciones Ampliadas o Texto Libre utilizar los sufijos Txt o Txt1 o Txt2 – Ejemplo: Sales_Order_Commet_Txt
para Montos utilizar el sufijo Amt – Ejemplo: Local_Curr_Tot_Prc_Amt
para Cantidades utilizar el sufijo Qty – Ejemplo: Shipped_Item_Qty
para Porcentajes utilizar sufijo Pct – Ejemplo: Order_Discount_Pct
para Flags utilizar sufijo Flg o Ind – Ejemplo: Question_Active_Flg
para Fechas utilizar el sufijo Dt – Ejemplo: Calendar_Dt
para Fechas-Hora utilizar el sufijo Dttm – Ejemplo: Start_Action_Dttm
para Hora utilizar el sufijo Tm – Ej. Work_Order_Start_Tm

Tambien verificar que las tablas contengan cluster por los campos clave.
