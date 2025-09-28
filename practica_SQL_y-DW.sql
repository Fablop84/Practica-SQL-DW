-- 3. CREAR TABLA DE ivr_detail

/* Vamos a realizar el modelo de datos correspondiente a una IVR de atención al cliente. 
Desde los ficheros ivr_calls, ivr_modules, e ivr_steps crear las tablas con los mismos nombres dentro del dataset keepcoding. 
En ivr_calls encontramos los datos referentes a las llamadas. 
En ivr_modules encontramos los datos correspondientes a los diferentes módulos por los que pasa la llamada. Se relaciona con la tabla de ivr_calls a través del campo ivr_id. 
En ivr_steps encontramos los datos correspondientes a los pasos que da el usuario dentro de un módulo. Se relaciona con la tabla de módulos a través de los campos ivr_id y module_sequence.*/

-- Crear la tabla con el tipo de datos que cada campo debe tener:

CREATE TABLE keepcoding.ivr_detail (
  calls_ivr_id FLOAT64,
  calls_phone_number STRING,
  calls_ivr_result STRING,
  calls_vdn_label STRING,
  calls_start_date TIMESTAMP,
  calls_start_date_id INT64,
  calls_end_date TIMESTAMP,
  calls_end_date_id INT64,
  calls_total_duration FLOAT64,
  calls_customer_segment STRING,
  calls_ivr_language STRING,
  calls_steps_module INT64,
  calls_module_aggregation STRING,
  module_sequece INT64,
  module_name STRING,
  module_duration INT64,
  module_result STRING,
  step_sequence INT64,
  step_name STRING,
  step_result STRING,
  step_description_error STRING,
  document_type STRING,
  document_identification STRING,
  customer_phone STRING,
  billig_account_id STRING
);

-- 3.1 Poblar los campos

/* Insertar los campos en la nueva tabla:
l SELECT se hace cruzando las 3 tablas con los campos que deseo llevar a la tabla recién creada
2 Hago un INSERT INTO dentro de la tabla ivr_details */



INSERT INTO `keepcoding.ivr_detail` 
(calls_ivr_id,
calls_phone_number,
calls_ivr_result,
calls_vdn_label, 
calls_start_date,
calls_start_date_id,
calls_end_date,
calls_end_date_id,
calls_total_duration, 
calls_customer_segment, 
calls_ivr_language,
calls_steps_module, 
calls_module_aggregation,
module_sequece,
module_name,
module_duration, 
module_result,
step_sequence,
step_name,
step_result, 
step_description_error,
document_type,
document_identification,
customer_phone,
billig_account_id)
SELECT 
cal.ivr_id,
cal.phone_number,														  		
cal.ivr_result, 
cal.vdn_label, 
cal.start_date,
CAST(FORMAT_TIMESTAMP('%Y%m%d', cal.start_date) AS INT64), 
cal.end_date,
CAST(FORMAT_TIMESTAMP('%Y%m%d', cal.end_date) AS INT64), 
cal.total_duration, 
cal.customer_segment, 
cal.ivr_language,
cal.steps_module, 
cal.module_aggregation,
modl.module_sequece,
modl.module_name,
modl.module_duration,
modl.module_result,
stp.step_sequence, 
stp.step_name,
stp.step_result, 
stp.step_description_error,
stp.document_type,
stp.document_identification,
stp.customer_phone,
stp.billing_account_id
FROM `keepcoding.ivr_calls` AS cal
LEFT JOIN `keepcoding.ivr_modules` AS modl 
  ON cal.ivr_id = modl.ivr_id
LEFT JOIN `keepcoding.ivr_steps` AS stp 
  ON modl.ivr_id = stp.ivr_id AND modl.module_sequece = stp.module_sequece;

  -- Me quedo mal el nombre de un campo, lo voy a renombrar (me di cuenta en el punto 5)
  ALTER TABLE keepcoding.ivr_detail
  RENAME COLUMN billig_account_id TO billing_account_id;

-- 4. Generar el campo vdn_aggregation

/* Generar el campo para cada llamada, es decir, queremos tener el campo 
calls_ivr_id y el campo vdn_aggregation con la siguiente lógica:  
es una generalización del campo vdn_label. Si vdn_label empieza por ATC pondremos FRONT, si empieza por TECH pondremos TECH si es ABSORPTION dejaremos ABSORPTION y si no es ninguna de las anteriores pondremos RESTO.*/


-- Creo una ETC con el campo vdn_aggregation como vista, no lo agrego a la tabla ivr_detail


WITH aggregation AS (
  SELECT 
    calls_ivr_id,
    calls_vdn_label,
    CASE
      WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT'
      WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
      WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
      ELSE 'RESTO'
    END AS vdn_aggregation
  FROM `keepcoding.ivr_detail`
)
SELECT * FROM aggregation;


-- 5. Generar los campos document_type y document_identification  

/* En ocasiones es posible identificar al cliente en alguno de los pasos de detail obteniendo su tipo de documento y su identificación.  
Como en el ejercicio anterior queremos tener un registro por cada llamada y un sólo cliente identificado para la misma. */ 

SELECT
  calls_ivr_id,
  ANY_VALUE(document_type) AS document_type,
  ANY_VALUE(document_identification) AS document_identification
FROM `keepcoding.ivr_detail`
WHERE document_type NOT IN ('UNKNOWN', 'DESCONOCIDO')
  AND document_identification NOT IN ('UNKNOWN', 'DESCONOCIDO')
GROUP BY calls_ivr_id;

/*Utilizo la función ANY_VALUE porque permite agrupar únicamente por un campo (calls_ivr_id) sin necesidad que tener que agrupar por los otros campos seleccionados. Como en una llamada el document_type y el document_identification del cliente que llama no van a cambiar entre los pasos que pueda recorrer, por eso se puede seleccionar cualquier registro de esos dos campos en un ivr_id (en la llamada). La utilizo para este punto y hasta el punto 7.*/

-- 6. Generar el campo customer_phone 

/* En ocasiones es posible identificar al cliente en alguno de los pasos de detail obteniendo su número de teléfono. Como en el ejercicio anterior queremos tener un registro por cada llamada y un sólo cliente identificado para la misma. */ 

SELECT
  calls_ivr_id,
  ANY_VALUE(customer_phone) AS customer_phone
FROM `keepcoding.ivr_detail`
WHERE customer_phone NOT IN ('UNKNOWN', 'DESCONOCIDO')
GROUP BY calls_ivr_id;

-- Comprobación: La anterior consulta arroja 15.878 registros con algún customer_phone diferente a UNKOWN // 15.878 + 277471 = 293.349 = Total registros de inicio  

/*SELECT
    customer_phone,
    COUNT(*) AS total_desc
FROM `keepcoding.ivr_detail`
WHERE customer_phone = 'UNKNOWN'
group by customer_phone;
Esta consulta arroja 277.471 resultados de registros UNKNOWN*/


-- 7. Generar el campo billing_account_id 

/* En ocasiones es posible identificar al cliente en alguno de los pasos de detail obteniendo su número de cliente. 
Como en el ejercicio anterior queremos tener un registro por cada llamada y un sólo cliente identificado para la misma. */

SELECT
  calls_ivr_id,
  ANY_VALUE(billing_account_id) AS billing_account_id
FROM `keepcoding.ivr_detail`
WHERE billing_account_id NOT IN ('UNKNOWN', 'DESCONOCIDO')
GROUP BY calls_ivr_id;


-- 8. Generar el campo masiva_lg 

/* Como en el ejercicio anterior queremos tener un registro por cada llamada y un flag que indique si la llamada ha pasado por el módulo AVERIA_MASIVA. Si es así indicarlo con un 1 de lo contrario con un 0. Entregar el código en un fichero .sql */

SELECT
  ivr_id AS calls_ivr_id,
  CASE
    WHEN COUNTIF(module_name = 'AVERIA_MASIVA') > 0 THEN 1
    ELSE 0
  END AS masiva_lg
FROM `keepcoding.ivr_modules`
GROUP BY ivr_id;

/* Respuesta: Junto con la función CASE WHEN he incluido el COUNTIF que es un contar los registros con alguna(s) condicion(es), en este caso que cuente los casos que sean mayor a cero con la condición module_name = 'AVERIA MASIVA'. El CASE le coloca un '1' para el resto '0' */


-- 9. Generar el campo info_by_phone_lg 

/* Como en el ejercicio anterior queremos tener un registro por cada llamada y un flag que indique si la llamada pasa por el step de nombre CUSTOMERINFOBYPHONE.TX y su step_result es OK, quiere decir que hemos podido identificar al cliente a través de su número de teléfono. En ese caso pondremos un 1 en este flag, de lo contrario llevará un 0. */


WITH customer_ident_phone AS(
  SELECT
      calls_ivr_id,
    CASE
      WHEN COUNTIF(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK') > 0 THEN 1
      ELSE 0    
    END AS info_by_phone_lg
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id
)
SELECT * FROM customer_ident_phone;


/* Respuesta: Junto con la función CASE WHEN he incluido el COUNTIF que es un contar los registros con alguna(s) condicion(es), en este caso que cuente los que sean mayor a cero con las codndiones step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK' y el CASE le coloca un '1' para el resto '0' */

-- 10. Generar el campo info_by_dni_lg 

/* Como en el ejercicio anterior queremos tener un registro por cada llamada y un flag que indique si la llamada pasa por el step de nombre CUSTOMERINFOBYDNI.TX y su step_result es OK, quiere decir que hemos podido identificar al cliente a través de su número de dni. En ese caso pondremos un 1 en este flag, de lo contrario llevará un 0.*/

WITH customer_ident_dni AS (
  SELECT
    calls_ivr_id,
    CASE
      WHEN COUNTIF(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result = 'OK') > 0 THEN 1
      ELSE 0
    END AS info_by_dni_lg
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id
)
SELECT * FROM customer_ident_dni;

/* Respuesta: Junto con la función CASE WHEN he incluido el COUNTIF que es un contar los registros con alguna(s) condicion(es), en este caso que cuente los que sean mayor a cero con las codndiones step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result = 'OK' y el CASE le coloca un '1' para el resto '0' */



-- 11. Generar los campos repeated_phone_24H, cause_recall_phone_24H
/* Como en el ejercicio anterior queremos tener un registro por cada llamada y dos flags que indiquen si el calls_phone_number tiene una llamada las anteriores 24 horas o en las siguientes 24 horas. En caso afirmativo pondremos un 1 en estos flag, de lo contrario llevará un 0. */

WITH repeated_calls AS (
  SELECT
    bst1.ivr_id AS calls_ivr_id,
    CASE
      WHEN EXISTS (
        SELECT 1
        FROM `keepcoding.ivr_calls` bst2
        WHERE bst1.phone_number = bst2.phone_number
          AND bst1.start_date BETWEEN TIMESTAMP_SUB(bst2.start_date, INTERVAL 24 HOUR) AND bst2.start_date
          AND bst1.ivr_id != bst2.ivr_id
      ) THEN 1 ELSE 0
    END AS repeated_phone_24H,

    CASE
      WHEN EXISTS (
        SELECT 1
        FROM `keepcoding.ivr_calls` bst2
        WHERE bst2.phone_number = bst1.phone_number
          AND bst2.start_date BETWEEN bst1.start_date AND TIMESTAMP_ADD(bst1.start_date, INTERVAL 24 HOUR)
          AND bst2.ivr_id != bst1.ivr_id
      ) THEN 1 ELSE 0
    END AS cause_recall_phone_24H
  FROM `keepcoding.ivr_calls` bst1
)
SELECT * FROM repeated_calls;


/* Respuesta: En este punto la verdad es que tuve que recurrir a la IA porque no sabía como comparar con el mismo campo (start_date) las llamadas que entraron antes y después de 24H. Por eso el uso de los alias bst1 y bst2 que significan (base_regist estado 1 y base_regist estado 2) estado 1 las llamadas del registro "actuales que se evaluan" y estado 2 otras llamadas del mismo número antes o después, con un periodo definido de 24 horas.
Además el uso del WHEN EXISTS que encuentra al menos 1 registro (por eso el SELECT 1) de las condiciones indicadas, en este caso que fueran del mismo número y que las llamadas tuvieran una diferencia de menos de +/- 24 horas */


-- 12. CREAR TABLA DE ivr_summary (Para nota) 
/*Con la base de la tabla ivr_detail y el código de todos los ejercicios anteriores vamos a crear la tabla ivr_sumary . Ésta será un resumen de la llamada donde se incluyen los indicadores más importantes de la llamada. Por tanto, sólo tendrá un registro por llamada. */


-- 12.1 Primero creo que la tabla ivr_summary con los campos solicitados

CREATE TABLE keepcoding.ivr_summary(
  ivr_id STRING,
  phone_number STRING,
  ivr_result STRING,
  vdn_aggregation STRING,
  start_date TIMESTAMP,
  end_date TIMESTAMP,
  total_duration INT64,
  customer_segment STRING,
  ivr_language STRING,
  steps_module INT64,
  module_aggregation STRING,
  document_type STRING,
  document_identification STRING,
  customer_phone STRING,
  billing_account_id STRING,
  masiva_lg INT64,
  info_by_phone_lg INT64,
  info_by_dni_lg INT64,
  repeated_phone_24H INT64,
  cause_recall_phone_24H INT64
);

-- Eliminar registros de la tabla ivr_summary: Cree la tabla y la poblé pero tuve duplicidad de registros, así que he tenido que borrar los registros ya insertados.

DELETE FROM `keepcoding.ivr_summary`WHERE TRUE;

--12.2 Poblar la tabla ivr_detail

INSERT INTO `keepcoding.ivr_summary`
WITH base AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS ivr_id,
    CAST(ANY_VALUE(calls_phone_number) AS STRING) AS phone_number,
    CAST(ANY_VALUE(calls_ivr_result) AS STRING) AS ivr_result,
    CAST(ANY_VALUE(calls_start_date) AS TIMESTAMP) AS start_date,
    CAST(ANY_VALUE(calls_end_date) AS TIMESTAMP) AS end_date,
    CAST(ANY_VALUE(calls_total_duration) AS INT64) AS total_duration,
    CAST(ANY_VALUE(calls_customer_segment) AS STRING) AS customer_segment,
    CAST(ANY_VALUE(calls_ivr_language) AS STRING) AS ivr_language,
    CAST(ANY_VALUE(calls_steps_module) AS INT64) AS steps_module,
    CAST(ANY_VALUE(calls_module_aggregation) AS STRING) AS module_aggregation,
    CAST(ANY_VALUE(calls_vdn_label) AS STRING) AS calls_vdn_label
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id
),

vdn AS (
  SELECT
    ivr_id AS calls_ivr_id,
    CASE
      WHEN STARTS_WITH(calls_vdn_label, 'ATC') THEN 'FRONT'
      WHEN STARTS_WITH(calls_vdn_label, 'TECH') THEN 'TECH'
      WHEN calls_vdn_label = 'ABSORPTION' THEN 'ABSORPTION'
      ELSE 'RESTO'
    END AS vdn_aggregation
  FROM base
),

document AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS calls_ivr_id,
    CAST(ANY_VALUE(document_type) AS STRING) AS document_type,
    CAST(ANY_VALUE(document_identification) AS STRING) AS document_identification
  FROM `keepcoding.ivr_detail`
  WHERE document_type NOT IN ('UNKNOWN', 'DESCONOCIDO')
    AND document_identification NOT IN ('UNKNOWN', 'DESCONOCIDO')
  GROUP BY calls_ivr_id
),

phone AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS calls_ivr_id,
    CAST(ANY_VALUE(customer_phone) AS STRING) AS customer_phone
  FROM `keepcoding.ivr_detail`
  WHERE customer_phone NOT IN ('UNKNOWN', 'DESCONOCIDO')
  GROUP BY calls_ivr_id
),

account AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS calls_ivr_id,
    CAST(ANY_VALUE(billing_account_id) AS STRING) AS billing_account_id
  FROM `keepcoding.ivr_detail`
  WHERE billing_account_id NOT IN ('UNKNOWN', 'DESCONOCIDO')
  GROUP BY calls_ivr_id
),

masive AS (
  SELECT
    CAST(ivr_id AS STRING) AS calls_ivr_id,
    CAST(CASE WHEN COUNTIF(module_name = 'AVERIA_MASIVA') > 0 THEN 1 ELSE 0 END AS INT64) AS masiva_lg
  FROM `keepcoding.ivr_modules`
  GROUP BY ivr_id
),

dni_flag AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS calls_ivr_id,
    CAST(CASE WHEN COUNTIF(step_name = 'CUSTOMERINFOBYDNI.TX' AND step_result = 'OK') > 0 THEN 1 ELSE 0 END AS INT64) AS info_by_dni_lg
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id
),

phone_flag AS (
  SELECT
    CAST(calls_ivr_id AS STRING) AS calls_ivr_id,
    CAST(CASE WHEN COUNTIF(step_name = 'CUSTOMERINFOBYPHONE.TX' AND step_result = 'OK') > 0 THEN 1 ELSE 0 END AS INT64) AS info_by_phone_lg
  FROM `keepcoding.ivr_detail`
  GROUP BY calls_ivr_id
),

repeated_calls AS (
  SELECT
    CAST(bst1.ivr_id AS STRING) AS calls_ivr_id,
    CAST(CASE
      WHEN EXISTS (
        SELECT 1
        FROM `keepcoding.ivr_calls` bst2
        WHERE CAST(bst1.phone_number AS STRING) = CAST(bst2.phone_number AS STRING)
          AND bst1.start_date BETWEEN TIMESTAMP_SUB(bst2.start_date, INTERVAL 24 HOUR) AND bst2.start_date
          AND bst1.ivr_id != bst2.ivr_id
      ) THEN 1 ELSE 0
    END AS INT64) AS repeated_phone_24H,

    CAST(CASE
      WHEN EXISTS (
        SELECT 1
        FROM `keepcoding.ivr_calls` bst2
        WHERE CAST(bst2.phone_number AS STRING) = CAST(bst1.phone_number AS STRING)
          AND bst2.start_date BETWEEN bst1.start_date AND TIMESTAMP_ADD(bst1.start_date, INTERVAL 24 HOUR)
          AND bst2.ivr_id != bst1.ivr_id
      ) THEN 1 ELSE 0
    END AS INT64) AS cause_recall_phone_24H
  FROM `keepcoding.ivr_calls` bst1
)

SELECT
  base.ivr_id,
  base.phone_number,
  base.ivr_result,
  vdn.vdn_aggregation,
  base.start_date,
  base.end_date,
  base.total_duration,
  base.customer_segment,
  base.ivr_language,
  base.steps_module,
  base.module_aggregation,
  document.document_type,
  document.document_identification,
  phone.customer_phone,
  account.billing_account_id,
  masive.masiva_lg,
  phone_flag.info_by_phone_lg,
  dni_flag.info_by_dni_lg,
  repeated_calls.repeated_phone_24H,
  repeated_calls.cause_recall_phone_24H
FROM base
LEFT JOIN vdn ON base.ivr_id = vdn.calls_ivr_id
LEFT JOIN document ON base.ivr_id = document.calls_ivr_id
LEFT JOIN phone ON base.ivr_id = phone.calls_ivr_id
LEFT JOIN account ON base.ivr_id = account.calls_ivr_id
LEFT JOIN masive ON base.ivr_id = masive.calls_ivr_id
LEFT JOIN phone_flag ON base.ivr_id = phone_flag.calls_ivr_id
LEFT JOIN dni_flag ON base.ivr_id = dni_flag.calls_ivr_id
LEFT JOIN repeated_calls ON base.ivr_id = repeated_calls.calls_ivr_id;



/* Respuesta: Este punto ha sido muy complejo de realizar, debo aceptar que tuve que recurrir a la IA para que me ayudará a revisar el código de la query y he tenido problemas con los tipos de datos para hacer los cruces y agregar a la tabla ivr_summary, por eso la solución de colocar CAST en cada bloque. Además tuve problemas con la duplicidad de datos, tuve que revisar bien la CTE base para que no trajera registros duplicados, por eso el uso del ANY_VALUE explicitamente en cada campo. En este punto tomé la decisión de no "aliasar" las tablas pues necesitaba revisar de manera clara de dónde provenia cada campo*/


-- validacion de duplicados

SELECT
  COUNT(*) AS total_registros,
  COUNT(DISTINCT ivr_id) AS llamadas_unicas
FROM `keepcoding.ivr_summary`;

-- 13. CREAR FUNCIÓN DE LIMPIEZA DE ENTEROS 
/*Crear una función de limpieza de enteros por la que si entra un null la función devuelva el valor -999999. */

CREATE FUNCTION `keepcoding.clean_integ`(input_value INT64)
RETURNS INT64
AS (
  IFNULL(input_value, -999999)
);

-- la función IFNULL es más directa y sencilla que hacerlo con un CASE WHEN


