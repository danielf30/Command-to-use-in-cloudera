--# @author("Daniel Fajardo")
--# @description("Scripts hql y comandos unix para el consolidado de conocimientos en Applied Intelligence - Accenture")
--# @comment("Cualquier variable, esquema, tabla, ruta aquí mencionados son inventadas, cualquier parecido con la realidad es pura coincidencia. :)")
--# @version("1.0.0")

--# Da nombre al proceso en ejecución dentro de cloudera que se puede visualizar en dashboar de hadoop
set mapred.job.name = '__PAIS__ - __SECUENCIA__';
set hive.auto.convert.join=false;

--# Ejecución de consultas desde consola unix con hive
hive -f /path/someFile.hql
hive -e 'some query'

--# Ejecución de scripts py para encontrar errores en el código
/usr/bin/spark-submit --master yarn-client script_path >> log_path

--# Eliminar partición
ALTER TABLE scheme.table DROP IF EXISTS PARTITION(partition=202104); 

--# Para mover archivos de un servidor a otro se usa el siguiente comando. 
--## scp -r ip: path_donde_llegaría_el_archivo el_archivo | con el . pasan todos los archivos en el directorio especificado
get file_path.zip /path_directory/
set path_directory/file.zip path_directory_sftp
scp -r 11.111.111.111:/path_directory/ file.zip
scp -r 11.111.111.111:/path_directory/ .

--# Crear una tabla a partir de archivo csv
--## Se crea primeramente la tabla con la estructura
CREATE TABLE IF NOT EXISTS scheme.table (atrib int);
--## Posterior se insertan los datos
hive -e 'LOAD DATA LOCAL INPATH "/path/archivo.csv" OVERWRITE INTO TABLE scheme.table;'

--# Subir archivo txt directamente a hadoop, tener en cuenta que al crear la entidad sea de tipo textfile
hive -e 'drop table if exists esquema.tabla_tmp;'
hive -e "CREATE TABLE IF NOT EXISTS esquema.tabla_tmp (atrib type comment 'comment here', ...) ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' stored as TEXTFILE;"
tail -n +2 path_txt/file.txt > path_txt/file_noheader.txt
hdfs dfs -put path_txt/file_noheader.txt path_hdfs_tmp
hive -e "set hive.exec.dynamic.partition.mode=nonstrict; insert into esquema.tabla partition(periodo) select atrib1, atrib2, atribn from esquema.tabla_tmp;"

--# Casteo de fechas de biginteger a timestamp y viceversa
select cast(cast(unix_timestamp('01-12-2019' , 'dd-MM-yyyy') as double)as bigint)*1000;
select cast(1588024800000/1000 as timestamp);

--# Visualización de metadata
describe formatted scheme.table;
describe scheme.table;

--# Conocer la volumnetría
--## Con esta línea de comando extraemos la ubicación de las entidades en el data warehouse
ANALYZE TABLE scheme.table COMPUTE STATISTICS noscan;
--## Con la ubicación definica se pasa la siguiente línea de comando por consola
hdfs dfs -du -s -h 	hdfs://hdfs_path/scheme.db/table_name

--# Escribir los resultados de una query en una ruta específica dentro de unix 
--## Donde >> sirve para insertar datos en un documento existente
hive -e "select * from scheme.table" > /unix_path/query_results.csv
hive -e "select * from scheme.table limit 100" > /unix_path/query_results.csv

--# Diferencia exacta en meses entre dos fechas
(cast(year(cast(atrib_fecha1/1000 as TIMESTAMP)) as int) - cast(year(atrib_fecha2) as int) )*12 +  (cast(month(cast(atrib_fecha1/1000 as TIMESTAMP)) as int) - cast(month(atrib_fecha2) as int) )  +  IF(day(cast(atrib_fecha1/1000 as TIMESTAMP))  > day(atrib_fecha2), 1, 0 )

--# Sumatoria de campos con valores null 
--## Se utiliza la función nvl
SELECT 
id,
id_2,
(nvl(saldo_1,0) +
 nvl(saldo_2,0) +
 nvl(saldo_3,0) +
 nvl(saldo_4,0) +
 nvl(saldo_5,0) +
 nvl(saldo_6,0)) saldo,
[every column]
FROM scheme.table LIMIT 100;

--# Encuentra registros duplicados por cada agrupación de atributos
select [every column], count(*)
from mytable
group by [every column]
having count(*) > 1;

--# Solución para obtener todas las entradas
SELECT *
FROM (
SELECT t1.*, 
       t2.price,
       ROW_NUMBER() OVER(PARTITION by some_id ORDER BY timestamp_field) as row_count
FROM table1 AS t1
JOIN table2 AS t2
ON COALESCE(t1.string_field, '') = COALESCE(t2.string_field, '')
)
WHERE row_count = 1

--# Cuando se necesita rendimiento, reescribo las consultas definidas con tablas temporales en una consulta definida con la instrucción WITH.
WITH helper_table1 AS (
  SELECT *
  FROM table_1
  WHERE field = 1
),
helper_table2 AS (
  SELECT *
  FROM table_2
  WHERE field = 1
),
helper_table3 AS (
  SELECT *
  FROM helper_table1 as ht1
  JOIN helper_table2 as ht2
  ON ht1.field = ht2.field
)
SELECT * FROM helper_table3;

--# Calcula la suma acumulada desde el primer registro hasta el registro actual.
sum(atrib) OVER (PARTITION BY CustomerID BY ts ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cumulative_sum

--## [preceding]	= del primero al último | [following]	= del último al primero
select 
	  object_id
	, [preceding]	= count(*) over(order by object_id ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW )
	, [central]	= count(*) over(order by object_id ROWS BETWEEN 2 PRECEDING AND 2 FOLLOWING )
	, [following]	= count(*) over(order by object_id ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
from sys.objects
order by object_id asc

--# LAG es una función de ventana que genera una fila que viene antes de la fila actual.
--# LEAD es una función de ventana que genera una fila que viene después de la fila actual, esencialmente lo contrario que LAG.
LAG(Monthly_Revenue, 1) OVER (PARTITION BY CustomerID ORDER BY Month) AS Previous_Month_Revenue,
LEAD(Monthly_Revenue, 1) OVER (PARTITION BY CustomerID ORDER BY Month) AS Next_Month_Revenue

--## Elimina los campos duplicados
create table __OWNER_DESTINO__.__TABLA_ORIGEN___ stored as orc as
WITH CTE AS
( 
SELECT [every column],
ROW_NUMBER()OVER(PARTITION BY id, cod_row ORDER BY fecha desc) RN
FROM __OWNER_DESTINO__.__TABLA_ORIGEN__ tmp
)
SELECT * 
FROM CTE WHERE RN = 1;