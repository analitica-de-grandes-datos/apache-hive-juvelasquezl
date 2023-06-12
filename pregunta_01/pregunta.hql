/* 
Pregunta
===========================================================================

Para resolver esta pregunta use el archivo `data.tsv`.

Compute la cantidad de registros por cada letra de la columna 1.
Escriba el resultado ordenado por letra. 

Apache Hive se ejecutará en modo local (sin HDFS).

Escriba el resultado a la carpeta `output` de directorio de trabajo.

        >>> Escriba su respuesta a partir de este punto <<<
*/

DROP TABLE IF EXISTS FILES;
DROP TABLE IF EXISTS WORD_COUNTS;
DROP TABLE IF EXISTS ULTIMA;

CREATE TABLE FILES (LINE STRING);
LOAD DATA LOCAL INPATH './data.tsv' overwrite INTO TABLE FILES;

CREATE TABLE WORD_COUNTS  AS
SELECT WORD, COUNT(1) AS COUNT FROM 
(SELECT EXPLODE (split(LINE, '\\s')) AS WORD FROM FILES) W
GROUP BY WORD
ORDER BY WORD DESC
lIMIT 5;

CREATE TABLE ULTIMA AS
SELECT * FROM WORD_COUNTS 
ORDER BY WORD;

INSERT OVERWRITE DIRECTORY 'output/'
ROW FORMAT DELIMITED FIELDS TERMINATED BY ','
SELECT * FROM ULTIMA;