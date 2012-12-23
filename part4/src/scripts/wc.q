-- prepare DDL for loading the raw data

CREATE TABLE raw_docs (
 doc_id STRING,
 text STRING
)
ROW FORMAT DELIMITED                             
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
;

CREATE TABLE raw_stop (
 stop STRING
)
ROW FORMAT DELIMITED                             
FIELDS TERMINATED BY '\t'
STORED AS TEXTFILE
;

-- load the raw data

LOAD DATA 
LOCAL INPATH 'data/rain.txt' 
OVERWRITE INTO TABLE raw_docs
;

LOAD DATA 
LOCAL INPATH 'data/en.stop' 
OVERWRITE INTO TABLE raw_stop
;

-- additional steps to remove headers, yay

CREATE TABLE docs (
 doc_id STRING,
 text STRING
)
;

INSERT OVERWRITE TABLE docs
SELECT
 *
FROM raw_docs
WHERE doc_id <> 'doc_id'
;

CREATE TABLE stop (
 stop STRING
)
;

INSERT OVERWRITE TABLE stop
SELECT
 *
FROM raw_stop
WHERE stop <> 'stop'
;

-- tokenize using external Python script

CREATE TABLE tokens (
 token STRING
)
;

INSERT OVERWRITE TABLE tokens
SELECT
 TRANSFORM(text) USING 'python ./src/scripts/tokenizer.py' AS token
FROM docs
;

-- filter with a left join, then count

SELECT token, COUNT(*) AS count
FROM (
  SELECT
   *
  FROM tokens LEFT OUTER JOIN stop
   ON (tokens.token = stop.stop)
  WHERE stop IS NULL
) t
GROUP BY token
;
