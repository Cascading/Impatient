-- Steve Severance
-- http://stackoverflow.com/questions/10039949/word-count-program-in-hive

CREATE TABLE input (line STRING);

LOAD DATA LOCAL INPATH 'input.tsv' 
OVERWRITE INTO TABLE input;

SELECT
 word, COUNT(*)
FROM input 
 LATERAL VIEW explode(split(text, ' ')) lTable AS word 
GROUP BY word
;