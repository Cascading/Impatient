CREATE TABLE text_docs (line STRING);

LOAD DATA LOCAL INPATH 'data/rain.txt'
OVERWRITE INTO TABLE text_docs
;

SELECT
 word, COUNT(*)
FROM 
(SELECT
  split(line, '\t')[1] AS text
 FROM text_docs
) t
LATERAL VIEW explode(split(text, '[ ,\.\(\)]')) lTable AS word
GROUP BY word
;
