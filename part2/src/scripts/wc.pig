-- kudos to Dmitriy Ryaboy

docPipe = LOAD '$docPath'
  USING PigStorage('\t', 'tagsource') AS (doc_id, text);
docPipe = FILTER docPipe BY doc_id != 'doc_id';

-- specify a regex to split "document" text lines into token stream
tokenPipe = FOREACH docPipe
  GENERATE doc_id, FLATTEN(TOKENIZE(text, ' [](),.')) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

-- determine the word counts
tokenGroups = GROUP tokenPipe BY token;
wcPipe = FOREACH tokenGroups
  GENERATE group AS token, COUNT(tokenPipe) AS count;

-- output
STORE wcPipe INTO '$wcPath'
  USING PigStorage('\t', 'tagsource');
EXPLAIN -out dot/wc_pig.dot -dot wcPipe;
