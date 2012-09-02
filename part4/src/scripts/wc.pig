set pig.exec.mapPartAgg true

docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);
docPipe = FILTER docPipe BY doc_id != 'doc_id';

stopPipe = LOAD '$stopPath' USING PigStorage('\t', 'tagsource') AS (stop:chararray);
stopPipe = FILTER stopPipe BY stop != 'stop';

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE doc_id, FLATTEN(TOKENIZE(LOWER(text), ' [](),.')) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

--- perform a left join to remove stop words, discarding the rows
--- which joined with stop words, i.e., were non-null after left join
tokenPipe = JOIN tokenPipe BY token LEFT, stopPipe BY stop using 'replicated';
tokenPipe = FILTER tokenPipe BY stopPipe::stop is NULL;

-- DUMP tokenPipe;

-- determine the word counts
tokenGroups = GROUP tokenPipe BY token;
wcPipe = FOREACH tokenGroups GENERATE group AS token, COUNT(tokenPipe) AS count;

-- output
STORE wcPipe INTO '$wcPath' using PigStorage('\t', 'tagsource');
--explain wcPipe
-- EXPLAIN -out dot/wc_pig.dot -dot wcPipe;
