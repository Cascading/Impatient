docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);
stopPipe = LOAD '$stopPath' USING PigStorage('\t', 'tagsource') AS (stop:chararray);

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE FLATTEN(TOKENIZE(LOWER(text))) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w+';

-- perform a left join to remove stop words, discarding the rows
-- which joined with stop words, i.e., were non-null after left join
tokenPipe = JOIN tokenPipe BY $0 LEFT, stopPipe BY $0;
filterPipe = FILTER tokenPipe BY $1 is NULL;

-- determine the word counts
tokenGroups = GROUP filterPipe BY token;
wcPipe = FOREACH tokenGroups GENERATE COUNT(filterPipe) AS count, group AS token;
 
STORE wcPipe INTO '$wcPath' using PigStorage('\t', 'tagsource');
