docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE FLATTEN(TOKENIZE(LOWER(text))) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w+';

-- determine the word counts
tokenGroups = GROUP tokenPipe BY token;
wcPipe = FOREACH tokenGroups GENERATE COUNT(tokenPipe) AS count, group AS token;
 
STORE wcPipe INTO '$wcPath' using PigStorage('\t', 'tagsource');
