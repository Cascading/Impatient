docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);
docPipe = FILTER docPipe BY doc_id != 'doc_id';

stopPipe = LOAD '$stopPath' USING PigStorage('\t', 'tagsource') AS (stop:chararray);
stopPipe = FILTER stopPipe BY stop != 'stop';

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE doc_id, FLATTEN(TOKENIZE(LOWER(text), ' [](),.')) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

-- perform a left join to remove stop words, discarding the rows
-- which joined with stop words, i.e., were non-null after left join
tokenPipe = JOIN tokenPipe BY token LEFT, stopPipe BY stop;
tokenPipe = FILTER tokenPipe BY stopPipe::stop is NULL;
-- DUMP tokenPipe;

-- one branch of the flow tallies the token counts for term frequency (TF)
tfGroups = GROUP tokenPipe BY (doc_id, token);
tfPipe = FOREACH tfGroups GENERATE FLATTEN(group) AS (doc_id, tf_token), COUNT(tokenPipe) AS tf_count;
-- DUMP tfPipe;

-- one branch counts the number of documents (D)
dPipe = FOREACH tokenPipe GENERATE doc_id;
dPipe = DISTINCT dPipe;
dGroups = GROUP dPipe ALL;
dPipe = FOREACH dGroups GENERATE COUNT(dPipe) AS n_docs;
-- DUMP dPipe;

-- one branch tallies the token counts for document frequency (DF)
dfPipe = DISTINCT tokenPipe;
dfGroups = GROUP dfPipe BY token;
dfPipe = FOREACH dfGroups GENERATE group AS df_token, COUNT(dfPipe) AS df_count;
-- DUMP dfPipe;

-- join to bring together all the components for calculating TF-IDF 
idfPipe = CROSS dfPipe, dPipe;
tfidfPipe = JOIN tfPipe BY tf_token, idfPipe BY df_token;
tfidfPipe = FOREACH tfidfPipe GENERATE doc_id, (double) tf_count * LOG( (double) n_docs / ( 1.0 + (double) df_count ) ) AS tfidf, tf_token AS token;

-- output
STORE tfidfPipe INTO '$tfidfPath' using PigStorage('\t', 'tagsource');
EXPLAIN -out dot/tfidf_pig.dot -dot tfidfPipe;

-- determine the word counts
-- THIS PART DIES IN APACHE PIG W/O HELPFUL EXCEPTION MESSAGES
--tokenGroups = GROUP tokenPipe BY token;
--wcPipe = FOREACH tokenGroups GENERATE COUNT(tokenPipe) AS count, group AS token;
--wcPipe = ORDER wcPipe BY count DESC;
--STORE wcPipe INTO '$wcPath' using PigStorage('\t', 'tagsource');
