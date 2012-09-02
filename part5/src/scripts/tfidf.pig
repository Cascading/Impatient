set pig.exec.mapPartAgg true

docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);
docPipe = FILTER docPipe BY doc_id != 'doc_id';

stopPipe = LOAD '$stopPath' USING PigStorage('\t', 'tagsource') AS (stop:chararray);
stopPipe = FILTER stopPipe BY stop != 'stop';

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE doc_id, FLATTEN(TOKENIZE(LOWER(text), ' [](),.')) AS token;
tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';

-- perform a left join to remove stop words, discarding the rows
-- which joined with stop words, i.e., were non-null after left join
tokenPipe = JOIN tokenPipe BY token LEFT, stopPipe BY stop using 'replicated';
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
dPipe = FOREACH dGroups {
  GENERATE COUNT(dPipe) AS n_docs;
}

-- one branch tallies the token counts for document frequency (DF)
-- note that here, we calculate distinct inside the foreach, whereas
-- for global count, we used the top-level DISTINCT operator.
-- the difference is that one is slower (requires an MR job), but extremely
-- scalable; the other is done in memory, on a reducer, per-group.
-- since here we expect much smaller groups, we favor the method that will
-- be faster and not produce an extra MR job.
tokenGroups = GROUP tokenPipe BY token;
dfPipe = FOREACH tokenGroups {
  dfPipe = distinct tokenPipe.doc_id;
  GENERATE group AS df_token, COUNT(dfPipe) AS df_count;
}
-- DUMP dfPipe;

-- join to bring together all the components for calculating TF-IDF 
tfidfPipe = JOIN tfPipe BY tf_token, dfPipe BY df_token;
-- Note how we refer to dPipe.n_docs , even though it's a relation we didn't join in!
-- That's a special case for single-tuple relations that allows one to simply treat them as
-- constants. Seem more here: http://squarecog.wordpress.com/2010/12/19/new-features-in-apache-pig-0-8/
tfidfPipe = FOREACH tfidfPipe GENERATE
  doc_id,
  (double) tf_count * LOG( (double) dPipe.n_docs / ( 1.0 + (double) df_count ) ) AS tfidf,
  tf_token AS token;

-- output
STORE tfidfPipe INTO '$tfidfPath' using PigStorage('\t', '-tagsource -schema');
-- EXPLAIN -out dot/tfidf_pig.dot -dot tfidfPipe;

-- determine the word counts
tokenGroups = GROUP tokenPipe BY token;
wcPipe = FOREACH tokenGroups GENERATE group AS token, COUNT(tokenPipe) AS count;
wcPipe = ORDER wcPipe BY count DESC;
STORE wcPipe INTO '$wcPath' using PigStorage('\t', '-tagsource -schema');
