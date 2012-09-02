docPipe = LOAD '$docPath' USING PigStorage('\t', 'tagsource') AS (doc_id, text);
docPipe = FILTER docPipe BY doc_id != 'doc_id';

-- specify a regex operation to split the "document" text lines into a token stream
tokenPipe = FOREACH docPipe GENERATE doc_id, FLATTEN(TOKENIZE(LOWER(text), ' [](),.')) AS token;

-- part 3 of this tutorial teaches how to implement a custom function for cleaning the tokens.
-- here, we simply use the built-in LOWER() UDF to do so.
-- You can see the complete implementation of the UDF here:
-- https://github.com/apache/pig/blob/trunk/src/org/apache/pig/builtin/LOWER.java

tokenPipe = FILTER tokenPipe BY token MATCHES '\\w.*';
-- DUMP tokenPipe;

-- determine the word counts
tokenGroups = GROUP tokenPipe BY token;
wcPipe = FOREACH tokenGroups GENERATE group AS token, COUNT(tokenPipe) AS count;

-- output
STORE wcPipe INTO '$wcPath' using PigStorage('\t', 'tagsource');
EXPLAIN -out dot/wc_pig.dot -dot wcPipe;
