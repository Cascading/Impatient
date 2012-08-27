copyPipe = LOAD '$inPath' USING PigStorage('\t', 'tagsource');
STORE copyPipe INTO '$outPath' using PigStorage('\t', 'tagsource');
