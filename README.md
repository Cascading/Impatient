part1
	implements the simplest possible Cascading app
	this just copies each text line from source tap to sink tap
	roughly, in about a dozen lines of code
	physical plan: 1 Mapper

part2
	implements a simple example of WordCount
	uses a regex to split the input text lines into a token stream
	automatically switch between Hfs and Lfs
	generates a DOT file, to show the Cascading flow graphically
	physical plan: 1 Mapper, 1 Reducer

part3
	uses a custom Function to scrub the token stream
	shows how to sort the output (ascending, based on token counts)
	physical plan: 1 Mapper, 1 Reducer

part4
	shows how to use a HashJoin on two pipes
	filters a list of stop words out of the token stream
	physical plan: 2 Mappers, 2 Reducers

part5
	calculates TF-IDF
	shows how to use a Cascade
	physical plan: 10 Mappers, 10 Reducers

part6
	includes unit tests in the build
	shows how to use checkpoints, assertions, traps, debug

part7
	has a switch to run the example instead in local mode
