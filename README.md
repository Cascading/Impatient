simple1
	implements the simplest possible Cascading app
	this just copies each text line from source tap to sink tap
	physical plan: 1 Mapper

simple2
	automatically switch between Hfs and Lfs
	uses a regex to split the input text lines into a token stream
	generates a DOT file, to show the Cascading flow graphically
	physical plan: 1 Mapper, 1 Reducer

simple3
	uses a custom Function to scrub the token stream
	shows how to sort the output (ascending, based on token counts)
	physical plan: 1 Mapper, 1 Reducer

simple4
	shows how to use a HashJoin on two pipes
	filters a list of stop words out of the token stream
	physical plan: 2 Mappers, 2 Reducers

simple5
	calculates TF-IDF
	shows how to use a Cascade
	physical plan: 10 Mappers, 10 Reducers

simple6
	includes unit tests in the build
	shows how to use checkpoints, assertions, traps, debug

simple7
	has a switch to run the example instead in local mode
