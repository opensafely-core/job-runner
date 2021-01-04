. local license : env STATA_LICENSE
. file open output using "output/env.txt", write text
. file write output "`license'" 
. file close output

