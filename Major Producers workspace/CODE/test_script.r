cmd_args <- commandArgs(trailingOnly = TRUE)    # Fetch command line arguments
print('Congratulations, you ran an R script!')
print('Passed arguments:')
cat(cmd_args)     # cat() will add things to the stdout stream
