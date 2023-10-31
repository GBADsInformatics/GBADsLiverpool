#%% About
'''
William Gilbert has written a function in R to calculate the slaughter day(s) for poultry flocks
to achieve a target weight, with the option to specify a first-cut thinning weight. This reflects
the practice of thinning poultry flocks part way through the growout, which is common in the UK.

This code tests the initial version of William's function.
'''
#%% Preliminaries

r_executable = 'C:\\Program Files\\R\\R-4.2.0\\bin\\x64\\Rscript.exe'    # Full path to the Rscript executable

#%% Test with basic R

r_script = os.path.join(CURRENT_FOLDER ,'test_script.R')    # Full path to the R program you want to run
r_args = ['arg_1' ,'arg_2']     # Arguments to R function, as list of strings
run_cmd([r_executable ,r_script] + r_args)

# def run_r_script(
#       R_EXECUTABLE     # String: Full path to the Rscript executable
#       ,R_SCRIPT        # String: Full path to the R program you want to run
#       ,R_ARGS          # List of strings: Arguments to R function
#       ):
#    cmd = [R_EXECUTABLE ,R_SCRIPT] + R_ARGS
#    cmd_status = subprocess.run(cmd, capture_output=True, shell=SHELL)

#    stdout_list = []
#    stdout_txt  = cmd_status.stdout.decode()
#    stdout_list = stdout_txt.strip().splitlines()

#    return stdout_list

#%% Test with William's code

r_script = os.path.join(CURRENT_FOLDER ,'initial placement m2 JR EDIT.R')    # Full path to the R program you want to run
# r_args = [     # Arguments to R function, as list of strings
#    '2'        # weight at thinning. (zero means no thinning, single lift at end of flock) (kg live, target weight)
#    ,'2.5'      # target weight at house clearance
#    ,'0'        # time to thinning ( if 0 will calculate using breed standard)
#    ,'0'        # time to clear (if 0 will use breed standard)
#    ,'2'        # stocking limit, kg/m^2
#    ,'0.05'     # total mortality (0:1) as a proportion, not percentage
#    ,'0.5'      # (0:1) proportion of total mortality in first 7 days, 0.5 as suggested starting point
# ]
r_args = [     # Arguments to R function, as list of strings
   'w_thin=2' # weight at thinning
            # (zero means no thinning, single lift at
            # end of flock) (kg live, target weight)

   ,'w_clear=2.5' # target weight at house clearance

   ,'t_thin=0' #time to thinning
   # ( if 0 will calculate using breed standard)

   ,'t_clear=0' # time to clear
   # (if 0 will use breed standard)

   ,'max_density=2' #stocking limit, kg/m^2

   ,'total_mortality=0.05'# (0:1) as a proportion,
   # not percentage

   ,'d7_proportion=0.5'
]
run_cmd([r_executable ,r_script] + r_args)
