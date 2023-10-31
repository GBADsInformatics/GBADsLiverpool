# To get the name of an object, as a string
def getobjectname(OBJECT):
    objectname = [x for x in globals() if globals()[x] is OBJECT][0]
    return objectname

# To time a piece of code
def timerstart(LABEL=None):      # String (opt): add a label to the printed timer messages
   global _timerstart ,_timerstart_label
   funcname = inspect.currentframe().f_code.co_name
   _timerstart_label = LABEL
   _timerstart = dt.datetime.now()
   if _timerstart_label:
      print(f"\n<{funcname}> {_timerstart_label} {_timerstart :%I:%M:%S %p} \n")
   else:
      print(f"\n<{funcname}> {_timerstart :%I:%M:%S %p} \n")
   return None

def timerstop():
   global _timerstop
   funcname = inspect.currentframe().f_code.co_name
   if '_timerstart' in globals():
      _timerstop = dt.datetime.now()
      elapsed = _timerstop - _timerstart
      hours = (elapsed.days * 24) + (elapsed.seconds // 3600)
      minutes = (elapsed.seconds % 3600) // 60
      seconds = (elapsed.seconds % 60) + (elapsed.microseconds / 1000000)
      print(f"\n<{funcname}> {_timerstop :%I:%M:%S %p}")
      if _timerstart_label:
         print(f"<{funcname}> {_timerstart_label} Elapsed {hours}h: {minutes}m: {seconds :.1f}s \n")
      else:
         print(f"<{funcname}> Elapsed {hours}h: {minutes}m: {seconds :.1f}s \n")
   else:
      print(f"<{funcname}> Error: no start time defined. Call timerstart() first.")
   return None

# To get index(es) of a dataframe as regular columns
# Must assign the output to a dataframe e.g. df = indextocolumns(df).
def indextocolumns(
      INPUT_DF
      ,RESET_INDEX=True      # Set False to retain the original index column(s) as an index. Otherwise will reset index to simple numeric.
   ):
   dfmod = pd.merge(left=INPUT_DF.index.to_frame() ,right=INPUT_DF ,left_index=True ,right_index=True)
   if RESET_INDEX:
      dfmod.reset_index(drop=True ,inplace=True)
   return dfmod

# To turn column indexes into names. Will remove multi-indexing.
# Must assign the output to a dataframe e.g. df = colnames_from_index(df).
def colnames_from_index(INPUT_DF):
   cols = list(INPUT_DF)
   cols_new = []
   for item in cols:
      if type(item) == str:   # Columns that already have names will be strings. Use unchanged.
         cols_new.append(item)
      else:   # Columns that are indexed or multi-indexed will appear as tuples. Turn them into strings joined by underscores.
         cols_new.append('_'.join(str(i) for i in item))   # Convert each element of tuple to string before joining. Avoids error if an element is nan.

   # Write dataframe with new column names
   dfmod = INPUT_DF
   dfmod.columns = cols_new
   return dfmod

# To clean up column names in a dataframe
def cleancolnames(INPUT_DF):
   # Comments inside the statement create errors. Putting all comments at the top.
   # Convert to lowercase
   # Strip leading and trailing spaces, then replace spaces with underscore
   # Replace slashes, parenthesis, and brackets with underscore
   # Replace some special characters with underscore
   # Replace other special characters with words
   INPUT_DF.columns = INPUT_DF.columns.str.lower() \
      .str.strip().str.replace(' ' ,'_') \
      .str.replace('/' ,'_').str.replace('\\' ,'_') \
      .str.replace('(' ,'_').str.replace(')' ,'_') \
      .str.replace('[' ,'_').str.replace(']' ,'_') \
      .str.replace('{' ,'_').str.replace('}' ,'_') \
      .str.replace('!' ,'_').str.replace('?' ,'_') \
      .str.replace('-' ,'_').str.replace('+' ,'_') \
      .str.replace('^' ,'_').str.replace('*' ,'_') \
      .str.replace('.' ,'_').str.replace(',' ,'_') \
      .str.replace('|' ,'_').str.replace('#' ,'_') \
      .str.replace('>' ,'_gt_') \
      .str.replace('<' ,'_lt_') \
      .str.replace('=' ,'_eq_') \
      .str.replace('@' ,'_at_') \
      .str.replace('$' ,'_dol_') \
      .str.replace('%' ,'_pct_') \
      .str.replace('&' ,'_and_')
   return None

# To print df.info() with header for readability, and optionally write data info to text file
def datainfo(
      INPUT_DF
      ,OUTFOLDER=None     # String (opt): folder to output {dataname}_info.txt. If None, no file will be created.
   ):
   funcname = inspect.currentframe().f_code.co_name
   dataname = [x for x in globals() if globals()[x] is INPUT_DF][0]
   rowcount = INPUT_DF.shape[0]
   colcount = INPUT_DF.shape[1]
   idxcols = str(list(INPUT_DF.index.names))
   header = f"Data name: {dataname :>26s}\nRows:      {rowcount :>26,}\nColumns:   {colcount :>26,}\nIndex:     {idxcols :>26s}\n"
   divider = ('-'*26) + ('-'*11) + '\n'
   bigdivider = ('='*26) + ('='*11) + '\n'
   print(bigdivider + header + divider)
   INPUT_DF.info()
   print(divider + f"End:       {dataname:>26s}\n" + bigdivider)

   if OUTFOLDER:     # If something has been passed to OUTFOLDER parameter
      filename = f"{dataname}_info"
      print(f"\n<{funcname}> Creating file {OUTFOLDER}\{filename}.txt")
      datetimestamp = 'Created on ' + time.strftime('%Y-%m-%d %X', time.gmtime()) + ' UTC' + '\n'
      buffer = io.StringIO()
      INPUT_DF.info(buf=buffer, max_cols=colcount)
      filecontents = header + divider + datetimestamp + buffer.getvalue()
      tofile = os.path.join(OUTFOLDER, f"{filename}.txt")
      with open(tofile, 'w', encoding='utf-8') as f: f.write(filecontents)
      print(f"<{funcname}> ...done.")
   return None

# To create output files for a basic description of data
# {dataname}_desc_numeric.csv: mean, SD, min, max, and quartiles for each numeric column (the output of df.describe, transposed)
# {dataname}_desc_categorical.csv: count of distinct values and measure of imbalance for each categorical column
# {dataname}_hdtl.csv: first 50 and last 50 rows of the data
# {dataname}_rand.csv: a random sample of 100 rows of the data
def datadesc(
      INPUT_DF
      ,OUTFOLDER          # Folder to write CSV files to
      ,NROWS_HEADTAIL=50  # Number of rows to include from each of head() and tail()
      ,NROWS_RAND=100     # Number of rows to include in random sample
   ):
   funcname = inspect.currentframe().f_code.co_name
   dataname = [x for x in globals() if globals()[x] is INPUT_DF][0]

   # Get description of numeric columns and write to file
   filename = f"{dataname}_desc_numeric"
   print(f"\n<{funcname}> Creating file 1 of 4: {OUTFOLDER}\{filename}.csv")
   desc = INPUT_DF.describe(percentiles=[.01 ,.05 ,.10 ,.25 ,.5 ,.75 ,.90 ,.95 ,.99]).transpose()
   desc.rename_axis('column_name', axis='index', inplace=True)
   desc.to_csv(os.path.join(OUTFOLDER, f"{filename}.csv"))
   print(f"<{funcname}> ...done.")

   # Get description of categorical columns and write to file
   filename = f"{dataname}_desc_categorical"
   print(f"\n<{funcname}> Creating file 2 of 4: {OUTFOLDER}\{filename}.csv")
   ctgcols = list(INPUT_DF.select_dtypes(exclude='number'))   # Get list of non-numeric columns
   ctgdesc_aslist = []
   for COL in ctgcols:
      freq = INPUT_DF[COL].value_counts()   # Frequency table
      nobs = freq.sum()                     # Number of non-missing observations
      ndist = freq.shape[0]                 # Number of distinct values
      try:
         ent = sps.entropy(freq)            # Entropy. Requires package scipy.stats as sps.
      except:
         ent = np.nan
      # Get most frequent values IF there are enough distinct values
      try:
         mfv1 = freq.index[0]
         mfc1 = freq[0]
      except:
         mfv1 = ' '
         mfc1 = ' '
      try:
         mfv2 = freq.index[1]
         mfc2 = freq[1]
      except:
         mfv2 = ' '
         mfc2 = ' '
      try:
         mfv3 = freq.index[2]
         mfc3 = freq[2]
      except:
         mfv3 = ' '
         mfc3 = ' '
      ctgrow = [{
         "column_name":COL
         ,"count":nobs
         ,"distinct_values":ndist
         ,"entropy":ent
         ,"most_freq_value_1":mfv1 ,"most_freq_value_1_count":mfc1
         ,"most_freq_value_2":mfv2 ,"most_freq_value_2_count":mfc2
         ,"most_freq_value_3":mfv3 ,"most_freq_value_3_count":mfc3
         }]
      ctgdesc_aslist.extend(ctgrow)
   ctgdesc = pd.DataFrame.from_dict(ctgdesc_aslist ,orient='columns')     # Convert list of dictionaries into data frame
   ctgdesc.to_csv(os.path.join(OUTFOLDER, f"{filename}.csv") ,index=False)
   print(f"<{funcname}> ...done.")

   # Get Head and Tail and write to file
   filename = f"{dataname}_hdtl{NROWS_HEADTAIL * 2}"
   print(f"\n<{funcname}> Creating file 3 of 4: {OUTFOLDER}\{filename}.csv")
   head = INPUT_DF.head(NROWS_HEADTAIL)
   tail = INPUT_DF.tail(NROWS_HEADTAIL)
   headtail = head.append(tail)
   headtail.to_csv(os.path.join(OUTFOLDER, f"{filename}.csv"))
   print(f"<{funcname}> ...done.")

   # Create random sample of rows and write to file
   filename = f"{dataname}_rand{NROWS_RAND}"
   print(f"\n<{funcname}> Creating file 4 of 4: {OUTFOLDER}\{filename}.csv")
   rows_to_sample = min(NROWS_RAND ,INPUT_DF.shape[0])   # Use smaller of NROWS_RAND or number of rows in data
   sample = INPUT_DF.sample(n=rows_to_sample ,random_state=12345)
   sample.to_csv(os.path.join(OUTFOLDER, f"{filename}.csv"))
   print(f"<{funcname}> ...done.")
   return None

# Histogram with descriptive statistics
def plot_histogram_withinset(
      INPUTDF                 # DataFrame: Pandas data frame to use
      ,VAR                    # String: variable to plot
      ,NBINS=20               # Integer: number of bins for histogram
      ,WHERE_VAR_GTE=None     # Number (optional): filter records before plotting where VAR >= this value
      ,WHERE_VAR_LTE=None     # Number (optional): filter records before plotting where VAR <= this value. Note can pass np.inf.
   ):
   funcname = inspect.currentframe().f_code.co_name

   # Check user-specified filters
   if WHERE_VAR_GTE:
      if WHERE_VAR_LTE:                         # If both have been specified...
         if (WHERE_VAR_GTE > WHERE_VAR_LTE):    # ...and lower bound is larger...
            print(f'<{funcname}> WARNING: User-specified lower bound is greater than upper bound!')

   # Filter data
   plotdf = INPUTDF
   if WHERE_VAR_GTE:
      plotdf = plotdf.loc[INPUTDF[VAR] >= WHERE_VAR_GTE]
   if WHERE_VAR_LTE:
      plotdf = plotdf.loc[INPUTDF[VAR] <= WHERE_VAR_LTE]

   # Create histogram
   sns.displot(plotdf[VAR] ,kind='hist' ,stat='probability' ,bins=NBINS)
   plt.ylabel('Proportion of Records')

   # Get basic statistics
   nobs = plotdf.shape[0]
   minimum = plotdf[VAR].min()
   maximum = plotdf[VAR].max()
   mean = plotdf[VAR].mean()
   median = plotdf[VAR].median()
   std = plotdf[VAR].std()

   # Add inset
   plt.annotate(
      f'''
      N: {nobs :>20,.0f}
      Min: {minimum :>18,.2f}
      Median: {median :>15,.2f}
      Max: {maximum :>18,.2f}
      Mean: {mean :>17,.2f}
      SD: {std :>19,.2f}
      '''
      ,xy=(0.4 ,0.7)
      ,xycoords='axes fraction'
      ,family='monospace'                                # Use monospace font so spacing is consistent
   )
   if (minimum < 0) & (maximum > 0):                     # If variable can be both positive and negative...
      plt.axvline(x=0 ,linestyle='-' ,color='grey')      # ...add reference line at zero

   # Add footnote indicating filters, if any
   if WHERE_VAR_GTE:
      if WHERE_VAR_LTE:
         footnote_text = f'Showing records where {WHERE_VAR_GTE} <= {VAR} <= {WHERE_VAR_LTE}'
      else:
         footnote_text = f'Showing records where {VAR} >= {WHERE_VAR_GTE}'
   elif WHERE_VAR_LTE:
         footnote_text = f'Showing records where {VAR} <= {WHERE_VAR_LTE}'
   else:
      footnote_text = None
   if footnote_text:
      plt.figtext(0.99 ,0.01 ,footnote_text ,horizontalalignment='right' ,fontsize=8)

   return None

# Run a command on the command line using subprocess package
# Example usage: run_cmd(['dir' ,'c:\\users'] ,SHELL=True ,SHOW_MAXLINES=10)
# To run an R program:
   # r_executable = 'C:\\Program Files\\R\\R-4.0.3\\bin\\x64\\Rscript'    # Full path to the Rscript executable
   # r_script = os.path.join(CURRENT_FOLDER ,'test_script.r')             # Full path to the R program you want to run
   # r_args = ['3']                                                       # List of arguments to pass to script, if any
   # run_cmd([r_executable ,r_script] + r_args)
def run_cmd(
      CMD                 # String or List of strings: the command to run. IMPORTANT: use double backslashes (\\) so they are not interpreted as escape characters.
      ,SHELL=False        # True: submit CMD to command prompt (for builtin commands: dir, del, mkdir, etc.). False (default): run another program. First argument in CMD must be an executable.
      ,SHOW_MAXLINES=99
   ):
   funcname = inspect.currentframe().f_code.co_name

   print(f'\n<{funcname}> Running command:\n    {" ".join(CMD)}')
   cmd_status = subprocess.run(CMD, capture_output=True, shell=SHELL)

   stderr_list = []
   stdout_list = []
   if cmd_status.stderr:
      stderr_txt  = cmd_status.stderr.decode()
      stderr_list = stderr_txt.strip().splitlines()
      print(f'\n<{funcname}> stderr messages:')
      for line in stderr_list:
         print(f'    {line}')
   if cmd_status.stdout:
      stdout_txt  = cmd_status.stdout.decode()
      stdout_list = stdout_txt.strip().splitlines()
   if SHOW_MAXLINES:
      print(f'\n<{funcname}> stdout messages (max={SHOW_MAXLINES}):')
      for line in stdout_list[:SHOW_MAXLINES]:
         print(f'    {line}')
   print(f'<{funcname}> Ended with returncode = {cmd_status.returncode}')

   return None    # If you want to use something that is returned, add it here. Assign it when you call the function e.g. returned_object = run_cmd().
