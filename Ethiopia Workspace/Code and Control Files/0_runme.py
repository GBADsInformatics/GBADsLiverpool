# Execute or Import control files from current working directory
exec(open('_libraries.py').read())    # Execute _libraries file. This contains IMPORT statements.
exec(open('_functions.py').read())    # Execute _functions file so that functions are defined in main namespace.
import _constants as uc               # User-defined constants
imp.reload(uc)                        # Reload, in case module has been updated in middle of session

# Make these all-uppercase to clean up Spyder variable explorer
CURRENT_FOLDER = os.getcwd()
PARENT_FOLDER = os.path.dirname(CURRENT_FOLDER)
GRANDPARENT_FOLDER = os.path.dirname(PARENT_FOLDER)

# Folder for shared code with Liverpool
ETHIOPIA_CODE_FOLDER = CURRENT_FOLDER
ETHIOPIA_OUTPUT_FOLDER = os.path.join(PARENT_FOLDER ,'Program outputs')
ETHIOPIA_DATA_FOLDER = os.path.join(PARENT_FOLDER ,'Data')

DASH_DATA_FOLDER = os.path.join(GRANDPARENT_FOLDER, 'AHLE Dashboard' ,'Dash App' ,'data')
