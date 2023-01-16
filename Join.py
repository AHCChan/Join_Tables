HELP_DOC = """
JOIN TABLES
(version 1.0)
by Angelo Chan

This is a program for joining two table files into one table file. A new table
file is created. The input files are not affected.

Accepted file formats:
    - TSV (Tab-Separated Values)
    - CSV (Comma-Separated Values)
    - SSV (Space-Separated Values)



USAGE:
    
    python27 Join.py <input_path_left> <{input_format_left}> <key_columns_left>
            <input_path_right> <{input_format_right}> <key_columns_right>
            [-o <output_path> {output_format}] [-t <join_type>] [-s <sort>]
            [-h <headers>]



MANDATORY:
    
    input_path_left / input_path_right
        
        The filepaths of the input files.
    
    input_format_left / input_format_right
        
        The file format of the input file. Acceptable options are:
            tsv - Tab-separated values
            csv - Comma-separated values
            ssv - Space-separated values
    
    key_columns_left / key_columns_right
        
        A comma separated list of the columns on which the join occurs.
        Note that the column system uses the 1-index system. (The first column
        is column 1)

OPTIONAL:
    
    output_path
        
        The filepath of the output file. If no filepath is specified, one will
        automatically be generated using the names of the input files and the
        join type used.
    
    output_format
        
        The file format of the output file. If no format is specified, the
        output format will be the same as the input format of the "left" input
        file. Acceptable options are:
            tsv - Tab-separated values
            csv - Comma-separated values
            ssv - Space-separated values
    
    join_type
        
        (DEFAULT: inner)
        
        The type of join to be carried out. Acceptable options are:
            inner - Inner join (key must be found in both)
            left  - Left join (key must be found in left table)
            right - Right join (key must be found in right table)
            outer - Outer join (key can be found in either table)
            xor   - Exclusive or (key must only be found in one table)
    
    sort
        
        (DEFAULT: Y)
        
        Whether or not to sort by key.
    
    headers
        
        (DEFAULT: N)
        
        Whether or not the first row should be treated as column headers.



EXAMPLES EXPLANATION:
    
    

EXAMPLES:
    
    

USAGE:
    
    python27 Join.py <input_path_left> <{input_format_left}> <key_columns_left>
            <input_path_right> <{input_format_right}> <key_columns_right>
            [-o <output_path> {output_format}] [-t <join_type>] [-s <sort>]
            [-h <headers>]
"""



# Configurations ###############################################################

AUTORUN = True

WRITE_PREVENT = False # Completely prevent overwritting existing files
WRITE_CONFIRM = True # Check to confirm overwritting existing files

PRINT_ERRORS = True
PRINT_PROGRESS = True
PRINT_METRICS = True



# Defaults #####################################################################
"NOTE: altering these will not alter the values displayed in the HELP DOC"

DEFAULT__join = 1 #INNER
DEFAULT__sort = True
DEFAULT__headers = False



# Imported Modules #############################################################

import sys
import os



# Enums ########################################################################

class JOIN:
    INNER=1
    LEFT=2
    RIGHT=3
    OUTER=4
    XOR=5



# Strings ######################################################################

STR__use_help = "\nUse the -h option for help:\n\t python Join.py -h"

STR__no_inputs = "\nERROR: No inputs were given."
STR__insufficient_inputs = "\nERROR: Not enough basic inputs were given."

STR__IO_error_read = "\nERROR: Input file does not exist or could not be "\
        "opened:\n\r{s}"
STR__overwrite_confirm = "\nFile already exists. Do you wish to overwrite it? "\
        "(y/n): "
STR__IO_error_write_forbid = """
ERROR: You specified an output file which already exists and the administrator
for this program has forbidden all overwrites. Please specify a different
output file, move the currently existing file, or configure the default options
in Join.py."""
STR__IO_error_write_unable = """
ERROR: Unable to write to the specified output file."""
STR__invalid_file_format = """
ERROR: Invalid {io} file format: {s}
Please specify one of:
    tsv
    csv
    ssv"""

STR__invalid_keys = """
ERROR: Invalid key column(s) specified: {s}
Please specify the column(s) which contain(s) the values which comprise the
key."""

STR__unequal_keys = "\nERROR: Unequal number of key columns."

STR__invalid_flag = "\nERROR: Invalid flag: {s}"

STR__not_enough_values = "ERROR: Not enough values were given after flag: {s}"

STR__invalid_join = """
ERROR: Invalid join type: {s}
Please specify one of:
    inner
    left
    right
    outer
    xor"""

STR__invalid_bool = """
ERROR: Invalid boolean: {s}
Please specify one of:
    yes
    no"""



STR__metrics_lines = """
    METRICS:

     Lines (O): {A}
     Lines (L): {B} ({C}%)
     Lines (R): {D} ({E}%)

    Columns(O): {F}
    Columns(L): {G}
    Columns(R): {H}
"""

STR__parsing_args = "\nParsing arguments..."

STR__join_begin = "\nRunning Join..."

STR__join_complete = "\nJoin successfully finished."



# Lists ########################################################################

LIST__help = ["-h", "-H", "-help", "-Help", "-HELP"]

LIST__yes = ["Y", "y", "YES", "Yes", "yes", "T", "t", "TRUE", "True", "true"]
LIST__no = ["N", "n", "NO", "No", "no", "F", "f", "FALSE", "False", "false"]

LIST__tsv = ["\t", "T", "t", "TSV", "Tsv", "tsv", "TAB", "Tab", "tab"]
LIST__csv = [",", "C", "c", "CSV", "Csv", "csv", "COMMA", "Comma", "comma"]
LIST__ssv = [" ", "S", "s", "SSV", "Ssv", "ssv", "SPACE", "Space", "space"]

LIST__inner = ["I", "i", "IN", "In", "in", "INNER", "Inner", "inner"]
LIST__left = ["L", "l", "LEFT", "Left", "left"]
LIST__right = ["R", "r", "RIGHT", "Right", "right"]
LIST__outer = ["O", "o", "OUT", "Out", "out", "OUTER", "Outer", "outer"]
LIST__xor = ["X", "x", "XOR", "Xor", "xor"]



# Dictionaries #################################################################

DICT__delim = {}
for i in LIST__tsv: DICT__delim[i] = "\t"
for i in LIST__csv: DICT__delim[i] = ","
for i in LIST__ssv: DICT__delim[i] = " "



DICT__join = {}
for i in LIST__inner: DICT__join[i] = JOIN.INNER
for i in LIST__left: DICT__join[i] = JOIN.LEFT
for i in LIST__right: DICT__join[i] = JOIN.RIGHT
for i in LIST__outer: DICT__join[i] = JOIN.OUTER
for i in LIST__xor: DICT__join[i] = JOIN.XOR



DICT__delim_format = {
    "\t": "tsv",
    ",": "csv",
    " ": "ssb"}

DICT__join_str = {
    JOIN.INNER = "INNER_JOIN"
    JOIN.LEFT = "LEFT_JOIN"
    JOIN.RIGHT = "RIGHT_JOIN"
    JOIN.OUTER = "OUTER_JOIN"
    JOIN.XOR = "XOR



# File Processing Code #########################################################

def Join_Tables(path_l, delim_l, keys_l, path_r, delim_r, keys_r, path_out,
            delim_out, join, sort, headers)
    """
    """
    return



# Command Line Parsing #########################################################

def Parse_Command_Line_Input__Join_Tables(raw_command_line_input):
    """
    Parse the command line input and call the Join_Tables function with
    appropriate arguments if the command line input is valid.
    """
    printP(STR__parsing_args)
    # Remove the runtime environment variable and program name from the inputs
    inputs = Strip_Non_Inputs(raw_command_line_input)

    # No inputs
    if not inputs:
        printE(STR__no_inputs)
        printE(STR__use_help)
        return 1
    
    # Help option
    if inputs[0] in LIST__help:
        print(HELP_DOC)
        return 0

    # Initial validation
    if len(inputs) < 6:
        printE(STR__insufficient_inputs)
        printE(STR__use_help)
        return 1
    
    valid_in_l = Validate_Read_Path(inputs[0])
    if valid_in_l == 1:
        printE(STR__IO_error_read.format(s = inputs[0]))
        return 1
    valid_in_r = Validate_Read_Path(inputs[3])
    if valid_in_r == 1:
        printE(STR__IO_error_read.format(s = inputs[3]))
        return 1
    
    delim_l = Validate_File_Format(inputs[1])
    if not delim_l:
        printE(STR__invalid_file_format.format(io = "input", s = inputs[1]))
        return 1
    delib_r = Validate_File_Format(inputs[4])
    if not delim_r:
        printE(STR__invalid_file_format.format(io = "input", s = inputs[4]))
        return 1
    
    keys_l = Validate_Keys(inputs[2])
    if not keys_l:
        printE(STR__invalid_keys.format(s = inputs[2]))
        return 1
    keys_r = Validate_Keys(inputs[2])
    if not keys_r:
        printE(STR__invalid_keys.format(s = inputs[5]))
        return 1
    if len(keys_l) != len(keys_r):
        printE(STR__unequal_keys)
        return 1
    
    path_l = inputs.pop(0)
    inputs.pop(0)
    inputs.pop(0)
    path_r = inputs.pop(0)
    inputs.pop(0)
    inputs.pop(0)
    
    
    
    # Set up rest of the parsing
    path_out = ""
    format_out = delim_in # Default behaviour
    join = DEFAULT__join
    sort = DEFAULT__sort
    headers = DEFAULT__headers
    
    # Parse the rest
    while inputs:
        arg = inputs.pop(0)
        try:
            if arg in ["-o"]:
                arg2 = inputs.pop(0)
                arg3 = inputs.pop(0)
            elif arg in ["-t", "-s", "-h"]:
                arg2 = inputs.pop(0)
            else:
                printE(STR__invalid_flag.format(s = arg))
                return 1
        except:
            printE(STR__not_enough_values.format(s = arg))
            return 1
        if arg == "-o": # Output file format
            valid = Validate_Write_Path(arg2)
            if valid == 2:
                return 1
            if valid == 3:
                printE(STR__IO_error_write_forbid)
                return 1
            elif valid == 4:
                printE(STR__IO_error_write_unable)
                return 1
            path_out = arg2
            delim = Validate_File_Format(arg3)
            if delim:
                delim_out = delim
            else:
                printE(STR__invalid_file_format.format(io = "output", s = arg3))
                return 1
        elif arg in ["-s", "-h"]:
            bool_ = Validate_Bool(arg2)
            if bool_ == None:
                printE(STR__invalid_bool.format(s = arg2))
            else:
                if arg == "-s": sort = bool_
                else: headers = bool_
        else: # "-j" Flag - Join options
            join = DICT__join.get(arg2, 0)
            if not join:
                printE(STR__invalid_join.format(s = arg2))
                return 1
    
    # Default path generation
    if not path_out:
        delim_out = delim_l
        path_out = Generate_Output_Filename(path_l, path_r, join, delim_l)
    
    # Run program
    exit_code = Join_Tables(path_l, delim_l, keys_l, path_r, delim_r, keys_r,
            path_out, delim_out, join, sort, headers)
    
    # Irregular exit codes
    
    # Safe exit
    return 0



def Validate_Read_Path(filepath):
    """
    Validates the filepath of the input file.
    Return 0 if the filepath is valid.
    Return 1 otherwise.
    
    Validate_Read_Path(str) -> int
    """
    try:
        f = open(filepath, "U")
        f.close()
        return 0
    except:
        return 1




def Generate_Output_Filename(path_l, path_r, join, delim):
    """
    Generate a filepath for the output based on the input files, the join
    method, and the delimiter used for the left file.
    
    Generate_Output_Filename(str, str, int) -> str
    """
    dirname = os.path.dirname(path_l)
    file_l = Get_File_Name(path_l)
    file_r = Get_File_Name(path_r)
    join_str = DICT__join_str[join]
    format_ = DICT__delim_format[delim]
    return (dirname + "\\" + file_l + "__" + join_str + "__" + file_r + "." +
            format_)



def Get_File_Name(filepath):
    """
    Return the name of the file, minus the file extension, for the file
    specified by [filepath].
    
    Assumes [filepath] is the filepath for a file. If a directory path is
    supplied, an empty string will also be returned.
    
    Get_File_Name(str) -> str
    """
    # Slash and backslash
    index_slash = filepath.rfind("/")
    index_bslash = filepath.rfind("\\")
    if index_slash == index_bslash == -1: pass # Simple path
    else: # Complex path
        right_most = max(index_slash, index_bslash)
        filepath = filepath[right_most+1:]
    # Find period
    index_period = filepath.rfind(".")
    if index_period == -1: return filepath
    return filepath[:index_period]


    
def Validate_Write_Path(filepath):
    """
    Validates the filepath of the output file.
    Return 0 if the filepath is writtable.
    Return 1 if the user decides to overwrite an existing file.
    Return 2 if the user declines to overwrite an existing file.
    Return 3 if the file exists and the program is set to forbid overwriting.
    Return 4 if the program is unable to write to the filepath specified.
    
    Validate_Write_Path(str) -> int
    """
    try:
        f = open(filepath, "U")
        f.close()
    except: # File does not exist. 
        try:
            f = open(filepath, "w")
            f.close()
            return 0 # File does not exist and it is possible to write
        except:
            return 4 # File does not exist but it is not possible to write
    # File exists
    if WRITE_PREVENT: return 3
    if WRITE_CONFIRM:
        confirm = raw_input(STR__overwrite_confirm)
        if confirm not in LIST__yes: return 2
    # User is not prevented from overwritting and may have chosen to overwrite
    try:
        f = open(filepath, "w")
        f.close()
        if WRITE_CONFIRM: return 1 # User has chosen to overwrite existing file
        return 0 # Overwriting existing file is possible
    except:
        return 4 # Unable to write to specified filepath



def Validate_File_Format(string):
    """
    Validates the file format specified.
    Return the appropriate delimiter.
    Return an empty string if the file format is invalid.
    
    Validate_File_Format(str) -> str
    """
    return DICT__delim.get(string, "")



def Validate_Keys(string):
    """
    Validates and returns a list of columns numbers which comprise the table
    key.
    Return an empty list if the input is invalid.
    
    The input column numbers follow a 1-index system, but the output list of
    integers follows a 0-index system.
    
    @string
        (str)
        A comma separated list of integers.
    
    Validate_Key(str) -> list<int>
    """
    result = []
    temp = string.split(",")
    for s in temp:
        num = Validate_Column_Number
        if num == 0: return []
        result.append(num-1)
    return result



def Validate_Column_Number(string):
    """
    Validates and returns the column number specified.
    Returns the column number under an index 1 system if valid.
    Return 0 if the input is invalid.
    
    @string
        (str)
        A string denoting the column number under the index 1 system.
        
    Validate_Column_Number(str) -> int
    """
    try:
        n = int(string)
    except:
        return 0
    if n < 0: return 0
    return n # Input is in index 1 system, output is in index 1



def Validate_Bool(string):
    """
    Validates and returns the boolean specified. Accepts variants of Yes, No,
    True, and False.
    Return a boolean if the string is valid.
    Return None if the string is invalid.
    
    @string
        (str)
        A string denoting a boolean. (Includes Yes/No)
    
    Validate_Bool(str) -> bool
    Validate_Bool(str) -> None
    """
    if string in LIST__yes: return True
    elif string in LIST__no: return False
    else: return None



def Strip_Non_Inputs(list1):
    """
    Remove the runtime environment variable and program name from the inputs.
    Assumes this module was called and the name of this module is in the list of
    command line inputs.
    
    Strip_Non_Inputs(list) -> list
    """
    if "Join" in list1[0]: return list1[1:]
    return list1[2:]



# Controlled Print Statements ##################################################

def printE(string):
    """
    A wrapper for the basic print statement.
    It is intended to be used for printing error messages.
    It can be controlled by a global variable.
    """
    if PRINT_ERRORS: print(string)

def printP(string):
    """
    A wrapper for the basic print statement.
    It is intended to be used for printing progress messages.
    It can be controlled by a global variable.
    """
    if PRINT_PROGRESS: print(string)

def printM(string):
    """
    A wrapper for the basic print statement.
    It is intended to be used for printing file metrics.
    It can be controlled by a global variable.
    """
    if PRINT_METRICS: print(string)



# Main Loop ####################################################################

if AUTORUN and (__name__ == "__main__"):
    exit_code = Parse_Command_Line_Input__Join_Tables(sys.argv)
