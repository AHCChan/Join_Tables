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
            [-h <headers>] [-i <integers>]



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
        
        (DEFAULT: forward)
        
        Whether or not to sort by key, and how to sort. Acceptable options are:
            no      - No sorting. Retain original order.
            forward - Forward sorting.
                          (Ascending value, alphabetical order)
            reverse - Reverse sorting.
                          (Descending value, reverse alphabetical order)
    
    headers
        
        (DEFAULT: N)
        
        Whether or not the first row should be treated as column headers.
    
    integers
        
        (DEFAULT: Y)
        
        Whether or not to treat columns which look like integers as integers for
        the purpose of sorting.



EXAMPLES EXPLANATION:
    
    

EXAMPLES:
    
    

USAGE:
    
    python27 Join.py <input_path_left> <{input_format_left}> <key_columns_left>
            <input_path_right> <{input_format_right}> <key_columns_right>
            [-o <output_path> {output_format}] [-t <join_type>] [-s <sort>]
            [-h <headers>] [-i <integers>]
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
DEFAULT__sort = 2 #FORWARD
DEFAULT__headers = False
DEFAULT__integers = True



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

class SORT:
    NO=1
    FORWARD=2
    REVERSE=3



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

STR__invalid_sort = """
ERROR: Invalid sorting method: {s}
Please specify one of:
    no
    forward
    reverse"""


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

LIST__forward = ["F", "f", "FORWARD", "Forward", "forward"]
LIST__reverse = ["R", "r", "REVERSE", "Reverse", "reverse"]

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
    JOIN.XOR = "XOR"}



# File Processing Code #########################################################

def Join_Tables(path_l, delim_l, keys_l, path_r, delim_r, keys_r, path_out,
            delim_out, join, sort, headers, integers):
    """
    Return an exit code of 1/2 if table width is inconsistent in the left/right
    table.
    """
    printP(STR__join_begin)
    
    # Key types and width check
    key_types, width_l, width_r = Get_Key_Types()
    if type(key_types) == int: return key_types
    if not integers:key_types = len(keys_l) * [False]
    
    # Headers and blanks
    header_values = []
    if headers:
        header_values = Get_Header_Values(path_l, delim_l, keys_l, path_r,
            delim_r, keys_r, join)
    blank_l = width_l*delim_out
    blank_r = width_r*delim_out
    
    # Process inputs
    data_l = Process_Table(path_l, delim_l, keys_l, key_types, headers)
    data_r = Process_Table(path_r, delim_r, keys_r, key_types, headers)
    dict_l, keys_l = data_l
    dict_r, keys_r = data_r
    
    # Sorting
    if sort == SORT.FORWARD:
        keys_l = sorted(keys_l, None, None, False)
    elif sort == SORT.REVERSE:
        keys_l = sorted(keys_l, None, None, True)
    
    # Join tables
    Write_Table__DICTs(dict_l, keys_l, blank_l, dict_r, keys_r, blank_r,
            path_out, delim_out, join, headers)
    
    #
    return 0

def Get_Key_Types(path_l, delim_l, keys_l, path_r, delim_r, keys_r, headers):
    """
    Return 1/2 if the left/right table has an inconsistent number of columns.
    """
    # Verify widths and get key types
    keys_l, width_l = Get_Key_Types_(path_l, delim_l, keys_l, headers)
    if not keys_l: return 1
    keys_r, width_r = Get_Key_Types_(path_r, delim_r, keys_r, headers)
    if not keys_r: return 2
    # Combine key types
    results = []
    range_ = range(len(keys_l))
    for i in range_:
        if keys_l[i] and keys_r[i]: results.append(True)
        else: results.append(False)
    #
    return [results, width_l, width_r]

def Get_Key_Types_(filepath, delim, keys, headers):
    """
    """
    # Setup
    results = len(keys)*[True]
    key_len = len(keys)
    range_ = range(key_len)
    width = 0
    # Width
    f = open(filepath, "U")
    line = f.readline()
    values = line.split(delim)
    width = len(values)
    f.close()
    # Processing
    f = open(filepath, "U")
    if headers: f.readline()
    line = f.readline()
    while line:
        # Initial processing
        values = line.split(delim)
        if values[-1][-1] == "\n": values[-1] = values[-1][:-1]
        # Values
        for i in range_:
            key_i = keys[i]
            value = values[key_i]
            if not value.isdigit(): results[i] = False
        # Next
        line = f.readline()
    # Close and return
    f.close()
    return [results, width-key_len]

def Get_Header_Values(path_l, delim_l, keys_l, path_r, delim_r, keys_r, join):
    """
    """
    results = []
    # Values
    f_l = open(path_l, "U")
    line_l = f.readline()
    values_l = line_l.split(delim_l)
    f_l.close()
    f_r = open(path_r, "U")
    line_r = f.readline()
    values_r = line_r.split(delim_r)
    f_r.close()
    # Key headers
    if join in [JOIN.INNER, JOIN.LEFT, JOIN.OUTER, JOIN.XOR]:
        for i in keys_l:
            value = values_l[i]
            results.append(value)
    else: # Right join
        for i in keys_r:
            value = values_r[i]
            results.append(value)
    # Sort and pop
    keys_sorted_l = sorted(keys_l, None, None, True)
    for i in keys_sorted_l:
        values_l.pop(l)
    keys_sorted_r = sorted(keys_r, None, None, True)
    for i in keys_sorted_r:
        values_r.pop(l)
    # Add and return
    results = results + values_l + values_r
    return results

def Process_Table(filepath, delim, keys, key_types, headers):
    """
    Return [dictionary, keys]
    """
    # Setup
    results_data = {}
    results_keys = []
    #
    range_ = range(len(keys))
    f = open(filepath, "U")
    # Sort for popping
    sorted_keys = sorted(keys, None, None, True)
    # Header and first line
    if headers: f.readline()
    line = f.readline()
    # Iterate
    while line:
        # String to values
        values = line.split(delim)
        if values[-1][-1] == "\n": values[-1] = values[-1][:-1]
        # Key
        key = []
        for i in range_:
            key_i = keys[i]
            is_int = key_types[i]
            value = values[key_i]
            if is_int: value = int(value)
            key.append(value)
        key = tuple(key)
        # Pop
        for i in sorted_keys: values.pop(i)
        # Process
        results_data[key] = values
        results_keys.append(key)
        # Next
        line = f.readline()
    #
    f.close()
    return [results_data, results_keys]

def Write_Table__DICTs(dict_l, keys_l, blank_l, dict_r, keys_r, blank_r,
            path_out, delim_out, join, headers):
    """
    """
    # Setup
    o = open(path_out, "w")
    # Headers
    if headers:
        header_str = delim_out.join(headers) + "\n"
        o.write(header_str)
    # Join type
    if join in JOIN.INNER:
        all_keys = []
        for key in keys_l:
            if key in dict_r: all_keys.append(key)
    elif join == JOIN.LEFT:
        all_keys = keys_l
    elif join == JOIN.RIGHT:
        all_keys = keys_r
    elif join == JOIN.OUTER:
        all_keys = keys_l
        for k in keys_r:
            if k not in dict_l:
                all_keys.append(k)
    else: # XOR
        all_keys = []
        for key in keys_l:
            if key not in dict_r: all_keys.append(key)
        for key in keys_r:
            if key not in dict_l: all_keys.append(key)
    # Iterate 
    for key in all_keys:
        key_list = list(key)
        key_str = delim_out.join(key_list)
        o.write(key_str)
        if join == JOIN.INNER:
            val_l = dict_l[key]
            str_l = delim_out.join(val_l)
            if str_l: o.write(delim_out + str_l)
            val_r = dict_r[key]
            str_r = delim_out.join(val_r)
            if str_r: o.write(delim_out + str_r)
        elif join == JOIN.LEFT:
            val_l = dict_l[key]
            str_l = delim_out.join(val_l)
            if str_l: o.write(delim_out + str_l)
            if key in dict_r:
                val_r = dict_r[key]
                str_r = delim_out.join(val_r)
                if str_r: o.write(delim_out + str_r)
            else:
                o.write(blank_r)
        elif join == JOIN.RIGHT:
            if key in dict_r:
                val_l = dict_l[key]
                str_l = delim_out.join(val_l)
                if str_l: o.write(delim_out + str_l)
            else:
                o.write(blank_l)
            val_r = dict_r[key]
            str_r = delim_out.join(val_r)
            if str_r: o.write(delim_out + str_r)
        else: # Outer or Xor
            if key in dict_r:
                val_l = dict_l[key]
                str_l = delim_out.join(val_l)
                if str_l: o.write(delim_out + str_l)
            else:
                o.write(blank_l)
            if key in dict_r:
                val_r = dict_r[key]
                str_r = delim_out.join(val_r)
                if str_r: o.write(delim_out + str_r)
            else:
                o.write(blank_r)
        o.write("\n")
    # 
    o.close()
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
    integers = DEFAULT__integers
    
    # Parse the rest
    while inputs:
        arg = inputs.pop(0)
        try:
            if arg in ["-o"]:
                arg2 = inputs.pop(0)
                arg3 = inputs.pop(0)
            elif arg in ["-t", "-s", "-h", "-i"]:
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
        elif arg in ["-s", "-h", "-i"]:
            bool_ = Validate_Bool(arg2)
            if bool_ == None:
                printE(STR__invalid_bool.format(s = arg2))
                return 1
            else:
                if arg == "-h": headers = bool_
                elif arg == "-i": integers = bool_
        elif arg in ["-s"]:
            sort = Validate_Sort(arg2)
            if not sort:
                printE(STR__invalid_sort.format(s = arg2))
                return 1
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
            path_out, delim_out, join, sort, headers, integers)
    
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



def Validate_Sort(string)
    """
    Validate and return the enum corresponding to the type of sorting to be
    used.
    Return 0 if no valid string was provided.
    
    @string
        (str)
        A string denoting the sorting method.
    """
    if string in LIST__no: return SORT.NO
    if string in LIST__forward: return SORT.FORWARD
    if string in LIST__reverse: return SORT.REVERSE
    return 0



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
