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
    
    unique_only
        
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



# Imported Modules #############################################################

import sys



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
STR__insufficient_inputs = "\nERROR: Not enough inputs were given."

STR__IO_error_read = "\nERROR: Input file does not exist or could not be "\
        "opened."
STR__overwrite_confirm = "\nFile already exists. Do you wish to overwrite it? "\
        "(y/n): "
STR__IO_error_write_forbid = """
ERROR: You specified an output file which already exists and the administrator
for this program has forbidden all overwrites. Please specify a different
output file, move the currently existing file, or configure the default options
in t2t.py."""
STR__IO_error_write_unable = """
ERROR: Unable to write to the specified output file."""
STR__invalid_file_format = """
ERROR: Invalid io file format: {s}
Please specify one of:
    tsv
    csv
    ssv"""

STR__invalid_flag = "\nERROR: Invalid operation specified."

STR__no_value = "ERROR: No value given after flag: {s}"

STR__invalid_join = """
ERROR: Invalid join type: {s}
Please specify one of:
    inner
    left
    right
    outer
    xor"""

STR__invalid_bool = """
ERRPR: Invalid boolean: {s}
Please specify one of:
    yes
    no"""



STR__metrics_lines = """
    METRICS:

     Lines (L): {A}
     Lines (R): {B}
     Lines (O): {C}

    Columns(L): {D}
    Columns(R): {E}
    Columns(O): {F}
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



# File Processing Code #########################################################

def Join_Tables(path_in, delim_in, path_out, delim_out, columns,
            inc_filters, exc_filters, headers, novel_unique):
    """
    """
    pass



# Command Line Parsing #########################################################

def Parse_Command_Line_Input__Join_Tables(raw_command_line_input):
    """
    """
    pass



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
