HELP_DOC = """
JOIN TABLES
(version 3.0)
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
            [-o <output_path> {output_format}] [-j <join_type>] [-s <sort>]
            [-h Y|N] [-i Y|N] [-l Y|N] [-r Y|N]



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
    
    (-h)
        
        (DEFAULT: N)
        
        Whether or not the first row should be treated as column headers.
    
    (-i)
        
        (DEFAULT: Y)
        
        Whether or not to treat columns which look like integers as integers for
        the purpose of sorting.
    
    (-l)
        
        (DEFAULT: N)
        
        Whether or not duplicate keys are allowed in the left table.
    
    (-r)
        
        (DEFAULT: N)
        
        Whether or not duplicate keys are allowed in the right table.



EXAMPLES EXPLANATION:
    
    1:
    Straight forward join of two table files.
    
    2:
    Outer join of two table files, sorted in reverse order. Integer-like fields
    are treated as integers.
    
    3:
    Sorted left join of two table files with headers. Output file and format
    specified.

EXAMPLES:
    
    python27 Join.py table_1.tsv tsv 1,2 table_2.csv csv 3,2
    
    python27 Join.py table_1.tsv tsv 1,2 table_2.csv csv 3,2 -j O -i Y -s R
    
    python27 Join.py table_3.tsv tsv 1,2 table_4.csv csv 3,2 -j L -h Y -o
            merged_data.txt ssv

USAGE:
    
    python27 Join.py <input_path_left> <{input_format_left}> <key_columns_left>
            <input_path_right> <{input_format_right}> <key_columns_right>
            [-o <output_path> {output_format}] [-j <join_type>] [-s <sort>]
            [-h Y|N] [-i Y|N] [-l Y|N] [-r Y|N]
"""



# Configurations ###############################################################

AUTORUN = True

WRITE_PREVENT = False # Completely prevent overwritting existing files
WRITE_CONFIRM = True # Check to confirm overwritting existing files

PRINT_ERRORS = True
PRINT_PROGRESS = True
PRINT_METRICS = True

WARN_UNEQUAL_DUPLICATES = True
UNEQUAL_DUPLICATE_EXAMPLES = 10



# Defaults #####################################################################
"NOTE: altering these will not alter the values displayed in the HELP DOC"

DEFAULT__join = 1 #INNER
DEFAULT__sort = 2 #FORWARD
DEFAULT__headers = False
DEFAULT__integers = True
DEFAULT__left_dup = False
DEFAULT__right_dup = False



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
    JOIN METRICS:

     Lines (O): {A}
     Lines (L): {B} ({C}%)
     Lines (R): {D} ({E}%)

    Columns(O): {F}
    Columns(L): {G}
    Columns(R): {H}"""

STR__parsing_args = "\nParsing arguments..."

STR__join_begin = "\nRunning Join..."

STR__join_complete = "\nJoin successfully finished."



STR__invalid_width = """
ERROR: {s} table file does not have a consistent number of columns:
    {p}"""
STR__non_unique_key = """
Identified non-unique key:
    {s}"""
STR__invalid_key = """
ERROR: Keys for {s} table file are not unique."""



STR__unequal_duplicates = """
WARNING: Duplicate entries with the same key but different values detected for:
    {n} entries
Duplicate keys include:
    {s}"""



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
    JOIN.INNER: "INNER_JOIN",
    JOIN.LEFT: "LEFT_JOIN",
    JOIN.RIGHT: "RIGHT_JOIN",
    JOIN.OUTER: "OUTER_JOIN",
    JOIN.XOR: "XOR"}

DICT__repeat_permissions = {
    JOIN.INNER: [False, False],
    JOIN.LEFT: [True, False],
    JOIN.RIGHT: [False, True],
    JOIN.OUTER: [False, False],
    JOIN.XOR: [True, True]}



# Internals ####################################################################

UNEQUAL_DUPLICATE_EXAMPLES = UNEQUAL_DUPLICATE_EXAMPLES + 1



# File Processing Code #########################################################

def Join_Tables(path_l, delim_l, keys_l, path_r, delim_r, keys_r, path_out,
            delim_out, join, sort, headers, integers, dup_l, dup_r):
    """
    Join two tables (delimited table formatted files) and create a new table
    (also in a delimiated table format file).
    Return an exit code of 1/2 if table width is inconsistent in the left/right
    table.
    Return an exit code of 3/4 if the table key is non-unique in the left/right
    table.
    
    In the output table, the key columns will be first, followed by the non-key
    columns of the left table in their original order, followed by the non-key
    columns of the right table in their original order.
    
    @path_l
            (str - filepath)
            The filepath of the left table file.
    @delim_l
            (str)
            The delimiter to be used for the left table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_l
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the left table.
            (Uses a 0-index system.)
    @path_r
            (str - filepath)
            The filepath of the right table file.
    @delim_r
            (str)
            The delimiter to be used for the right table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_r
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the right table.
            (Uses a 0-index system.)
    @path_out
            (str - filepath)
            The filepath of the file where the output will be written into.
    @delim_out
            (str)
            The delimiter to be used for the output table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @join
            (int - ENUM)
            An integer denoting what kind of join operation will be performed.
            The options are as follows:
                1 - Inner join
                2 - Left join (left outer join)
                3 - Right join (right outer join)
                4 - Outer join (full outer join)
                5 - XOR (XOR operation)
    @sort
            (int - ENUM)
            An integer denoting what kind of sorting should be performed on the
            table keys.
                1 - No sorting
                2 - Forward sorting:
                        Sort integers in ascending order
                        Sort strings in alphabetical order
                3 - Reversed sorting:
                        Sort integers in descending order
                        Sort strings in reversed alphabetical order
    @headers
            (bool)
            Whether or not there are headers in the input files. Headers will be
            retained in the output file. The column headers for the key columns
            will use their corresponding column headers in the LEFT table file
            unless RIGHT OUTER JOIN was specified.
    @integers
            (bool)
            Whether or not to treat values from columns, which only contain
            digit-only strings, as integers instead of strings.
    @dup_l
            (bool)
            Whether or not duplicates would be allowed in the left table.
    @dup_r
            (bool)
            Whether or not duplicates would be allowed in the right table.
    
    Join_Tables(str, str, str, str, str, str, str, str, int, int, bool, bool)
            -> int
    """
    printP(STR__join_begin)
    
    # Key types and width check
    key_types, width_k, width_l, width_r = Get_Key_Types(path_l, delim_l,
            keys_l, path_r, delim_r, keys_r, headers)
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
    data_l = Process_Table(path_l, delim_l, keys_l, key_types, headers, dup_l)
    if not data_l: return 3
    data_r = Process_Table(path_r, delim_r, keys_r, key_types, headers, dup_r)
    if not data_r: return 4
    dict_l, keys_l, rows_l = data_l
    dict_r, keys_r, rows_r = data_r
    
    # Sorting
    if sort == SORT.FORWARD:
        keys_l = sorted(keys_l, None, None, False)
    elif sort == SORT.REVERSE:
        keys_l = sorted(keys_l, None, None, True)
    
    # Join tables
    metrics_out = Write_Table__DICTs(dict_l, keys_l, blank_l, dict_r, keys_r,
            blank_r, path_out, delim_out, join, header_values)
    metrics_in = [rows_l, rows_r]
    metrics_widths = [width_k, width_l, width_r]
    
    # Metrics
    Report_Metrics(metrics_out+metrics_in+metrics_widths)
    
    #
    printP(STR__join_complete)
    return 0



def Get_Key_Types(path_l, delim_l, keys_l, path_r, delim_r, keys_r, headers):
    """
    Return a list of:
        Booleans. Each boolean describes the two columns specified by the key in
        the key lists in the same indexed position. The boolean will be True if
        all values in both columns can be converted to an integer, and False
        otherwise.
        3 integers describing the number of key columns, the number of non-key
        columns in the left table, and the number of non-key columns in the
        right table.
    Return 1/2 if the left/right table has an inconsistent number of columns.
    
    Example:
        If the left table's keys are [0,1,2]
        And the right table's keys are [5,0,1]
        And the output of this function is [False, True, False]
        Then that would mean that all values, in column 1 of the left table and
        all values in column 0 of the right table, can be converted to integers.
    
    @path_l
            (str - filepath)
            The filepath of the left table file.
    @delim_l
            (str)
            The delimiter to be used for the left table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_l
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the left table.
            (Uses a 0-index system.)
    @path_r
            (str - filepath)
            The filepath of the right table file.
    @delim_r
            (str)
            The delimiter to be used for the right table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_r
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the right table.
            (Uses a 0-index system.)
    @headers
            (bool)
            Whether or not there are headers in the input files. If this is set
            to True, the first line in each file will be excluded from
            consideration for the analysis.
    
    Get_Key_Types(str, str, str, str, str, str, bool) ->
            [list<bool>, int, int, int]
    Get_Key_Types(str, str, str, str, str, str, bool) -> int
    """
    # Verify widths and get key types
    keys_l, width_l = Get_Key_Types_(path_l, delim_l, keys_l, headers)
    if not keys_l: return 1
    keys_r, width_r = Get_Key_Types_(path_r, delim_r, keys_r, headers)
    if not keys_r: return 2
    # Combine key types
    results = []
    width_k = len(keys_l)
    range_ = range(width_k)
    for i in range_:
        if keys_l[i] and keys_r[i]: results.append(True)
        else: results.append(False)
    #
    return [results, width_k, width_l, width_r]

def Get_Key_Types_(filepath, delim, keys, headers):
    """
    Subfunction of Get_Key_Types() but only for one table.
    
    Get_Key_Types(str, str, str, bool) -> [list<bool>, int]
    Get_Key_Types(str, str, str, str, str, str, bool) -> int
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
    Return the column headers of the input files, as a list in the order
    appropriate for the output file.
    
    @path_l
            (str - filepath)
            The filepath of the left table file.
    @delim_l
            (str)
            The delimiter to be used for the left table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_l
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the left table.
            (Uses a 0-index system.)
    @path_r
            (str - filepath)
            The filepath of the right table file.
    @delim_r
            (str)
            The delimiter to be used for the right table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys_r
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the right table.
            (Uses a 0-index system.)
    @join
            (int - ENUM)
            An integer denoting what kind of join operation will be performed.
            The options are as follows
                1 - Inner join
                2 - Left join (left outer join)
                3 - Right join (right outer join)
                4 - Outer join (full outer join)
                5 - XOR (XOR operation)
    
    Get_Header_Values(str, str, str, str, str, str, int) -> list<str>
    """
    results = []
    # Values
    f_l = open(path_l, "U")
    line_l = f_l.readline()
    values_l = line_l.split(delim_l)
    if values_l[-1][-1] == "\n": values_l[-1] = values_l[-1][:-1]
    f_l.close()
    f_r = open(path_r, "U")
    line_r = f_r.readline()
    values_r = line_r.split(delim_r)
    if values_r[-1][-1] == "\n": values_r[-1] = values_r[-1][:-1]
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
        values_l.pop(i)
    keys_sorted_r = sorted(keys_r, None, None, True)
    for i in keys_sorted_r:
        values_r.pop(i)
    # Add and return
    results = results + values_l + values_r
    return results

def Process_Table(filepath, delim, keys, key_types, headers, repeats):
    """
    Read in the data in a table file and store that data in a dictionary, with
    the dictionary key being a tuple composed of the values of the table's keys.
    Return that dictionary, a list of all the keys in the order in which they
    occurred, and the number of rows of data in the file.
    Return an empty list if the key is non-unique.
    
    When repeats are allowed, a key may return the results of multiple rows of
    data as a multi-element list. When repeats are not allowed, a key will
    return a single-element list, with that single element being the data of the
    row.
    
    @path
            (str - filepath)
            The filepath of the table file.
    @delim
            (str)
            The delimiter to be used for the table file. File formats and their
            corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @keys
            (list<int>)
            A list of the column numbers for the columns which comprise the key
            for the table.
            (Uses a 0-index system.)
    @key_types
            (list<bool>)
            A list of booleans corresponding to the columns specified by [keys].
            A "True" indicates that that column should be treated as integers
            while a "False" indicates that that column should be treated as
            strings.
    @headers
            (bool)
            Whether or not there are headers in the input file. If this is set
            to True, the first line in each file will be excluded from
            consideration for the analysis.
    @repeat
            (bool)
            Whether or not duplicate entries (as determined by the key) are
            allowed. If yes, the key will appear multiple times in the output
            list of keys.
            The net result of this will be that the duplicate entry may appear
            multiple times in the output.
            It is important to note that the values for the last instance of
            an entry with a particular key will be used for all instances. If
            two entries have the same key but different values, this will result
            in inaccuracies.
    
    Process_Table(str, str, list<int>, list<bool>, bool) ->
            [dict<tuple<str>:list<list<str>>>, list<tuple<str>>, int]
    Process_Table(str, str, list<int>, list<bool>, bool) -> []
    """
    # Setup
    results_data = {}
    results_keys = []
    rows = 0
    warning_list = []
    warning_list_count = 0
    warning_list_count_u = 0
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
        rows += 1
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
        if key == ("",): # Empty key from bad Excel exports
            pass
        elif key in results_data and not repeats: # Non-unique key
            printE(STR__non_unique_key.format(s = key))
            return []
        else: # Valid key
            # Pop
            for i in sorted_keys: values.pop(i)
            # Process
            if key in results_data:
                results_data[key].append(values)
                if results_data[key] != values:
                    warning_list_count += 1
                    if warning_list_count_u < UNEQUAL_DUPLICATE_EXAMPLES:
                        if key not in warning_list:
                            warning_list_count_u += 1
                            if (warning_list_count_u ==
                                    UNEQUAL_DUPLICATE_EXAMPLES):
                                warning_list.append("...")
                            else:
                                warning_list.append(key)
            else:
                results_data[key] = [values]
            results_keys.append(key)
        # Next
        line = f.readline()
    # Duplicates warning
    if WARN_UNEQUAL_DUPLICATES:
        warning_list = [str(i) for i in warning_list]
        warning_string = "\n    ".join(warning_list)
        printM(STR__unequal_duplicates.format(n=warning_list_count,
                s=warning_string))
    #
    f.close()
    return [results_data, results_keys, rows]

def Write_Table__DICTs(dict_l, keys_l, blank_l, dict_r, keys_r, blank_r,
            path_out, delim_out, join, header_values):
    """
    Write the data, obtained from the input table files, into the output file.
    Return the metrics of the operation as a list. The integers in this list
    represent:
        
    
    @dict_l
            (dict<tuple<str>:list<list<str>>>)
            A dictionary containing the data from the the left table.
            The dictionary keys are tuples created from the values in the key
            columns. The dictionary values are lists of the values in the
            non-key columns.
    @keys_l
            (list<str>)
            A list of the keys from the left table either sorted or unsorted
            based on the user specifications.
    @blank_l
            (str)
            The string to write to the write file if no data exists in the left
            file which corresponds to the querying key.
    @dict_r
            (dict<tuple<str>:list<list<str>>>)
            A dictionary containing the data from the the right table.
            The dictionary keys are tuples created from the values in the key
            columns. The dictionary values are lists of the values in the
            non-key columns.
    @keys_r
            (list<str>)
            A list of the keys from the right table either sorted or unsorted
            based on the user specifications.
    @blank_r
            (str)
            The string to write to the write file if no data exists in the right
            file which corresponds to the querying key.
    @path_out
            (str - filepath)
            The filepath of the file where the output will be written into.
    @delim_out
            (str)
            The delimiter to be used for the output table file. File formats and
            their corresponding delimiters are as follows:
                TSV - "\t" (tab character)
                CSV - ","  (comma character)
                SSV - " "  (whitespace character)
    @join
            (int - ENUM)
            An integer denoting what kind of join operation will be performed.
            The options are as follows
                1 - Inner join
                2 - Left join (left outer join)
                3 - Right join (right outer join)
                4 - Outer join (full outer join)
                5 - XOR (XOR operation)
    @header_values
            (list<str>)
            A list of the column header strings for the output file. An empty
            list is supplied here if no headers are to be written to the output
            file.
        
    Write_Table__DICTs(dict<tuple:list<str>>, list<tuple>, str,
            dict<tuple:list<str>>, list<tuple>, str, str, str, int, bool) ->
            [int, int, int]
    """
    # Setup
    o = open(path_out, "w")
    # Metrics
    lines_o = 0
    lines_l_o = 0
    lines_r_o = 0
    # Headers
    if header_values:
        header_str = delim_out.join(header_values) + "\n"
        o.write(header_str)
    # Join type
    if join == JOIN.INNER:
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
    lines_o = len(all_keys)
    # Duplicate handler
    registrar_l = {}
    registrar_r = {}
    for key in keys_l: registrar_l[key] = 0
    for key in keys_r: registrar_r[key] = 0
    # Iterate 
    for key in all_keys:
        key_list = []
        for k in key:
            key_list.append(str(k))
        key_str = delim_out.join(key_list)
        o.write(key_str)
        if join == JOIN.INNER:
            lines_l_o += 1
            lines_r_o += 1
            val_l = dict_l[key][0]
            str_l = delim_out.join(val_l)
            if str_l: o.write(delim_out + str_l)
            val_r = dict_r[key][0]
            str_r = delim_out.join(val_r)
            if str_r: o.write(delim_out + str_r)
        elif join == JOIN.LEFT:
            #
            index_l = registrar_l[key]
            registrar_l[key] += 1
            #
            lines_l_o += 1
            val_l = dict_l[key][index_l]
            str_l = delim_out.join(val_l)
            if str_l: o.write(delim_out + str_l)
            if key in dict_r:
                lines_r_o += 1
                val_r = dict_r[key][0]
                str_r = delim_out.join(val_r)
                if str_r: o.write(delim_out + str_r)
            else:
                o.write(blank_r)
        elif join == JOIN.RIGHT:
            #
            index_r = registrar_r[key]
            registrar_r[key] += 1
            #
            lines_r_o += 1
            if key in dict_l:
                lines_l_o += 1
                val_l = dict_l[key][0]
                str_l = delim_out.join(val_l)
                if str_l: o.write(delim_out + str_l)
            else:
                o.write(blank_l)
            val_r = dict_r[key][index_r]
            str_r = delim_out.join(val_r)
            if str_r: o.write(delim_out + str_r)
        elif join == JOIN.OUTER: # Outer
            if key in dict_l:
                lines_l_o += 1
                val_l = dict_l[key][0]
                str_l = delim_out.join(val_l)
                if str_l: o.write(delim_out + str_l)
            else:
                o.write(blank_l)
            if key in dict_r:
                lines_r_o += 1
                val_r = dict_r[key][0]
                str_r = delim_out.join(val_r)
                if str_r: o.write(delim_out + str_r)
            else:
                o.write(blank_r)
        else: # XOR
            if key in dict_l:
                #
                index_l = registrar_l[key]
                registrar_l[key] += 1
                #
                lines_l_o += 1
                val_l = dict_l[key][index_l]
                str_l = delim_out.join(val_l)
                if str_l: o.write(delim_out + str_l)
            else:
                o.write(blank_l)
            if key in dict_r:
                #
                index_r = registrar_r[key]
                registrar_r[key] += 1
                #
                lines_r_o += 1
                val_r = dict_r[key][index_r]
                str_r = delim_out.join(val_r)
                if str_r: o.write(delim_out + str_r)
            else:
                o.write(blank_r)
        o.write("\n")
    #
    o.close()
    return [lines_o, lines_l_o, lines_r_o]

def Report_Metrics(metrics):
    """
    Takes a set of numbers representing the metrics of the join operation and
    print the metrics of the join operation into the commandline.
    
    @metrics
            (list<int>)
            A list of integers showing:
                * The number of lines of data in the output table.
                * The number of lines of data in the left table outputted.
                * The number of lines of data in the right table outputted.
                * The number of lines of data in the left table.
                * The number of lines of data in the right table.
                * The number of columns in the key.
                * The number of columns in the left table.
                * The number of columns in the right table.
    
    Print_Metrics(list<int>) -> None
    """
    # Unpack
    lines_o = metrics[0]
    lines_l_o = metrics[1]
    lines_r_o = metrics[2]
    lines_l = metrics[3]
    lines_r = metrics[4]
    columns_k = metrics[5]
    columns_l = metrics[6]
    columns_r = metrics[7]
    # Calculate
    if lines_l == 0:
        percent_l = 0.0
    else:
        percent_l = (lines_l_o*100.0)/lines_l
    if lines_r == 0:
        percent_r = 0.0
    else:
        percent_r = (lines_r_o*100.0)/lines_r
    columns_a = columns_k + columns_l + columns_r
    columns_l = columns_k + columns_l
    columns_r = columns_k + columns_r
    # Strings
    lines_o = str(lines_o)
    lines_l = str(lines_l)
    percent_l = str(percent_l) + "00"
    lines_r = str(lines_r)
    percent_r = str(percent_r) + "00"
    columns_a = str(columns_a)
    columns_l = str(columns_l)
    columns_r = str(columns_r)
    # Trim
    index_l = percent_l.find(".")
    percent_l = percent_l[:index_l+3]
    index_r = percent_r.find(".")
    percent_r = percent_r[:index_l+3]
    # Pad
    max_size = max([len(lines_o), len(lines_l), len(lines_r), len(columns_a),
            len(columns_l), len(columns_r)])
    lines_o = ((" "*max_size) + lines_o)[-max_size:]
    lines_l = ((" "*max_size) + lines_l)[-max_size:]
    lines_r = ((" "*max_size) + lines_r)[-max_size:]
    columns_a = ((" "*max_size) + columns_a)[-max_size:]
    columns_l = ((" "*max_size) + columns_l)[-max_size:]
    columns_r = ((" "*max_size) + columns_r)[-max_size:]
    # Print
    printM(STR__metrics_lines.format(A = lines_o, B = lines_l, C = percent_l,
            D = lines_r, E = percent_r, F = columns_a, G = columns_l,
            H = columns_r))



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
    delim_r = Validate_File_Format(inputs[4])
    if not delim_r:
        printE(STR__invalid_file_format.format(io = "input", s = inputs[4]))
        return 1
    
    keys_l = Validate_Keys(inputs[2])
    if not keys_l:
        printE(STR__invalid_keys.format(s = inputs[2]))
        return 1
    keys_r = Validate_Keys(inputs[5])
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
    delim_out = delim_l # Default behaviour
    join = DEFAULT__join
    sort = DEFAULT__sort
    headers = DEFAULT__headers
    integers = DEFAULT__integers
    dup_l = DEFAULT__left_dup
    dup_r = DEFAULT__right_dup
    
    # Parse the rest
    while inputs:
        arg = inputs.pop(0)
        try:
            if arg in ["-o"]:
                arg2 = inputs.pop(0)
                arg3 = inputs.pop(0)
            elif arg in ["-j", "-s", "-h", "-i", "-l", "-r"]:
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
        elif arg in ["-h", "-i", "-l", "-r"]:
            bool_ = Validate_Bool(arg2)
            if bool_ == None:
                printE(STR__invalid_bool.format(s = arg2))
                return 1
            else:
                if arg == "-h": headers = bool_
                elif arg == "-i": integers = bool_
                elif arg == "-l": dup_l = bool_
                elif arg == "-r": dup_r = bool_
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
            path_out, delim_out, join, sort, headers, integers, dup_l, dup_r)
    
    # Irregular exit codes
    if exit_code == 1:
        printE(STR__invalid_width.format(p = path_l, s = "Left"))
    if exit_code == 2:
        printE(STR__invalid_width.format(p = path_r, s = "Right"))
    if exit_code == 3:
        printE(STR__invalid_key.format(s = "left"))
    if exit_code == 4:
        printE(STR__invalid_key.format(s = "right"))
    
    # Safe exit
    return exit_code



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
    abs_path = os.path.abspath(path_l)
    dirname = os.path.dirname(abs_path)
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
        num = Validate_Column_Number(s)
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



def Validate_Sort(string):
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
