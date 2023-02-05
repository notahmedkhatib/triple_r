"""
RRR (REPLACE, REMOVE, REDUNDANCY)
- This module contains 3 main functions called replace, remove, redundancy.
- Their functionality are self-explanatory.
- Replace function basically finds and replaces a text.
- Remove function removes a text if finds a match.
- Redundancy removes repetition within a string.
"""
import pandas as pd


def redundant(split_string: list) -> list: 
    """
    PARAMETERS:
    - split_string: list.
        - Takes a list of string split by a comma ( , ) as the only argument.

    RETURNS:
    - A list without any repeated / redundant elements.

    WORKING:
    - It can be defined as a CASE-INSENSITIVE LIST to SET conversion but while PRESERVING the order of the elements.
    - If it finds any repetitive elements, it will keep the first one.

    EXAMPLE:
    >>> string_list = ['AhMEd', 'Haamid', 'Ahmed']
        redundant(string_list)
        OUTPUT: ['AhMEd', 'Haamid']
    """
    lower_string = []
    final_string = []
    [lower_string.append(text.strip().lower()) for text in split_string if text.strip() != '' and text.strip().lower() not in lower_string]
    [(final_string.append(text.strip()), lower_string.remove(text.strip().lower())) for text in split_string if text.strip().lower() in lower_string]
    return final_string

def redundancy(df: pd.DataFrame, source_name: str, flag_col: str = 'flags') -> tuple:
    """
    PARAMETERS:
    - df: DataFrame.
        - The Pandas DataFrame you want to work with / on.
    - source_name: str.
        - The string containing the name of the source column which will be processed (It should belong to the DataFrame passed above).
    - flag_col: str.
        - Default value is: 'flags'.
        - The string containing the name of the flag column (It should belong to the DataFrame passed above).
        - If 'flags' column doesn't exist, will use an empty string in its stead.

    RETURNS:
    - A tuple containing 2 lists: The updated list containing the process result and list of flags.

    WORKING:
    - For each row of the SOURCE_COLUMN provided, it will split the string by a comma ( , ), then find and remove redundant values.
    - After removing the redundant values, it will form a string again and append the string inside UPDATED_LIST.
    - 'redundancy' will be appended to the flag_string (flag_row adjacent to the source_row) if any redundancies were removed and stored inside FLAG_LIST.
    - Finally both list will be returned inside a tuple.
    - It is CASE-INSENSITIVE and if find repeated elements, will keep the first one.
    - NOTE: Empty values, and bad spacings will be fixed automatically.

    EXAMPLE:
    >>> df = pd.DataFrame({'names': ['AhMeD,Haamid Ahmed']})
        redundancy(df, 'names')
        OUTPUT:
             names
        0   'AhMeD,Haamid'
    """
    updated_list = []
    flag_list = []
    for row in df.to_dict(orient = 'records'):
        split_string = [text.strip().title() if text.islower() else text.strip() for text in row[source_name].split(',') if text.strip() !='']
        flags = '' if flag_col not in df.columns else row[flag_col]
        split_flags = [flag.strip().lower() for flag in flags.split(',') if flag.strip() != '']
        final_string = redundant(split_string)
        split_flags.append('redundancy') if final_string != split_string and 'redundancy' not in split_flags else split_flags
        updated_list.append(', '.join(final_string))
        flag_list.append(', '.join(split_flags))
    return updated_list, flag_list

def remove(df: pd.DataFrame, source_name: str, remove_col: pd.Series, flag_col: str = 'flags', redundancy = True) -> tuple:
    """
    Parameters
    -----------
    df : DataFrame.
        The Pandas DataFrame you want to work with / on.
    source_name: str.
        The string containing the name of the source column which will be processed (It should belong to the DataFrame passed above).
    remove_col: Series.
        The column of a DataFrame containing the values to be searched for removal.\n
    flag_col: str (Default: 'flags').
        The string containing the name of the flag column (It should belong to the DataFrame passed above).
        If 'flags' column doesn't exist, will use an empty string in its stead.

    Returns
    -------
    A tuple containing 2 lists: The updated list containing the process result and list of flags.

    Working
    -------
    - For each row of the ``source_column`` provided, it will split the string by a comma ( , ), then find and remove redundant values.
    - After removing the redundant values, it will form a string again and append the string inside UPDATED_LIST.
    - 'redundancy' will be appended to the flag_string (flag_row adjacent to the source_row) if any redundancies were removed and stored inside FLAG_LIST.
    - Finally both list will be returned inside a tuple.
    - It is CASE-INSENSITIVE and if find repeated elements, will keep the first one.
    - NOTE: Empty values, and bad spacings will be fixed automatically.

    EXAMPLE:
    >>> df = pd.DataFrame({'names': ['AhMeD,Haamid Ahmed']})
        redundancy(df, 'names')
        OUTPUT:
             names
        0   'AhMeD,Haamid'
    """
    updated_list = []
    flag_list = []
    remove_dict = dict.fromkeys(remove_col.str.lower())
    for row in df.to_dict(orient = 'records'):
        split_string = [text.strip().title() if text.islower() else text.strip() for text in row[source_name].split(',') if text.strip() !='']
        flags = '' if flag_col not in df.columns else row[flag_col]
        split_flags = [flag.strip().lower() for flag in flags.split(',') if flag.strip() != '']
        final_string = [text for text in split_string if text.strip() != '' and text.lower() not in remove_dict]
        split_flags.append('remove') if final_string != split_string and 'remove' not in split_flags else split_flags
        if not redundancy:
            temp_string = redundant(final_string)
            split_flags.append('redundancy') if final_string != temp_string and 'redundancy' not in split_flags else split_flags
            final_string = temp_string
        updated_list.append(', '.join(final_string))
        flag_list.append(', '.join(split_flags))
    return updated_list, flag_list

def replace(df: pd.DataFrame, source_name: str, search_col: pd.Series, replace_col: pd.Series, remove_col: pd.Series = pd.Series(dtype=object), flag_col: str = 'flags', redundancy = True) -> tuple:
    updated_list = []
    flag_list = []
    remove_dict = dict.fromkeys(remove_col.str.lower())
    sr_dict = dict(zip(search_col.str.lower(), replace_col))
    for row in df.to_dict(orient = 'records'):
        split_string = [text.strip().title() if text.islower() else text.strip() for text in row[source_name].split(',') if text.strip() !='']
        flags = '' if flag_col not in df.columns else row[flag_col]
        split_flags = [flag.strip().lower() for flag in flags.split(',') if flag.strip() != '']
        final_string = [sr_dict[text.lower()] if text.lower() in sr_dict else text for text in split_string]
        split_flags.append('replace') if final_string != split_string and 'replace' not in split_flags else split_flags
        temp_string = [text for text in final_string if text.lower() not in remove_dict]
        split_flags.append('remove') if final_string != temp_string and 'remove' not in split_flags else split_flags
        final_string = temp_string
        if not redundancy:
            temp_string = redundant(final_string)
            split_flags.append('redundancy') if final_string != temp_string and 'redundancy' not in split_flags else split_flags
            final_string = temp_string
        updated_list.append(', '.join(final_string))
        flag_list.append(', '.join(split_flags))
    return updated_list, flag_list