''' Functions for printing in the console in a nice way.
'''

def print_script_header(header: str):
    """ Prints the header for a script's code block in the console output.

    Args:
        header (str): Header title
    """
    print('\n')
    print(f'=== {header.upper()}')
    print(50 * '=')