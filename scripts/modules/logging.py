''' Provides a logging class to print in the console output.
'''
import inspect

class Logger:
    """Creates a simplified logger class (I felt that using Python Logger seemed like a Pandora's box to learn)
    """

    def __init__(self, is_debugging: bool = False):
        """ Sets whether to print debugging output

        Args:
            is_debugging (bool): Get the debugging flag from the arguments
        """
        self.is_debugging = is_debugging

    def print_script_header(self, header: str):
        """ Prints the header for a script's code block in the console output.

        Args:
            header (str): Header title
        """
        print('\n' + 50 * '=')
        print(f'=== {header.upper()}')
        print(50 * '=')


    def debug(self, msg: str, type: str = 'DEBUG'):
        """ Prints a debug message to the console.

        Args:
            msg (str): Message
            type (str, optional): Type of debug statemend (e.g. `'DEBUG'` or `'ERROR'`). 
                Defaults to `'DEBUG'`.
        """

        if not self.is_debugging: return
        # reference: https://stackoverflow.com/questions/24438976/debugging-get-filename-and-line-number-from-which-a-function-is-called
        caller = inspect.getframeinfo(inspect.stack()[1][0])
        print(f'<{type}@{caller.filename}:{caller.lineno}> {msg}')