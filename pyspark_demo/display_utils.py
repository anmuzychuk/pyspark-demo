import pandas as pd
from rich.console import Console
from rich.table import Table


def display_dataframe(df: pd.DataFrame) -> None:
    """
    Display a pandas DataFrame in a nicely formatted table using the rich library.

    Parameters:
    df (pandas.DataFrame): The DataFrame to display.
    """

    console = Console()
    table = Table(show_header=True, header_style="bold magenta")

    # 1. Dynamically add the column headers
    for col in df.columns:
        table.add_column(col)

    # 2. Add rows by unpacking the values as strings
    for _, row in df.iterrows():
        # Convert each element in the row to a string so rich can render it
        table.add_row(*(str(val) for val in row))

    # Print the beautifully formatted table to the terminal
    console.print(table)
