import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from IPython.core.display import display,HTML

# if __name__ == '__main__':
# h = plt.hist(np.random.triangular(0, 5, 9, 1000), bins=100, linewidth=0)
# plt.show()

from pygments import highlight
from pygments.lexers import PythonLexer
from pygments.formatters import HtmlFormatter
from IPython.core.display import HTML

def display_nice(python_string):
    display(HTML("""
    <style>
    {pygments_css}
    </style>
    """.format(pygments_css=HtmlFormatter().get_style_defs('.highlight'))))
    display(HTML(data=highlight(python_string, PythonLexer(), HtmlFormatter())))