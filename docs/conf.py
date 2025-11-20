# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import sys
from pathlib import Path
ROOT = Path(__file__).resolve().parents[1]   # docs/ -> root/
sys.path.insert(0, str(ROOT / "src"))

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'Cscorer'
copyright = '2025, Etienne Godin'
author = 'Etienne Godin'
release = '0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    "sphinx_autodoc_typehints"


]
extensions.append("sphinx.ext.napoleon")
extensions.append("myst_parser")


autosummary_generate = True  # Required
autodoc_typehints = "description"
autodoc_typehints_format = "short"     # cleaner display
autodoc_member_order = "bysource"      # preserve code order
autoclass_content = "both"             # use __init__ docstring too
autodoc_inherit_docstrings = True


autodoc_default_options = {
    "members": True,
    "undoc-members": False,
    "private-members": False,
    "special-members": "__init__",
    "show-inheritance": True,
}


templates_path = ['_templates']
exclude_patterns = ['_build', 'Thumbs.db', '.DS_Store']



# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

html_theme = 'sphinx_rtd_theme'
#html_theme = 'furo'

html_static_path = ['_static']

source_suffix = {
    '.rst': 'restructuredtext',
    '.txt': 'markdown',
    '.md': 'markdown',
}