import os
import sys

# Configuration file for the Sphinx documentation builder.

sys.path.insert(0, os.path.abspath("../src/"))


# -- Path setup --------------------------------------------------------------

# If extensions (or modules to document with autodoc) are in another directory,
# add these directories to sys.path here. If the directory is relative to the
# documentation root, use os.path.abspath to make it absolute, like shown here.
#

sys.path.insert(0, os.path.abspath(".."))


# -- Project information

project = "heiDGAF"
copyright = "2025, Stefan Machmeier and Manuel Fuchs"
author = "Stefan Machmeier and Manuel Fuchs"

# exec(open("../src/version.py").read())

# version = __version__
# The full version, including alpha/beta/rc tags
# release = __version__

# -- General configuration

extensions = [
    "sphinx.ext.napoleon",
    "sphinx.ext.autodoc",
    "sphinx.ext.doctest",
    "sphinx.ext.intersphinx",
    "sphinx.ext.todo",
    "sphinx.ext.coverage",
    "sphinx.ext.mathjax",
    "sphinx.ext.ifconfig",
    "sphinx.ext.viewcode",
    "sphinx.ext.autosummary",
    "sphinxcontrib.bibtex",
    "sphinx.ext.mathjax",
]

# Napoleon settings
napoleon_google_docstring = True
napoleon_numpy_docstring = True
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False
napoleon_include_special_with_doc = True
napoleon_use_admonition_for_examples = False
napoleon_use_admonition_for_notes = False
napoleon_use_admonition_for_references = False
napoleon_use_ivar = False
napoleon_use_param = True
napoleon_use_rtype = False

intersphinx_mapping = {
    "python": ("https://docs.python.org/3/", None),
    "sphinx": ("https://www.sphinx-doc.org/en/master/", None),
}
intersphinx_disabled_domains = ["std"]

templates_path = ["_templates"]
exclude_patterns = ['_build, "Thumbs.db', ".DS_Store"]

# -- Options for HTML output
html_theme = "sphinx_book_theme"
html_theme_options = {
    "use_repository_button": True,
    "repository_url": "https://github.com/stefanDeveloper/heiDGAF",
}
html_logo = "../assets/heidgaf_logo_readthedocs.png"
# -- Options for EPUB output
epub_show_urls = "footnote"

language = "en"

# -- Bibliography ------------------------------------------------------------
bibtex_bibfiles = ["refs.bib"]
bibtex_default_style = "unsrt"
bibtex_reference_style = "author_year"
