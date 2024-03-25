# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'heidgaf'
copyright = '2024, Stefan Machmeier'
author = 'Stefan Machmeier'

release = '0.1'
version = '0.1.0'

# -- General configuration

extensions = [
    'sphinx.ext.autodoc',
    'sphinx.ext.doctest',
    'sphinx.ext.intersphinx',
    'sphinx.ext.todo',
    'sphinx.ext.coverage',
    'sphinx.ext.mathjax',
    'sphinx.ext.ifconfig',
    'sphinx.ext.viewcode',
    'sphinx.ext.napoleon',
    'sphinx_autodoc_typehints',
    'sphinxcontrib.apidoc',  # automatically generate API docs, see https://github.com/rtfd/readthedocs.org/issues/1139
    'nbsphinx',
    'myst_parser',
    'sphinx_design',
]

# -- apidoc settings ---------------------------------------------------------
apidoc_module_dir = '../../heidgaf'
apidoc_output_dir = 'api'
apidoc_excluded_paths = ['**/*test*']
apidoc_module_first = True
apidoc_separate_modules = True
apidoc_extra_args = ['-d 6']

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'sphinx_rtd_theme'

# -- Options for EPUB output
epub_show_urls = 'footnote'

language = "en"