# Configuration file for the Sphinx documentation builder.

# -- Project information

project = 'Yaetos'
copyright = '2018, Arthur Prevot'
author = 'Arthur Prevot'

release = '0.11'
version = '0.11.7'

# -- General configuration

extensions = [
    'sphinx.ext.duration',
    'sphinx.ext.doctest',
    'sphinx.ext.autodoc',
    'sphinx.ext.autosummary',
    'sphinx.ext.intersphinx',
]

intersphinx_mapping = {
    'python': ('https://docs.python.org/3/', None),
    'sphinx': ('https://www.sphinx-doc.org/en/master/', None),
}
intersphinx_disabled_domains = ['std']

templates_path = ['_templates']

# -- Options for HTML output

html_theme = 'alabaster'  # default: 'sphinx_rtd_theme'
html_theme_options = {
    'show_powered_by': False,
    'github_user': 'arthurprevot',
    'github_repo': 'yaetos',
    'github_banner': True,
    'github_type': 'star',
    'github_button': True,
    'show_related': False,
    'note_bg': '#FFF59C'
}  # TODO: add "Fork me on github" banner with 'sphinx_rtd_theme' theme, using _template, see https://stackoverflow.com/questions/53327826/how-to-add-fork-me-on-github-ribbon-using-sphinx-and-readthedocs-theme

# -- Options for EPUB output
epub_show_urls = 'footnote'
