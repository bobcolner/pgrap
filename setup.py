from distutils.core import setup

setup(
    name = 'pgrap',
    packages = ['pgrap'], # this must be the same as the name above
    version = '0.1',
    description = 'Postgres library for key-value and document style db access',
    author = 'Bob Colner',
    author_email = 'bcolner@gmail.com',
    url = 'https://github.com/bobcolner/pgrap', # use the URL to the github repo
    download_url = 'https://github.com/bobcolner/pgrap/tarball/0.1', # I'll explain this in a second
    keywords = ['postgres', 'sql', 'key-value'], # arbitrary keywords
    classifiers = [],
)