from setuptools import find_packages, setup

VERSION = '0.4'

setup(
    name = 'pgrap',
    packages = find_packages(),  
    version = VERSION,
    platforms=['any'],
    description = 'Postgres library for key-value and document style db access',
    author = 'Bob Colner',
    author_email = 'bcolner@gmail.com',
    license='MIT',
    url = 'https://github.com/bobcolner/pgrap',
    download_url = 'https://github.com/bobcolner/pgrap/tarball/{0}'.format(VERSION), 
    keywords = ['postgres', 'sql', 'key-value'], 
    # See https://pypi.python.org/pypi?%3Aaction=list_classifiers
    classifiers=[
        # How mature is this project? Common values are
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',
        # Indicate who your project is intended for
        'Intended Audience :: Developers',
        'Topic :: Database :: Front-Ends    ',
        # Pick your license as you wish (should match 'license' above)
        'License :: OSI Approved :: MIT License',
        # Specify the Python versions you support here.
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
    ],
    install_requires = [ i.strip() for i in open("requirements.txt").readlines() ]
)