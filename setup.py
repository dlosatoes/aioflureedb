from setuptools import setup, find_packages
from codecs import open
from os import path
from sys import platform
here = path.abspath(path.dirname(__file__))

requirements = ["starkbank-ecdsa>=2.0.3", "aiohttp", "base58"]

setup(
    name='aioflureedb',
    version='0.6.0',
    description='Asynchonous library for usage of the FlureeDB API',
    long_description="""An asynchonous client library for communicating with a FlureeDB server, making signed transactions and queries.
    """,
    url='https://github.com/pibara/aioflureedb',
    author='Rob J Meijer',
    author_email='pibara@gmail.com',
    license='BSD',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'License :: OSI Approved :: BSD License',
        'Programming Language :: Python :: 3',
        'Operating System :: OS Independent',
        'Environment :: Other Environment'
    ],
    keywords='flureedb fluree flureeql sparql graphql',
    install_requires=requirements,
    extras_require={'domainapi': ['jsonata>=0.2.3']},
    packages=find_packages(),
)

