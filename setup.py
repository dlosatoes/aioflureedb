from setuptools import setup, find_packages
from codecs import open
from os import path

here = path.abspath(path.dirname(__file__))

setup(
    name='aioflureedb',
    version='0.2.7',
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
        'Programming Language :: Python :: 2',
        'Programming Language :: Python :: 2.7',
        'Operating System :: OS Independent',
        'Environment :: Other Environment'
    ],
    keywords='flureedb fluree flureeql sparql graphql',
    install_requires=["starkbank-ecdsa>=1.1", "aiohttp", "base58"],
    packages=find_packages(),
)

