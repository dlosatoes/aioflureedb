from setuptools import setup, find_packages
from codecs import open
from os import path
from sys import platform
HAS_PYJSONATA = True
try:
    import pyjsonata
except ImportError:
    HAS_PYJSONATA = False
here = path.abspath(path.dirname(__file__))

requirements = ["starkbank-ecdsa>=2.0.3", "aiohttp", "base58"]
if platform in ["linux", "linux2"] or HAS_PYJSONATA:
    requirements.append("pyjsonata")
    if platform in ["linux", "linux2"]:
        print("Note: \033[96mjsonata enabled on Linux platform\033[0m")
    else:
        print("Note: \033[96mdetected jsonata installed, enabled jsonata as dependency\033[0m")
else:
    print("Warning: \033[93mjsonata disabled on " + platform + "\033[0m")

setup(
    name='aioflureedb',
    version='0.2.18',
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
    install_requires=requirements,
    packages=find_packages(),
)

