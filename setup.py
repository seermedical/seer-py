from pathlib import Path
from setuptools import setup, find_packages

setup(
    name='seerpy',
    version='0.6.2',
    description='Seer Platform SDK for Python',
    long_description=open('README.md').read(),
    url='https://github.com/seermedical/seer-py',
    author='Brendan Doyle',
    author_email='brendan@seermedical.com',
    license='MIT',
    classifiers=[
        'Development Status :: Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    keywords='api seer eeg ecg client',
    packages=find_packages(include=["seerpy*"]),
    install_requires=['gql', 'requests', 'numpy', 'pandas', 'pyjwt[crypto]'],
    extras_require={'viz': ['matplotlib']},
    tests_require=['pytest'],
)
