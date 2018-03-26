from setuptools import setup, find_packages

setup(
    name='seer',
    version='0.1.0',
    description='Seer Platform api for Python',
    long_description=open('README.md').read(),
    url='https://github.com/seermedical/seer-py',
    author='Shannon Clarke',
    author_email='shannon@seermedical.com',
    license='MIT',
    classifiers=[
        'Development Status :: Pre-Alpha',
        'Intended Audience :: Developers',
        'Topic :: Software Development :: Libraries',
#        'Programming Language :: Python :: 2',
#        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.3',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: Implementation :: PyPy',
    ],
    keywords='api seer eeg ecg client',
    packages=find_packages(include=["seer*"]),
    install_requires=[
        'gql',
        'requests',
        'matplotlib',
        'numpy',
        'pandas'
    ],
#    tests_require=['pytest>=2.7.2', 'mock'],
)