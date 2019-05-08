from setuptools import setup, find_packages

setup(
    name='seerpy',
    version='0.2.1',
    description='Seer Platform SDK for Python',
    long_description=open('README.md').read(),
    url='https://github.com/seermedical/seer-py',
    author='Shannon Clarke',
    author_email='shannon@seermedical.com',
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
    install_requires=[
        'gql',
        'requests',
        'matplotlib',
        'numpy',
        'pandas'
    ],
    tests_require=[
        'pytest'
    ],
)
