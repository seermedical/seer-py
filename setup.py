from setuptools import find_packages, setup

setup(
    name="seerpy",
    version="0.6.6",
    description="Seer Platform SDK for Python",
    long_description=open("README.md").read(),
    url="https://github.com/seermedical/seer-py",
    author="Brendan Doyle",
    author_email="brendan@seermedical.com",
    license="MIT",
    classifiers=[
        "Development Status :: Pre-Alpha",
        "Intended Audience :: Developers",
        "Topic :: Software Development :: Libraries",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: Implementation :: PyPy",
    ],
    keywords="api seer eeg ecg client",
    packages=find_packages(include=["seerpy*"]),
    install_requires=[
        "gql[requests]>=3",
        "numpy<2.0.0",
        "pandas<2.0.0",
        "pyjwt[crypto]",
        "urllib3<1.27.0",
    ],
    extras_require={"viz": ["matplotlib"]},
    tests_require=["pytest"],
)
