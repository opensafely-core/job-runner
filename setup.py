from setuptools import find_packages, setup

setup(
    name="jobrunner",
    packages=find_packages(),
    url="https://github.com/opensafely/job-runner",
    author="OpenSAFELY",
    author_email="tech@opensafely.org",
    python_requires=">=3.7",
    install_requires=["networkx", "pebble", "pyyaml", "requests"],
    classifiers=["License :: OSI Approved :: GNU General Public License v3 (GPLv3)"],
)
