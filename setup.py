import os

from setuptools import find_packages, setup

with open(os.path.join("VERSION")) as f:
    version = f.read().strip()

setup(
    name="opensafely-jobrunner",
    version=version,
    packages=find_packages(),
    include_package_data=True,
    url="https://github.com/opensafely-core/job-runner",
    author="OpenSAFELY",
    author_email="tech@opensafely.org",
    python_requires=">=3.7",
    install_requires=["ruamel.yaml", "requests"],
    classifiers=["License :: OSI Approved :: GNU General Public License v3 (GPLv3)"],
    entry_points=dict(
        console_scripts=[
            "local_run=jobrunner.local_run:run",
            "add_job=jobrunner.add_job:run",
            "kill_job=jobrunner.kill_job:run",
            "retry_job=jobrunner.retry_job:run",
        ],
    ),
)
