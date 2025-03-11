from setuptools import setup, find_packages

setup(
    name='vastdb_checkpoint_saver',
    version='0.1.0',
    description='A Python library for saving Langgraph VastDB Checkpoints',
    packages=find_packages(),
    include_package_data=True,
    install_requires=[
        'vastdb'
    ],
    python_requires='>=3.6',
)
