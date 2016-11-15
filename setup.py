from setuptools import setup

setup(
    name='beedel-dataset-converter',
    version='1.2',
    packages=[''],
    url='',
    license='CLOSED',
    author='Richard Guenther',
    author_email='richard.guenther@haw-hamburg.de',
    description='Converts BEEDeL zip datasets to Mat/CSV',
    scripts=['dsc'],
    install_requires=[
        'numpy',
	    'scipy',
	    'pymongo',
    ],
)
