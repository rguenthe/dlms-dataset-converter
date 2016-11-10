from setuptools import setup

setup(
    name='beedel-dataset-converter',
    version='1.0',
    packages=[''],
    url='',
    license='MIT',
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
