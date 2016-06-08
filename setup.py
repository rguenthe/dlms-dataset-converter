from setuptools import setup

setup(
    name='dlm-dataset-converter',
    version='1.0',
    packages=[''],
    url='',
    license='MIT',
    author='Richard Guenther',
    author_email='richard.guenther@haw-hamburg.de',
    description='Converts DLM zip datasets to Json/Xml/Csv',
    scripts=['dsc'],
    install_requires=[
        'numpy',
		'scipy',
    ],
)
