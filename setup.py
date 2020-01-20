from setuptools import setup


with open('README.md', 'r') as f:
    long_description = f.read()

setup(
    name='s3-concat',
    packages=['s3_concat'],
    version='0.1.7',
    description='Concat files in s3',
    long_description=long_description,
    long_description_content_type='text/markdown',
    author='Eddy Hintze',
    author_email="eddy@hintze.co",
    url="https://github.com/xtream1101/s3-concat",
    license='MIT',
    classifiers=[
        "Programming Language :: Python :: 3",
        "Operating System :: OS Independent",
        "Topic :: Software Development :: Libraries :: Python Modules",
        "Topic :: Utilities",
    ],
    entry_points={
        'console_scripts': [
            's3-concat=s3_concat.cli:cli',
        ],
    },
    install_requires=[
        'boto3',
    ],

)
