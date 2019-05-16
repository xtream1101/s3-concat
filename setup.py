from distutils.core import setup

try:
    import pypandoc
    long_description = pypandoc.convert('README.md', 'rst', format='md')
except (IOError, ImportError) as e:
    print(str(e))
    long_description = ''

setup(
    name='s3-concat',
    packages=['s3_concat'],
    version='0.1.1',
    description='Concat files in s3',
    long_description=long_description,
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
    install_requires=['boto3',
                      ],

)
