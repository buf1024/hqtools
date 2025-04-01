from setuptools import find_packages, setup

with open("README.MD", "r") as f:
    long_description = f.read()

setup(
    name='hqtools',
    version='0.0.1',
    packages=find_packages(include=['hqtools']),
    include_package_data=True,
    zip_safe=False,
    description='一些简单的工具集',
    long_description=long_description,
    long_description_content_type='text/markdown',
    url="https://github.com/buf1024/hqtools",
    platform="any",
    install_requires=[
        'motor',
        'pymongo',
        'nest_asyncio',
        'pandas'
    ],
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ]
)
