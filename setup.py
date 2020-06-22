from setuptools import setup, find_packages
from platform import python_version_tuple

version = python_version_tuple()
filepath = 'README.md'


def requirements():
    with open('requirements.txt', 'r') as fileobj:
        requirements = [line.strip() for line in fileobj]

        if version[0] == '2':
            requirements.append("futures")

        return requirements


def long_description():
    if version[0] == '2':
        with open(filepath, 'rb') as fileobj:
            return fileobj.read().decode('utf8')
    else:
        return open(filepath, encoding='utf-8').read()


setup(
    name='vodmigrate',
    version='1.0.7',
    url='https://www.qcloud.com/',
    license='MIT',
    author='vod',
    author_email='286242216@qq.com',
    description='vod migrate tool',
    long_description=long_description(),
    long_description_content_type='text/markdown',
    packages=find_packages(exclude=["test*"]),
    install_requires=requirements(),
    entry_points={
        'console_scripts': [
            'vodmigrate=qcloud_vod_migrate.cmd:_main',
        ],
    },
    data_files=[filepath]
)
