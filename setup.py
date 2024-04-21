# coding:utf-8

import os
import numpy


from setuptools import find_packages, setup, Extension
# or
# from distutils.core import setup  

# Detect Cython
try:
    import Cython

    ver = Cython.__version__
    _CYTHON_INSTALLED = ver >= "0.28"
except ImportError:
    _CYTHON_INSTALLED = False

if not _CYTHON_INSTALLED:
    print("Required Cython version >= 0.28 is not detected!")
    print('Please run "pip install --upgrade cython" first.')
    exit(-1)

# Numpy include
NUMPY_INCLUDE = numpy.get_include()

# Cython Extensions
extensions = [
    Extension(
        "tumbler.data.ops._libs.rolling",
        ["tumbler/data/ops/_libs/rolling.pyx"],
        language="c++",
        include_dirs=[NUMPY_INCLUDE],
    ),
    Extension(
        "tumbler.data.ops._libs.expanding",
        ["tumbler/data/ops/_libs/expanding.pyx"],
        language="c++",
        include_dirs=[NUMPY_INCLUDE],
    ),
]

here = os.path.abspath(os.path.dirname(__file__))

with open(os.path.join(here, "README.md"), encoding="utf-8") as f:
    long_description = f.read()

setup(
    name='tumbler',  # 包名字
    version='1.0.1',  # 包版本
    author="ipqhjjybj",
    author_email='250657661@qq.com',  # 作者邮箱
    license="AGPL v3",
    url='https://www.8btc.com/',  # 包的主页
    description='One trading system ',  # 简单描述
    long_description=long_description,
    include_package_data=True,
    package_data={'': ['*.abi']},
    ext_modules=extensions,
    # packages=['tumbler'],
    packages=find_packages(exclude=["run_tumbler", "test", "research", "doc", "data_analyse"]),  # 包
    classifiers=[
        # 发展时期,常见的如下
        #   3 - Alpha
        #   4 - Beta
        #   5 - Production/Stable
        'Development Status :: 3 - Alpha',

        # 开发的目标用户
        'Intended Audience :: Developers',

        # 属于什么类型
        'Topic :: Software Development :: Build Tools',

        # 目标 Python 版本
        'Programming Language :: Python :: 3.6',
    ]
)
