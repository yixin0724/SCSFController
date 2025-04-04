"""
ScSF（调度仿真框架）安装器
此脚本用于使用 setuptools 安装 ScSF 包
"""
from setuptools import setup, find_packages


# Synchronize version from code.
version = "0.1"

# 调用 setup 函数来配置包以供安装。
setup(
    name="scsf",
    packages=find_packages(),
    version=version,
    extras_require={},
    author="Gonzalo Rodrigo",
    author_email="GPRodrigoAlvarez@lbl.gov",
    maintainer="Gonzalo Rodrigo",
    url="https://bitbucket.org/gonzalorodrigo/scsf/",
    license="BSD 3-clause",
    description="""ScSF：调度仿真框架：涵盖调度仿真框架的工具：工作负载建模和生成，外部调度模拟器的控制，实验协调和结果分析。. 
    """,
    long_description="",
    keywords=["HPC", "workloads", "simulation"],
    classifiers=[
        "Programming Language :: Python :: 2",
        "Programming Language :: Python :: 2.7",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.3",
        "Programming Language :: Python :: 3.4",
        "Programming Language :: Python :: 3.5",
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "Operating System :: OS Independent",
        "Topic :: Scientific/Engineering :: Information Analysis",
        "Topic :: Software Development :: Libraries :: Python Modules"
    ],
)