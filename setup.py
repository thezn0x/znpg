from setuptools import setup, find_packages

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

setup(
    name="znpg",
    version="1.1.0",
    author="Zain(ZN-0X)",
    author_email="thezn0x.exe@gmail.com",
    description="A lightweight PostgreSQL wrapper for Python with connection pooling",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/thezn0x/znpg",
    packages=find_packages(),
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Developers",
        "Topic :: Database",
        "License :: OSI Approved :: MIT License",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
        "Programming Language :: Python :: 3.9",
        "Programming Language :: Python :: 3.10",
        "Programming Language :: Python :: 3.11",
    ],
    python_requires=">=3.7",
    install_requires=[
        "psycopg>=3.0.0",
        "psycopg-pool>=3.0.0",
    ],
    keywords="postgresql database wrapper psycopg pool",
)
