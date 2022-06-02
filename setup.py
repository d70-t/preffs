import setuptools
import versioneer


with open("README.md", "r") as fh:
    long_description = fh.read()

setuptools.setup(
    name="preffs",
    version=versioneer.get_version(),
    cmdclass=versioneer.get_cmdclass(),
    author="Tobias Kölling",
    author_email="tobias.koelling@mpimet.mpg.de",
    description="reference filesystem using parquet",
    long_description=long_description,
    long_description_content_type="text/markdown",
    packages=setuptools.find_packages(),
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: MIT License",
        "Operating System :: OS Independent",
    ],
    python_requires='>=3',
    install_requires=[
        "fsspec>=0.9.0",
        "pandas",
    ],
    entry_points={
        'fsspec.specs': [
            'preffs=preffs.PRefFileSystem',
        ],
    },
)
