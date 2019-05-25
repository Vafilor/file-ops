import setuptools

with open("README.md", "r") as fh:
    long_description = fh.read()
    
setuptools.setup(
    name="file-ops",
    version="1.0.3",
    author="Andrey Melnikov",
    author_email="vafilor@gmail.com",
    description="Filesystem operations to index files and hash their contents",
    long_description=long_description,
    long_description_content_type="text/markdown",
    url="https://github.com/Vafilor/file-ops",
    packages=setuptools.find_packages(),
    entry_points={"console_scripts": ["fileops=fileops.cli:main"]},
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License v3 (GPLv3)",
        "Operating System :: OS Independent",
    ],
)

